#!/usr/bin/env bash
# chaos-06 schema_drift (ADR-014 D3) — the one genuinely NEW scenario: a
# source schema change (never drilled before; ADR-011 explicitly says GE is
# not the schema-drift detector).
#
# Act A — ADDITIVE drift: ALTER TABLE users ADD COLUMN IF NOT EXISTS
# loyalty_tier text on the OLTP source. The mutator's own user updates carry
# it through CDC. Named mechanism BEFORE the act: NONE exists — the JSONB
# raw landing (raw.cdc_users.before/after are full payloads) makes additive
# drift non-breaking and invisible end-to-end. That honest gap is the
# measured finding: after-images carry the key, staging has no such column,
# zero tests fail, zero alerts, the close is green. Compensating control:
# full after-images are retained (forensic recoverability).
#
# Act B — RENAME probe: ALTER TABLE users RENAME COLUMN last_login_at TO
# last_login_chaos_tmp (guarded: only if last_login_at exists; reverted
# within the scenario). Named blast radius BEFORE the act: the mutator's
# UPDATE names the column, so its ticks fail loudly (app-level degradation,
# measured via its log line + health endpoint); the connector keeps its
# source; the warehouse cannot corrupt because last_login_at has NO not_null
# test and is excluded from the SCD2 check columns (ADR-009) — a gap on gap,
# documented, and bounded by the in-scenario revert.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 06 schema_drift: ADD COLUMN (invisible by measurement) + guarded RENAME probe (mutator blast radius) + revert"
chaos_preflight

# 0. guards: idempotency for re-runs
HAS_COL="$(oltpq "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='loyalty_tier'")"
[ "$HAS_COL" = "0" ] || chaos_note "loyalty_tier already present (re-run) — ADD COLUMN is IF NOT EXISTS, continuing"
HAS_ORIG="$(oltpq "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='last_login_at'")"
[ "$HAS_ORIG" = "1" ] || chaos_fail "last_login_at missing on oltp users (a previous probe did not revert?) — refusing to run"
OPEN_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
[ "$OPEN_BASE" = "0" ] || chaos_fail "quarantine open=$OPEN_BASE"
USERS_STG_BASE="$(whq "SELECT count(*) FROM staging.stg_users")"
LATEST_LSN_BASE="$(whq "SELECT count(*) FROM raw.cdc_users")"
chaos_ok "pre-state: staging.stg_users=$USERS_STG_BASE raw.cdc_users=$LATEST_LSN_BASE quarantine open=0"

# ---- Act A: additive drift -------------------------------------------------
chaos_note "ACT A: ALTER TABLE public.users ADD COLUMN IF NOT EXISTS loyalty_tier text"
ACT_TS="$(date -u +%Y-%m-%d\ %H:%M:%S+00)"
oltpq "ALTER TABLE public.users ADD COLUMN IF NOT EXISTS loyalty_tier text" >/dev/null
chaos_note "waiting for mutator churn to carry the new column through CDC (<=90s) ..."
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  N="$(whq "SELECT count(*) FROM raw.cdc_users WHERE op='u' AND after ? 'loyalty_tier' AND landed_at > '$ACT_TS'")"
  [ "$N" -ge 1 ] && break
  sleep 5
done
[ "$N" -ge 1 ] || chaos_fail "no after-image carrying loyalty_tier landed within 90s (churn/CDC broken?)"
chaos_ok "measured: $N landed after-image(s) contain the NEW key 'loyalty_tier' (JSONB landing absorbed the drift)"

chaos_note "full close over drifted source (expect GREEN — that IS the finding)"
DRIFT_RUN="chaos06-drift-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-06-etl-${DRIFT_RUN}.log"
bash scripts/run-etl.sh "$DRIFT_RUN" >"$ETL_LOG" 2>&1 &
ETL_PID=$!
wait "$ETL_PID"; ETL_RC=$?
[ "$ETL_RC" = "0" ] || { cat "$ETL_LOG"; chaos_fail "close over additive drift exited $ETL_RC (unexpected: additive drift should be invisible)"; }
chaos_ok "close over additive drift: SUCCESS (gate never noticed)"
STG_COL="$(whq "SELECT count(*) FROM information_schema.columns WHERE table_schema='staging' AND table_name='stg_users' AND column_name='loyalty_tier'")"
[ "$STG_COL" = "0" ] || chaos_fail "staging unexpectedly gained loyalty_tier"
ALERT_ST="$(chaos_alert_state HeliosAirflowTaskFailure)"
chaos_note "MEASURED GAP (the finding): after-images carry the key; staging.stg_users has NO loyalty_tier column; 0 tests failed; HeliosAirflowTaskFailure=$ALERT_ST; nothing in the chain detects additive schema drift (ADR-011: GE is not the schema-drift detector — nor is anything else). Natural future detector: a dbt source-level column contract."

# ---- Act B: rename probe (bounded, guarded, reverted in-scenario) ---------
chaos_note "ACT B: RENAME last_login_at -> last_login_chaos_tmp (the mutator's UPDATE names this column)"
# Guard against leaving the source degraded: the revert runs on script EXIT
# (assert failures included), then the explicit revert below does the normal path.
chaos_revert_rename() {
  local has_tmp
  has_tmp="$(docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "SELECT count(*) FROM information_schema.columns WHERE table_name='users' AND column_name='last_login_chaos_tmp'" 2>/dev/null || echo 0)"
  if [ "$has_tmp" = "1" ]; then
    docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -c "ALTER TABLE public.users RENAME COLUMN last_login_chaos_tmp TO last_login_at" >/dev/null 2>&1 \
      && echo "[chaos] EXIT-trap: leftover rename reverted"
  fi
}
trap chaos_revert_rename EXIT

MUT_FAILS_BEFORE="$(docker logs --tail 500 helios-oltp-mutator 2>&1 | grep -c 'tick failed' || true)"
oltpq "ALTER TABLE public.users RENAME COLUMN last_login_at TO last_login_chaos_tmp" >/dev/null
sleep 20   # several mutator ticks
MUT_FAILS_AFTER="$(docker logs --tail 500 helios-oltp-mutator 2>&1 | grep -c 'tick failed' || true)"
MUT_FAILS=$((MUT_FAILS_AFTER - MUT_FAILS_BEFORE))
MUT_HEALTH="$(docker exec helios-oltp-mutator python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8081/health').status)" 2>/dev/null || echo unreachable)"
MUT_LAST_ERR="$(docker logs --tail 5 helios-oltp-mutator 2>&1 | grep 'tick failed' | tail -1 | cut -c1-140)"
chaos_note "reverting the rename (the probe's bounded-blast-radius contract) — before asserting, so the source is restored even if the assert would fail"
oltpq "ALTER TABLE public.users RENAME COLUMN last_login_chaos_tmp TO last_login_at" >/dev/null
trap - EXIT
[ "$MUT_FAILS" -ge 1 ] || chaos_fail "expected mutator tick failures after the rename (its SQL names last_login_at) — measured delta $MUT_FAILS"
chaos_ok "measured blast radius (app layer): $MUT_FAILS mutator tick failure(s) during the broken window (delta $MUT_FAILS_BEFORE -> $MUT_FAILS_AFTER); health=$MUT_HEALTH (503 = stale); last error: $MUT_LAST_ERR"
CONN_ST="$(docker exec helios-cdc-connect curl -sf --max-time 10 localhost:8083/connectors/helios-oltp/status 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["tasks"][0]["state"])' 2>/dev/null || echo unreachable)"
chaos_note "connector task state during the broken window: $CONN_ST (CDC keeps its source; the source app is what breaks)"
chaos_note "MEASURED GAP (gap on the gap): last_login_at has NO dbt not_null test and is excluded from the SCD2 check columns (ADR-009) — the pipeline would NOT detect this rename either; bounded by the immediate revert"

chaos_note "waiting for mutator recovery (a successful POST-revert tick: health last_ok_epoch > revert time) ..."
RECOVER_TS="$(date +%s)"
deadline=$(( $(date +%s) + 120 ))
OK_EPOCH=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  OK_EPOCH="$(docker exec helios-oltp-mutator python -c "import urllib.request,json;print(json.load(urllib.request.urlopen('http://localhost:8081/health'))['last_ok_epoch'])" 2>/dev/null || echo "")"
  [ -n "$OK_EPOCH" ] && [ "$(python3 -c "print(1 if float('$OK_EPOCH') > float('$RECOVER_TS') else 0)")" = "1" ] && break
  sleep 5
done
[ -n "$OK_EPOCH" ] && [ "$(python3 -c "print(1 if float('$OK_EPOCH') > float('$RECOVER_TS') else 0)")" = "1" ] \
  || chaos_fail "no successful mutator tick after the revert within 120s (last_ok_epoch=${OK_EPOCH:-none}, revert at $RECOVER_TS)"
chaos_ok "mutator recovered: successful tick measured post-revert (last_ok_epoch=$OK_EPOCH > $RECOVER_TS), health fresh"
GROW="$(whq "SELECT count(*) FROM raw.cdc_users")"
[ "$GROW" -ge "$LATEST_LSN_BASE" ] || chaos_fail "raw.cdc_users shrank: $LATEST_LSN_BASE -> $GROW"
chaos_ok "raw.cdc_users baseline=$LATEST_LSN_BASE now=$GROW (churn resumed landing)"

chaos_ok "scenario 06 schema_drift PASSED (wall $(chaos_wall))"
echo "[chaos] ---- run-etl driver transcript (additive-drift close, $ETL_LOG) ----"
cat "$ETL_LOG"
