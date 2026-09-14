#!/usr/bin/env bash
# chaos-01 kill_worker (ADR-014 D2/D3): SIGKILL the helios-dbt one-shot
# container mid-build during a triggered daily_close.
#
# On LocalExecutor the one-shot tool container IS the worker (ADR-010 D1);
# the scheduler itself is control plane and is deliberately NOT this act
# (ADR-014 D2 scope decision).
#
# Degrade-safely mechanism (named in ADR-014 D3 BEFORE this code): the
# Airflow task retry (dbt_build retries=2, retry_delay 2 min) over an
# idempotent rebuild — hash-ledgered raw landing, lsn-guarded CDC,
# snapshot-safe dbt build. The failed attempt exits nonzero (loud).
# Convergence proof: the SAME dag run finishes green with dbt_build
# try_number >= 2, the snapshot invariant (row count + min(dbt_valid_from))
# unchanged, and the mapped up_for_retry task-finish counter incremented by
# the killed attempt (the terminal-failure alert does NOT fire for absorbed
# retries — ADR-014 D6).
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 01 kill_worker: SIGKILL helios-dbt mid-build; recovery = Airflow task retry"
chaos_preflight

RUN_ID="chaos01-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-01-etl-${RUN_ID}.log"

# 1. pre-state
FCT_BASE="$(whq "SELECT count(*) FROM marts.fct_orders")"
SNAP_BASE="$(whq "SELECT count(*) || '|' || to_char(min(dbt_valid_from),'YYYY-MM-DD HH24:MI:SS.US') FROM snapshots.customers_snapshot")"
OPEN_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
chaos_ok "pre-state: marts.fct_orders=${FCT_BASE} snapshot( ${SNAP_BASE%|*} rows ) dq_open=${OPEN_BASE}"
[ "$OPEN_BASE" = "0" ] || chaos_fail "preflight: dq_quarantine has ${OPEN_BASE} OPEN incidents (expected 0 before chaos)"
ALERT_BASE="$(chaos_alert_state HeliosAirflowTaskFailure)"
RETRY_BASE="$(chaos_metric 'sum(airflow_task_finish_total{state="up_for_retry",dag_id="daily_close",task_id="dbt_build"})')"
chaos_note "pre-state: HeliosAirflowTaskFailure=$ALERT_BASE dbt_build up_for_retry-counter=$RETRY_BASE"

# 2. chaos act: trigger, wait mid-build, SIGKILL the one-shot
chaos_note "triggering daily_close run_id=$RUN_ID (backgrounded run-etl driver)"
bash scripts/run-etl.sh "$RUN_ID" >"$ETL_LOG" 2>&1 &
ETL_PID=$!

chaos_note "waiting for dbt_build to be mid-build (dbt one-shot container running) ..."
deadline=$(( $(date +%s) + 900 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  TI="$(chaos_task_state daily_close "$RUN_ID" dbt_build)"
  DBT_CID="$(chaos_oneshot_ids dbt | awk '{print $1}')"
  if [ "${TI%%|*}" = "running" ] && [ -n "$DBT_CID" ]; then
    break
  fi
  sleep 5
done
[ "${TI%%|*}" = "running" ] && [ -n "$DBT_CID" ] || { kill "$ETL_PID" 2>/dev/null; chaos_fail "dbt_build never reached running with a live one-shot within 900s (last ti: $TI)"; }
sleep 20   # land mid-build, not at its first second
KILL_TS="$(date +%s)"
chaos_note "CHAOS ACT: docker kill $(docker inspect -f '{{.Name}}' "$DBT_CID") mid-build"
docker kill "$DBT_CID" >/dev/null || { kill "$ETL_PID" 2>/dev/null; chaos_fail "docker kill $DBT_CID"; }

# 3. degrade-safely assertions
chaos_note "asserting the failed attempt is LOUD: task attempt fails, retry scheduled"
chaos_wait_task_state daily_close "$RUN_ID" dbt_build up_for_retry 180
chaos_note "asserting the killed one-shot is gone so the retry's compose run cannot collide"
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  [ -z "$(chaos_oneshot_ids dbt)" ] && break
  sleep 3
done
if [ -n "$(chaos_oneshot_ids dbt)" ]; then
  chaos_note "killed container lingering — removing best-effort (own one-shot, loud)"
  docker rm -f $(chaos_oneshot_ids dbt) >/dev/null || chaos_fail "could not clear killed one-shot"
fi
chaos_ok "killed one-shot removed; retry window clear"
# The ADR-013 alert rule watches TERMINAL task failure (retries exhausted);
# an absorbed retry emits ti.finish...up_for_retry instead — measured here on
# the mapped counter (found-by-verification: the first attempt asserted the
# alert and correctly failed, because this failure was absorbed by retry).
chaos_wait_metric "sum(airflow_task_finish_total{state=\"up_for_retry\",dag_id=\"daily_close\",task_id=\"dbt_build\"})" "$((RETRY_BASE + 1))" 180
chaos_note "honest note: HeliosAirflowTaskFailure stays quiet for retried transients by rule design (ADR-014 D6); it fires on terminal failures only (scenarios 04/05)"

# 4. recovery: the SAME run's retry rebuilds
chaos_note "waiting for the run to converge (retry + full rebuild inside run-etl driver)"
wait "$ETL_PID"; ETL_RC=$?
[ "$ETL_RC" = "0" ] || { cat "$ETL_LOG"; chaos_fail "run-etl exited $ETL_RC (retry path failed?)"; }
chaos_ok "run-etl driver exit 0"

# 5. convergence proof (measured)
TI="$(chaos_task_state daily_close "$RUN_ID" dbt_build)"
TRY="${TI#*|}"
[ "${TI%%|*}" = "success" ] || chaos_fail "dbt_build final state ${TI%%|*}, expected success"
[ "$TRY" -ge 2 ] 2>/dev/null || chaos_fail "dbt_build try_number=$TRY — the retry path was not exercised (kill landed outside the build?)"
chaos_ok "dbt_build converged via retry: final state=success try_number=$TRY"
SNAP_NOW="$(whq "SELECT count(*) || '|' || to_char(min(dbt_valid_from),'YYYY-MM-DD HH24:MI:SS.US') FROM snapshots.customers_snapshot")"
[ "$SNAP_NOW" = "$SNAP_BASE" ] || chaos_fail "snapshot invariant moved: baseline($SNAP_BASE) now($SNAP_NOW) — history was touched"
chaos_ok "snapshot invariant unchanged: $SNAP_NOW"
FCT_NOW="$(whq "SELECT count(*) FROM marts.fct_orders")"
chaos_ok "marts.fct_orders baseline=$FCT_BASE now=$FCT_NOW (delta $((FCT_NOW - FCT_BASE)) = attributed live-CDC churn, never a rebuild reset)"
[ "$FCT_NOW" -ge "$FCT_BASE" ] || chaos_fail "marts.fct_orders SHRANK: $FCT_BASE -> $FCT_NOW"
OPEN_NOW="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
[ "$OPEN_NOW" = "0" ] || chaos_fail "dq_quarantine open=$OPEN_NOW after a green close"

echo "[chaos] ---- run-etl driver transcript ($ETL_LOG) ----"
cat "$ETL_LOG"
chaos_ok "scenario 01 kill_worker PASSED (wall $(chaos_wall))"
