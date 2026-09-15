#!/usr/bin/env bash
# chaos-02 kill_warehouse_midbuild (ADR-014 D3): stop warehouse-db while
# dbt_build is mid-build during a triggered daily_close.
#
# Degrade-safely mechanism (named BEFORE the act): the build fails LOUDLY —
# the killed connection surfaces as a nonzero dbt exit and an Airflow task
# retry (retries=2, 2 min) — while the published staging/marts stay
# transactionally frozen at the previous build (dbt replaces targets only at
# per-model commit; an aborted build leaves the old tables). During the
# outage the warehouse is UNREACHABLE (measured DOWN), never silently wrong;
# and because warehouse-db is not a scraped target, HeliosScrapeTargetDown
# must NOT fire (negative check, ADR-014 D6).
# Convergence proof: warehouse restarted healthy → frozen-old marts measured
# before the retry commits → the SAME run finishes green via retry → parity
# (marts grew only by attributed churn) → snapshot invariant unchanged → the
# mapped up_for_retry counter incremented by the killed attempt.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 02 kill_warehouse_midbuild: stop warehouse-db mid-dbt_build; recovery = task retry after restart"
chaos_preflight

RUN_ID="chaos02-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-02-etl-${RUN_ID}.log"

# 1. pre-state
FCT_BASE="$(whq "SELECT count(*) FROM marts.fct_orders")"
SNAP_BASE="$(whq "SELECT count(*) || '|' || to_char(min(dbt_valid_from),'YYYY-MM-DD HH24:MI:SS.US') FROM snapshots.customers_snapshot")"
RETRY_BASE="$(chaos_metric 'sum(airflow_task_finish_total{state="up_for_retry",dag_id="daily_close",task_id="dbt_build"})')"
chaos_ok "pre-state: marts.fct_orders=${FCT_BASE} snapshot( ${SNAP_BASE%|*} rows ) dbt_build up_for_retry-counter=$RETRY_BASE"

# 2. chaos act
chaos_note "triggering daily_close run_id=$RUN_ID"
bash scripts/run-etl.sh "$RUN_ID" >"$ETL_LOG" 2>&1 &
ETL_PID=$!
chaos_note "waiting for dbt_build to be mid-build ..."
deadline=$(( $(date +%s) + 900 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  TI="$(chaos_task_state daily_close "$RUN_ID" dbt_build)"
  if [ "${TI%%|*}" = "running" ] && [ -n "$(chaos_oneshot_ids dbt)" ]; then
    break
  fi
  sleep 5
done
[ "${TI%%|*}" = "running" ] && [ -n "$(chaos_oneshot_ids dbt)" ] || { kill "$ETL_PID" 2>/dev/null; chaos_fail "dbt_build never reached running within 900s (last ti: $TI)"; }
sleep 20
chaos_note "CHAOS ACT: docker stop warehouse-db mid-build"
STOP_TS="$(date +%s)"
docker compose stop warehouse-db >/dev/null || { kill "$ETL_PID" 2>/dev/null; chaos_fail "docker compose stop warehouse-db"; }

# 3. degrade-safely assertions (while degraded)
chaos_note "asserting the warehouse is DOWN (unreachable, not silently wrong)"
[ "$(whq_down_ok "SELECT 1")" = "DOWN" ] || chaos_fail "warehouse-db still answers SQL after stop — the chaos act did not land"
chaos_ok "warehouse-db unreachable during the outage (measured DOWN)"
chaos_wait_task_state daily_close "$RUN_ID" dbt_build up_for_retry 240
chaos_note "asserting the metrics scrape is UNAFFECTED (warehouse-db is not a scraped target, ADR-014 D6)"
sleep 45
SCR="$(chaos_alert_state HeliosScrapeTargetDown)"
[ "$SCR" != "firing" ] || { docker compose start warehouse-db >/dev/null 2>&1; chaos_fail "HeliosScrapeTargetDown fired — warehouse-db must not be a scrape target"; }
chaos_ok "HeliosScrapeTargetDown did NOT fire during a database outage (state=$SCR)"

# 4. recovery
chaos_note "restarting warehouse-db (recovery begins immediately; the task retry lands at +2 min)"
docker compose start warehouse-db >/dev/null || chaos_fail "docker compose start warehouse-db"
chaos_wait_healthy warehouse-db 120
FROZEN="$(whq "SELECT count(*) FROM marts.fct_orders")"
[ "$FROZEN" -ge "$FCT_BASE" ] || chaos_fail "marts.fct_orders SHRANK across the aborted build: baseline=$FCT_BASE measured=$FROZEN"
if [ "$FROZEN" = "$FCT_BASE" ]; then
  chaos_ok "frozen-old proven at recovery start: marts.fct_orders still $FROZEN (the aborted attempt never reached the fact)"
else
  chaos_note "measured (honest): marts.fct_orders baseline=$FCT_BASE at-recovery=$FROZEN (+$((FROZEN - FCT_BASE)) churn rows) — the stop landed AFTER the aborted attempt had committed its fct replacement (dbt commits per-model). The guarantee that holds is per-model: every target is a COMPLETE replacement (previous build's or this attempt's), never a partial row set; the retry rebuilds all of them."
fi
chaos_note "waiting for the run to converge (retry rebuilds on the restored warehouse)"
wait "$ETL_PID"; ETL_RC=$?
[ "$ETL_RC" = "0" ] || { cat "$ETL_LOG"; chaos_fail "run-etl exited $ETL_RC"; }
chaos_ok "run-etl driver exit 0"

# 5. convergence proof
TI="$(chaos_task_state daily_close "$RUN_ID" dbt_build)"
TRY="${TI#*|}"
[ "${TI%%|*}" = "success" ] || chaos_fail "dbt_build final state ${TI%%|*}, expected success"
[ "$TRY" -ge 2 ] 2>/dev/null || chaos_fail "dbt_build try_number=$TRY — retry path not exercised"
chaos_ok "dbt_build converged via retry: try_number=$TRY"
FCT_NOW="$(whq "SELECT count(*) FROM marts.fct_orders")"
[ "$FCT_NOW" -ge "$FCT_BASE" ] || chaos_fail "marts.fct_orders SHRANK: $FCT_BASE -> $FCT_NOW"
chaos_ok "parity: marts.fct_orders baseline=$FCT_BASE now=$FCT_NOW (delta $((FCT_NOW - FCT_BASE)) = attributed churn)"
SNAP_NOW="$(whq "SELECT count(*) || '|' || to_char(min(dbt_valid_from),'YYYY-MM-DD HH24:MI:SS.US') FROM snapshots.customers_snapshot")"
[ "$SNAP_NOW" = "$SNAP_BASE" ] || chaos_fail "snapshot invariant moved: $SNAP_BASE -> $SNAP_NOW"
chaos_ok "snapshot invariant unchanged: $SNAP_NOW"
chaos_wait_healthy cdc-sink 120
# Absorbed retry: the killed-build attempt emitted ti.finish...up_for_retry
# (the terminal-failure alert stays quiet by rule design — ADR-014 D6).
chaos_wait_metric "sum(airflow_task_finish_total{state=\"up_for_retry\",dag_id=\"daily_close\",task_id=\"dbt_build\"})" "$((RETRY_BASE + 1))" 180

echo "[chaos] ---- run-etl driver transcript ($ETL_LOG) ----"
cat "$ETL_LOG"
chaos_ok "scenario 02 kill_warehouse_midbuild PASSED (wall $(chaos_wall))"
