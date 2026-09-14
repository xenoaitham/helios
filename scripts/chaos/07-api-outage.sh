#!/usr/bin/env bash
# chaos-07 api_outage (ADR-014 D3): TOTAL outage of BOTH HTTP sources —
# rest-mock AND soap-service unreachable while a close is ingesting (the
# 429/500 retry logic was drilled in Phase 1; a full outage never was).
#
# MEASURED DESIGN CORRECTIONS (three scripted attempts, 2026-09-13) — the
# tool-container dependency contract (ADR-010 D1) self-heals source outages
# at the orchestration layer, so the outage that HOLDS must be a PERSISTENT
# network partition enforced against the healer itself:
#   1. `docker compose stop` of the sources: the scheduler's very next
#      `compose run ingest` STARTS the stopped dependencies (depends_on
#      service_healthy) — sources "Up" again within seconds; the children
#      succeeded 8 s into the run. Instant self-healing.
#   2. `docker compose pause`: the ingest_file one-shot BLOCKED ~10 minutes
#      waiting for the paused dependencies to turn healthy, then proceeded —
#      the pipeline STALLED but never failed (nothing half-landed).
#   3. `docker network disconnect` once: compose run's dependency-ensure
#      RECONNECTED the sources before the children's one-shots started —
#      children succeeded; the partition never held.
#   4. THEREFORE the act is a WATCHDOG-ENFORCED partition: a background loop
#      re-disconnects the sources whenever anything reattaches them (the
#      compose-run healer loses the race, 0.5 s vs its ensure step). The
#      containers stay healthy (their healthchecks run in-container) — the
#      partition is invisible to healthchecks, DNS just stops resolving.
#
# Degrade-safely mechanism (named BEFORE the act): the child ingest tasks
# fail LOUDLY (DNS/connection errors burn the ingest lib's documented retry
# budget — 8 attempts — then exit nonzero; Airflow retries 2 × 5 min exp
# backoff) — and because extraction is watermark/hash-guarded, a failed walk
# lands nothing half-kept.
# Convergence proof: partition lifted (operator recovery; note: attempt-2's
# own compose run would also self-heal — measured in attempt 3) → the SAME
# run's retries succeed (child tasks finish success with try_number >= 2) →
# raw row counts for soap/rest BIT-IDENTICAL to pre-run (the idempotence
# proof) → the mapped up_for_retry counter incremented by both failed
# attempts (absorbed retries do NOT fire the terminal-failure alert —
# ADR-014 D6).
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 07 api_outage: watchdog-enforced network partition of rest-mock + soap-service; recovery = partition lifted + in-run retries"
chaos_preflight

RUN_ID="chaos07-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-07-etl-${RUN_ID}.log"
CHAOS_NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' helios-rest-mock)"
[ -n "$CHAOS_NET" ] || chaos_fail "could not discover the compose network of rest-mock"
WATCHDOG_FLAG="/tmp/chaos07-watchdog.flag"

# Guard against leaving the stack degraded: lift the partition and stop the
# watchdog on script EXIT (assert failures included). Reconnect WITH the
# compose service alias — a plain `docker network connect` strips the alias
# and leaves the service name unresolvable (measured 2026-09-13: aliases=[]
# after reconnect, consumer DNS broken until a compose --force-recreate).
chaos_lift_partition() {
  echo off > "$WATCHDOG_FLAG" 2>/dev/null || true
  for svc in rest-mock soap-service; do
    ATT="$(docker inspect -f "{{range \$k,\$v := .NetworkSettings.Networks}}{{\$k}} {{end}}" "helios-$svc" 2>/dev/null | grep -c "$CHAOS_NET" || true)"
    if [ "$ATT" = "0" ]; then
      docker network connect --alias "$svc" "$CHAOS_NET" "helios-$svc" >/dev/null 2>&1 \
        && echo "[chaos] reconnected $svc (with compose service alias)"
    fi
  done
}
trap chaos_lift_partition EXIT

# 1. pre-state
SOAP_BASE="$(whq "SELECT count(*) FROM raw.soap_orders")"
RESTP_BASE="$(whq "SELECT count(*) FROM raw.rest_products")"
RESTPR_BASE="$(whq "SELECT count(*) FROM raw.rest_promotions")"
RETRY_BASE="$(chaos_metric 'sum(airflow_task_finish_total{state="up_for_retry"})')"
chaos_ok "pre-state: raw.soap_orders=$SOAP_BASE raw.rest_products=$RESTP_BASE raw.rest_promotions=$RESTPR_BASE up_for_retry-counter=$RETRY_BASE"

# 2. chaos act: persistent partition (watchdog wins every heal race)
chaos_note "CHAOS ACT: watchdog-enforced network disconnect on $CHAOS_NET (re-partitioned within 0.5s whenever anything heals it)"
docker network disconnect "$CHAOS_NET" helios-rest-mock >/dev/null || chaos_fail "disconnect rest-mock"
docker network disconnect "$CHAOS_NET" helios-soap-service >/dev/null || chaos_fail "disconnect soap-service"
echo on > "$WATCHDOG_FLAG"
chaos_partition_watchdog() {
  while [ "$(cat "$WATCHDOG_FLAG" 2>/dev/null || echo off)" = "on" ]; do
    for c in helios-rest-mock helios-soap-service; do
      ATT="$(docker inspect -f "{{range \$k,\$v := .NetworkSettings.Networks}}{{\$k}} {{end}}" "$c" 2>/dev/null | grep -c "$CHAOS_NET" || true)"
      [ "$ATT" = "0" ] || docker network disconnect "$CHAOS_NET" "$c" >/dev/null 2>&1
    done
    sleep 0.5
  done
}
chaos_partition_watchdog &
WATCHDOG_PID=$!

sleep 3
H_R="$(chaos_health rest-mock)"; H_S="$(chaos_health soap-service)"
[ "$H_R" = "healthy" ] && [ "$H_S" = "healthy" ] || chaos_note "NOTE: source health while partitioned: rest=$H_R soap=$H_S (healthchecks run in-container; the partition is invisible to them — that is the point)"
FROM_SCHED="$(docker exec helios-airflow-scheduler python3 -c "
import urllib.request
try:
    urllib.request.urlopen('http://rest-mock:8000/health', timeout=3)
    print('reachable')
except Exception as e:
    print('unreachable:', type(e).__name__)" 2>/dev/null || echo probe-failed)"
[[ "$FROM_SCHED" == unreachable* ]] || { chaos_lift_partition; chaos_fail "rest-mock still reachable from the consumer path: $FROM_SCHED — the act did not land"; }
chaos_ok "both HTTP sources PARTITIONED persistently (measured: healthy containers, '$FROM_SCHED' from the consumer path, watchdog enforcing)"

# 3. degraded run: attempt-1 failures are loud and retried, nothing half-lands
EXEC_TS="$(date -u +%Y-%m-%d\ %H:%M:%S)"
chaos_note "triggering daily_close run_id=$RUN_ID under the persistent partition"
bash scripts/run-etl.sh "$RUN_ID" >"$ETL_LOG" 2>&1 &
ETL_PID=$!
chaos_note "discovering the triggered child runs (ingest_soap / ingest_rest) ..."
deadline=$(( $(date +%s) + 600 ))
SOAP_CHILD=""; REST_CHILD=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  [ -z "$SOAP_CHILD" ] && SOAP_CHILD="$(chaos_latest_run_since ingest_soap "$EXEC_TS")"
  [ -z "$REST_CHILD" ] && REST_CHILD="$(chaos_latest_run_since ingest_rest "$EXEC_TS")"
  [ -n "$SOAP_CHILD" ] && [ -n "$REST_CHILD" ] && break
  sleep 5
done
[ -n "$SOAP_CHILD" ] && [ -n "$REST_CHILD" ] || { kill "$ETL_PID" 2>/dev/null; chaos_lift_partition; chaos_fail "child runs not discovered within 600s"; }
chaos_ok "child runs: ingest_soap=$SOAP_CHILD ingest_rest=$REST_CHILD"

chaos_note "asserting attempt-1 failures are LOUD and scheduled for retry (not silent, not skipped)"
chaos_wait_task_state ingest_soap "$SOAP_CHILD" ingest_soap up_for_retry 900
chaos_wait_task_state ingest_rest "$REST_CHILD" ingest_rest up_for_retry 900
chaos_ok "degrade-safely measured: both source tasks failed attempt 1 and entered the Airflow retry queue"

# 4. recovery: lift the partition (the operator action; attempt-2's compose
# run dependency-ensure would also self-heal — measured in attempt 3)
chaos_note "RECOVERY: lifting the partition (watchdog stops FIRST, then sources reconnect; +5 min backoff covers the second attempt)"
chaos_lift_partition
wait "$WATCHDOG_PID" 2>/dev/null || true
chaos_lift_partition   # the stopped watchdog's last tick may have re-partitioned — lift once more
chaos_wait_healthy rest-mock 120
chaos_wait_healthy soap-service 120
FROM_SCHED2="$(docker exec helios-airflow-scheduler python3 -c "
import urllib.request
urllib.request.urlopen('http://rest-mock:8000/health', timeout=3)
print('reachable')" 2>/dev/null || echo unreachable)"
[ "$FROM_SCHED2" = "reachable" ] || chaos_fail "consumer path still broken after recovery: $FROM_SCHED2"
chaos_ok "consumer path restored (measured: $FROM_SCHED2 from the scheduler)"

chaos_note "waiting for the run to converge (retries + dbt + dq inside the run-etl driver)"
wait "$ETL_PID"; ETL_RC=$?
[ "$ETL_RC" = "0" ] || { cat "$ETL_LOG"; chaos_fail "run-etl exited $ETL_RC"; }
chaos_ok "run-etl driver exit 0"

# 5. convergence proof
TI="$(chaos_task_state ingest_soap "$SOAP_CHILD" ingest_soap)"; SOAP_TRY="${TI#*|}"
[ "${TI%%|*}" = "success" ] || chaos_fail "ingest_soap final state ${TI%%|*}"
[ "$SOAP_TRY" -ge 2 ] 2>/dev/null || chaos_fail "ingest_soap try_number=$SOAP_TRY — retry path not exercised"
TI="$(chaos_task_state ingest_rest "$REST_CHILD" ingest_rest)"; REST_TRY="${TI#*|}"
[ "${TI%%|*}" = "success" ] || chaos_fail "ingest_rest final state ${TI%%|*}"
[ "$REST_TRY" -ge 2 ] 2>/dev/null || chaos_fail "ingest_rest try_number=$REST_TRY — retry path not exercised"
chaos_ok "both child tasks converged via retry: ingest_soap try=$SOAP_TRY ingest_rest try=$REST_TRY"
SOAP_NOW="$(whq "SELECT count(*) FROM raw.soap_orders")"
RESTP_NOW="$(whq "SELECT count(*) FROM raw.rest_products")"
RESTPR_NOW="$(whq "SELECT count(*) FROM raw.rest_promotions")"
[ "$SOAP_NOW" = "$SOAP_BASE" ] || chaos_fail "raw.soap_orders moved during a total outage: $SOAP_BASE -> $SOAP_NOW"
[ "$RESTP_NOW" = "$RESTP_BASE" ] || chaos_fail "raw.rest_products moved: $RESTP_BASE -> $RESTP_NOW"
[ "$RESTPR_NOW" = "$RESTPR_BASE" ] || chaos_fail "raw.rest_promotions moved: $RESTPR_BASE -> $RESTPR_NOW"
chaos_ok "idempotence proven: raw counts BIT-IDENTICAL through failure+retry (soap=$SOAP_NOW rest_products=$RESTP_NOW rest_promotions=$RESTPR_NOW)"
# Absorbed retries: each failed attempt emitted ti.finish...up_for_retry
# (the terminal-failure alert stays quiet by rule design — ADR-014 D6).
chaos_wait_metric "sum(airflow_task_finish_total{state=\"up_for_retry\"})" "$((RETRY_BASE + 2))" 180

echo "[chaos] ---- run-etl driver transcript ($ETL_LOG) ----"
cat "$ETL_LOG"
chaos_ok "scenario 07 api_outage PASSED (wall $(chaos_wall))"
