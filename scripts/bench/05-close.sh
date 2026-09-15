#!/usr/bin/env bash
# bench leg 05 — one real orchestrated daily_close end-to-end (ADR-015 D2),
# the platform's actual production path (scripts/run-etl.sh → trigger → poll).
# Walls come from BOTH clocks: the client-observed run-etl wall, and the
# server-side dag_run/task_instance timestamps in the airflow metadata DB —
# the honest cross-proof of legs 03/04. Composite rows/sec = the rows this
# close re-materialized ÷ the server-side dag wall.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

bench_preflight

RUN_ID="bench-close-${BENCH_LABEL:-manual}-$(date -u +%H%M%S)"

STG_SQL="SELECT count(*) FROM (SELECT 1 FROM staging.stg_users UNION ALL SELECT 1 FROM staging.stg_orders UNION ALL SELECT 1 FROM staging.stg_order_items UNION ALL SELECT 1 FROM staging.stg_payments UNION ALL SELECT 1 FROM staging.stg_file_customers UNION ALL SELECT 1 FROM staging.stg_file_products UNION ALL SELECT 1 FROM staging.stg_rest_products UNION ALL SELECT 1 FROM staging.stg_rest_promotions UNION ALL SELECT 1 FROM staging.stg_soap_orders) s"
MART_SQL="SELECT count(*) FROM (SELECT 1 FROM marts.fct_orders UNION ALL SELECT 1 FROM marts.fct_order_items UNION ALL SELECT 1 FROM marts.dim_customer UNION ALL SELECT 1 FROM marts.dim_product UNION ALL SELECT 1 FROM marts.dim_date) m"

echo "=== the close: bash scripts/run-etl.sh $RUN_ID ==="
bench_t0
bash scripts/run-etl.sh "$RUN_ID"
RC=$?
bench_t1
[ "$RC" = "0" ] || bench_fail "run-etl exited $RC"
CLIENT_WALL="$(bench_elapsed)"

echo
echo "=== server-side clocks (airflow metadata DB, dag_run + task_instance) ==="
ROW="$(afdbq "SELECT state || '|' || COALESCE(EXTRACT(EPOCH FROM (end_date - start_date))::text,'') FROM dag_run WHERE dag_id='daily_close' AND run_id='$RUN_ID'")"
STATE="${ROW%%|*}"; DAG_WALL="${ROW#*|}"
[ "$STATE" = "success" ] || bench_fail "dag run $RUN_ID state=$STATE (expected success)"
[ -n "$DAG_WALL" ] || bench_fail "dag run $RUN_ID has no end_date — cannot measure server-side wall"
echo "dag $RUN_ID: state=$STATE server-side wall=${DAG_WALL}s (client-observed incl. poll granularity: ${CLIENT_WALL}s)"

echo
echo "task walls (task_instance start->end, server-side):"
afdbq "SELECT task_id || ': ' || COALESCE(EXTRACT(EPOCH FROM (end_date - start_date))::text,'running') || 's (try ' || try_number || ')' FROM task_instance WHERE dag_id='daily_close' AND run_id='$RUN_ID' ORDER BY start_date"

echo
echo "=== rows this close re-materialized (measured post-close) ==="
STG_SUM="$(whq "$STG_SQL")"
MART_SUM="$(whq "$MART_SQL")"
echo "staging TOTAL: $STG_SUM | marts TOTAL: $MART_SUM"
TOTAL_ROWS=$((STG_SUM + MART_SUM))
bench_rate "$TOTAL_ROWS" "$DAG_WALL" "close.end_to_end (14 tables re-materialized ÷ server-side dag wall)"
DBT_TI="$(afdbq "SELECT COALESCE(EXTRACT(EPOCH FROM (end_date - start_date))::text,'') FROM task_instance WHERE dag_id='daily_close' AND run_id='$RUN_ID' AND task_id='dbt_build'")"
[ -n "$DBT_TI" ] && bench_rate "$TOTAL_ROWS" "$DBT_TI" "close.dbt_build_task (same numerator ÷ the orchestrated build task's server-side wall; incl. image-build + wrapper)" \
  || bench_note "dbt_build task wall unavailable"

echo
echo "=== read-only metrics cross-proof (Prometheus, dagrun duration surface) ==="
curl -sf --max-time 10 "$PROM_URL/api/v1/query" --get --data-urlencode 'query=airflow_dagrun_duration_seconds_count{dag_id="daily_close",status="success"}' \
  | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print("airflow_dagrun_duration_seconds_count[daily_close,success] =", sum(float(x["value"][1]) for x in r))' \
  || bench_note "prometheus probe unavailable (metrics stack down is a platform incident, not a bench number)"

echo
echo "=== closing invariants ==="
bench_snapshot_invariant
bench_quarantine_zero_open
bench_wait_cdc_lag_zero

echo
echo "[bench] leg 05-close DONE (client wall ${CLIENT_WALL}s, server dag wall ${DAG_WALL}s)"
