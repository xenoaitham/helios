#!/usr/bin/env bash
# bench leg 03 — THE full load (ADR-015 D1/D2): one real `dbt build` under the
# dbt-ol wrapper, the same invocation the DAG's dbt_build task runs
# (`docker compose build dbt && docker compose run --rm dbt build`). All 14
# models are materialized:table, so every build re-materializes the whole
# corpus (~7.3M staging + ~6.6M marts rows; stg_order_items > 5M). Numerators
# = post-build measured count(*)s; denominators = per-model walls parsed from
# dbt's own output plus the end-to-end wall. PASS=160 asserted.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

DBT_OUT=/tmp/bench-dbt-build.out

bench_preflight

echo "=== image currency check (outside the clock — mirrors the DAG's && chain) ==="
docker compose build dbt >/dev/null || bench_fail "docker compose build dbt failed"
bench_ok "dbt image current (cached no-op unless the project changed)"

count_all() { # prints "schema.table count" lines for the 15 relations
  for t in stg_users stg_orders stg_order_items stg_payments stg_file_customers stg_file_products stg_rest_products stg_rest_promotions stg_soap_orders; do
    echo "staging.$t $(whq "SELECT count(*) FROM staging.$t")"
  done
  for t in fct_orders fct_order_items dim_customer dim_product dim_date; do
    echo "marts.$t $(whq "SELECT count(*) FROM marts.$t")"
  done
  echo "snapshots.customers_snapshot $(whq "SELECT count(*) FROM snapshots.customers_snapshot")"
}

echo
echo "=== pre-build counts ==="
count_all | tee /tmp/bench-dbt-pre.txt
PRE_STG_SUM="$(awk '$1 ~ /^staging\./ {s+=$2} END{print s+0}' /tmp/bench-dbt-pre.txt)"
PRE_MART_SUM="$(awk '$1 ~ /^marts\./ {s+=$2} END{print s+0}' /tmp/bench-dbt-pre.txt)"
echo "pre staging TOTAL: $PRE_STG_SUM | pre marts TOTAL: $PRE_MART_SUM"

echo
echo "=== the full load: docker compose run --rm dbt build ==="
bench_t0
docker compose run --rm dbt build | tee "$DBT_OUT"
RC=${PIPESTATUS[0]}
bench_t1
[ "$RC" = "0" ] || bench_fail "dbt build exited $RC"
BUILD_WALL="$(bench_elapsed)"

CLEAN_OUT="$(bench_strip_ansi < "$DBT_OUT")"
grep -q 'PASS=160' <<< "$CLEAN_OUT" || bench_fail "dbt build output lacks PASS=160 (expected 1 hook + 1 snapshot + 14 tables + 144 tests) — not the full suite?"
grep -q 'Completed successfully' <<< "$CLEAN_OUT" || bench_fail "dbt build output lacks 'Completed successfully'"
bench_ok "dbt build PASS=160 (full suite ran)"

echo
echo "=== per-model walls (parsed from dbt's own output) ==="
STG_WALLS="$(grep ' OK created ' <<< "$CLEAN_OUT" | bench_parse_model_walls staging | tee /tmp/bench-dbt-stg-walls.txt)"
MART_WALLS="$(grep ' OK created ' <<< "$CLEAN_OUT" | bench_parse_model_walls marts | tee /tmp/bench-dbt-mart-walls.txt)"
SNAP_WALLS="$(grep -E ' OK snapshotted ' <<< "$CLEAN_OUT" | bench_parse_model_walls snapshots | tee /tmp/bench-dbt-snap-walls.txt)"
cat /tmp/bench-dbt-stg-walls.txt /tmp/bench-dbt-mart-walls.txt /tmp/bench-dbt-snap-walls.txt
read -r STG_WALL STG_N <<< "$(bench_sum_walls < /tmp/bench-dbt-stg-walls.txt)"
read -r MART_WALL MART_N <<< "$(bench_sum_walls < /tmp/bench-dbt-mart-walls.txt)"
read -r SNAP_WALL SNAP_N <<< "$(bench_sum_walls < /tmp/bench-dbt-snap-walls.txt)"
[ "$STG_N" = "9" ] || bench_fail "parsed $STG_N staging model walls, expected 9 — parser/output mismatch"
[ "$MART_N" = "5" ] || bench_fail "parsed $MART_N marts model walls, expected 5 — parser/output mismatch"
[ "$SNAP_N" = "1" ] || bench_fail "parsed $SNAP_N snapshot walls, expected 1 — parser/output mismatch"
bench_ok "parsed walls: staging Σ=${STG_WALL}s (9 models), marts Σ=${MART_WALL}s (5 models), snapshot=${SNAP_WALL}s"
bench_note "Σ model walls < end-to-end wall (compile + hook + test time is the difference): end-to-end ${BUILD_WALL}s"

echo
echo "=== post-build counts (the measured numerators) ==="
count_all | tee /tmp/bench-dbt-post.txt
POST_STG_SUM="$(awk '$1 ~ /^staging\./ {s+=$2} END{print s+0}' /tmp/bench-dbt-post.txt)"
POST_MART_SUM="$(awk '$1 ~ /^marts\./ {s+=$2} END{print s+0}' /tmp/bench-dbt-post.txt)"
SNAP_ROWS="$(awk '$1 == "snapshots.customers_snapshot" {print $2}' /tmp/bench-dbt-post.txt)"
echo "post staging TOTAL: $POST_STG_SUM | post marts TOTAL: $POST_MART_SUM | snapshot: $SNAP_ROWS"

# Sanity: a full re-materialization moves counts only by live churn (<2% here);
# a bigger swing means the bench measured something else. Loud fail, not a note.
for pair in "staging:$PRE_STG_SUM:$POST_STG_SUM" "marts:$PRE_MART_SUM:$POST_MART_SUM"; do
  IFS=: read -r what pre post <<< "$pair"
  awk -v w="$what" -v a="$pre" -v b="$post" 'BEGIN{ d=(b-a)/a; if (d < -0.02 || d > 0.02) exit 1 }' \
    || bench_fail "$what count moved $pre -> $post (>2%) — outside live-churn bounds; investigate before trusting this pass"
  bench_ok "$what rows $pre -> $post (within ±2% live-churn bound)"
done
ITEMS_STG="$(awk '$1 == "staging.stg_order_items" {print $2}' /tmp/bench-dbt-post.txt)"
ITEMS_MART="$(awk '$1 == "marts.fct_order_items" {print $2}' /tmp/bench-dbt-post.txt)"
ITEMS_STG_WALL="$(awk '$1 == "staging.stg_order_items" {print $2}' /tmp/bench-dbt-stg-walls.txt)"
ITEMS_MART_WALL="$(awk '$1 == "marts.fct_order_items" {print $2}' /tmp/bench-dbt-mart-walls.txt)"

echo
echo "=== rates (measured rows ÷ measured walls) ==="
bench_rate "$POST_STG_SUM" "$STG_WALL" "dbt.staging_materialize (9 tables)"
bench_rate "$POST_MART_SUM" "$MART_WALL" "dbt.marts_materialize (5 tables)"
bench_rate "$SNAP_ROWS" "$SNAP_WALL" "dbt.snapshot_scan (SCD2 in-place; 0 new versions expected)"
TOTAL_ROWS=$((POST_STG_SUM + POST_MART_SUM))
bench_rate "$TOTAL_ROWS" "$BUILD_WALL" "dbt.total (14 tables + snapshot, end-to-end wall)"
bench_rate "$ITEMS_STG" "$ITEMS_STG_WALL" "dbt.stg_order_items (THE 5M-row table)"
bench_rate "$ITEMS_MART" "$ITEMS_MART_WALL" "dbt.fct_order_items"

echo
echo "=== invariants after the full load ==="
bench_snapshot_invariant
bench_quarantine_zero_open
rm -f "$DBT_OUT" /tmp/bench-dbt-pre.txt /tmp/bench-dbt-post.txt /tmp/bench-dbt-stg-walls.txt /tmp/bench-dbt-mart-walls.txt /tmp/bench-dbt-snap-walls.txt

echo
echo "[bench] leg 03-dbt DONE (end-to-end wall ${BUILD_WALL}s)"
