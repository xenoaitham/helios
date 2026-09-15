#!/usr/bin/env bash
# bench leg 02 — CDC applied-drain rate on the LIVE, churning source
# (ADR-015 D2). The mutator is deliberately NOT paused: throughput on a
# quiescent source would measure a fiction. Numerator = delta of the raw CDC
# envelope row counts between two endpoints; denominator = the measured wall
# between the endpoint probes. Protocol: make cdc-status FIRST (lag TOTAL is
# attributed before anything else); lag must be 0 at the window end, else the
# applied count undercounts and the leg fails loudly.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

CDC_WINDOW_S="${CDC_WINDOW_S:-600}"

bench_preflight

echo "=== protocol: cdc-status FIRST (lag attribution before any measurement) ==="
bench_wait_cdc_lag_zero

echo
echo "=== mutator context: WAL churn probe (oltp-status, 15s window) ==="
docker compose run --rm oltp-seed python -m oltp.status --wal-window 15 || bench_fail "oltp-status failed"

CDC_SQL="SELECT count(*) FROM (SELECT 1 FROM raw.cdc_users UNION ALL SELECT 1 FROM raw.cdc_orders UNION ALL SELECT 1 FROM raw.cdc_order_items UNION ALL SELECT 1 FROM raw.cdc_payments) r"

echo
echo "=== drain window start ($(date -u '+%Y-%m-%dT%H:%M:%SZ')) ==="
N0="$(whq "$CDC_SQL")"
bench_t0; T0="$BENCH_T0"
echo "raw.cdc_* rows at start: $N0"
echo "sleeping ${CDC_WINDOW_S}s (mutator live) ..."
sleep "$CDC_WINDOW_S"
N1="$(whq "$CDC_SQL")"
bench_t1; T1="$BENCH_T1"
CDC_WALL="$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.3f", b-a}')"
DELTA=$((N1 - N0))
echo "raw.cdc_* rows at end:   $N1"
echo "measured window: ${CDC_WALL}s; applied events in window: $DELTA"
[ "$DELTA" -gt 0 ] || bench_fail "CDC delta over ${CDC_WALL}s window is $DELTA — mutator dead or wrong table set; refusing to print a zero-work rate"
bench_rate "$DELTA" "$CDC_WALL" "cdc.applied_drain (events applied/s, mutator live)"

echo
echo "=== end-state health ==="
bench_wait_cdc_lag_zero

echo
echo "[bench] leg 02-cdc DONE (window ${CDC_WALL}s)"
