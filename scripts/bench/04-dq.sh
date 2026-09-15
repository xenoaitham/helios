#!/usr/bin/env bash
# bench leg 04 — the Great Expectations semantic gate (ADR-011), timed the way
# the DAG's dq_gate task runs it (`docker compose build dq && docker compose
# run --rm dq python -m dq gate`). Numerator = the measured row count of the
# six suite tables (the gate reads FROZEN staging/marts tables in full);
# denominator = the gate wall. Exit 0 and 0 OPEN incidents asserted.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

bench_preflight

echo "=== image currency check (outside the clock — mirrors the DAG's && chain) ==="
docker compose build dq >/dev/null || bench_fail "docker compose build dq failed"
bench_ok "dq image current"

echo
echo "=== gate scope (the 6 suite tables, measured pre-gate) ==="
SCOPE_SQL="SELECT count(*) FROM (SELECT 1 FROM staging.stg_payments UNION ALL SELECT 1 FROM staging.stg_orders UNION ALL SELECT 1 FROM staging.stg_order_items UNION ALL SELECT 1 FROM staging.stg_file_customers UNION ALL SELECT 1 FROM staging.stg_soap_orders UNION ALL SELECT 1 FROM marts.fct_orders) s"
for t in staging.stg_payments staging.stg_orders staging.stg_order_items staging.stg_file_customers staging.stg_soap_orders marts.fct_orders; do
  echo "$t $(whq "SELECT count(*) FROM $t")"
done
SCOPE="$(whq "$SCOPE_SQL")"
echo "gate scope TOTAL: $SCOPE rows"
[ "$SCOPE" -ge 8000000 ] || bench_fail "gate scope $SCOPE rows — expected ~8.4M (suite tables shrank?)"

echo
echo "=== the gate: docker compose run --rm dq python -m dq gate ==="
bench_t0
docker compose run --rm dq python -m dq gate | tee /tmp/bench-dq-gate.out
RC=${PIPESTATUS[0]}
bench_t1
[ "$RC" = "0" ] || bench_fail "dq gate exited $RC (verdict 1 = data failure, 2 = config, 3 = crash)"
GATE_WALL="$(bench_elapsed)"

echo
bench_rate "$SCOPE" "$GATE_WALL" "dq.gate_scan (6 tables, semantic expectations)"
bench_quarantine_zero_open
rm -f /tmp/bench-dq-gate.out

echo
echo "[bench] leg 04-dq DONE (gate wall ${GATE_WALL}s)"
