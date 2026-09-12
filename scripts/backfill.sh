#!/usr/bin/env bash
# make backfill (ADR-010 D5): honest, replay-based backfill.
#
# The extractors accept no historical window parameters (measured: ingest.run
# takes --source and, for soap, --full only). The pipeline's history IS already
# backfilled — SOAP pulled 3 years (382,179 orders) at first landing, REST walks
# the full catalog, file lands every unseen file — and watermarks+the hash
# ledger keep it correct. What a "backfill" can therefore honestly do is a full
# replay of the daily pipeline, whose proof value is idempotency: unchanged
# content lands zero new rows (ledger), the SCD2 snapshot records zero new
# versions, mart counts stay bit-stable modulo CDC drift.
set -euo pipefail
cd "$(dirname "$0")/.."
# Bind-source parity (ADR-010 D1) — see scripts/run-etl.sh.
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"

RUN_ID="backfill-$(date -u +%Y%m%dT%H%M%SZ)"

cat <<'BANNER'
[backfill] Semantics (ADR-010 D5 — honest, no fake window):
[backfill]   No window parameters exist in the extract CLIs; the 3 years of
[backfill]   history were backfilled at first landing (watermarks + hash ledger).
[backfill]   This target = a full daily_close replay (run_id below).
[backfill]   Proof value: replay lands ZERO new content (raw.ingest_loads
[backfill]   rows_unchanged), snapshot INSERT 0 0, marts bit-stable modulo CDC
[backfill]   drift — attribute drift with `make cdc-status` first.
[backfill]   The one real window replay, SOAP --full (epoch→now, ~2.5 min,
[backfill]   hash-guarded), is a documented manual op:
[backfill]     docker compose run --rm ingest python -m ingest.run --source soap --full
BANNER
echo "[backfill] run_id: $RUN_ID"

bash scripts/run-etl.sh "$RUN_ID"

echo
echo "[backfill] replay done — run-ledger tail (rows_unchanged should dominate):"
docker compose run --rm ingest python -m ingest.status | tail -30
