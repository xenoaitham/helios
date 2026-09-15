#!/usr/bin/env bash
# bench leg 01 — batch ingest (ADR-015 D2).
# SOAP full walk (epoch→now) is the honest cold-ish read leg: the watermark
# makes daily runs incremental, so the full read rate is measured the way
# scripts/backfill.sh measures it — --full, hash-guarded, landing 0 rows on an
# unchanged corpus (0 is the idempotence contract, asserted, not hidden).
# file + REST are re-run walls with their measured landed counts: zero-work
# legs are reported as walls, no rate is fabricated for them.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

bench_preflight

parse_last_event() { # <json-key> <capture-file> — value of <key> in the last run_succeeded event
  bench_strip_ansi < "$2" | grep '"event": "run_succeeded"' | tail -1 | grep -o "\"$1\": [0-9]*" | head -1 | awk '{print $2}'
}

echo "=== SOAP full walk (epoch->now, hash-guarded; the cold-ish read leg) ==="
bench_t0
docker compose run --rm ingest python -m ingest.run --source soap --full | tee /tmp/bench-ingest-soap.out
RC=${PIPESTATUS[0]}
bench_t1
[ "$RC" = "0" ] || bench_fail "ingest soap exited $RC"
SOAP_WALL="$(bench_elapsed)"
SOAP_READ="$(parse_last_event rows_read /tmp/bench-ingest-soap.out)"
SOAP_LANDED="$(parse_last_event rows_landed /tmp/bench-ingest-soap.out)"
[ -n "$SOAP_READ" ] && [ "$SOAP_READ" -ge 380000 ] || bench_fail "soap rows_read='${SOAP_READ:-?}' — expected >= 380000 (full walk)"
[ "$SOAP_LANDED" = "0" ] || bench_fail "soap rows_landed='$SOAP_LANDED' — expected 0 (hash-guard idempotence on unchanged corpus)"
bench_rate "$SOAP_READ" "$SOAP_WALL" "ingest.soap_full_read (READ rate; landed=0 by idempotence)"
rm -f /tmp/bench-ingest-soap.out

echo
echo "=== file feed re-run (zero-work expected — walls, not rates) ==="
bench_t0
docker compose run --rm ingest python -m ingest.run --source file | tee /tmp/bench-ingest-file.out
RC=${PIPESTATUS[0]}
bench_t1
[ "$RC" = "0" ] || bench_fail "ingest file exited $RC"
FILE_WALL="$(bench_elapsed)"
FILE_READ="$(parse_last_event rows_read /tmp/bench-ingest-file.out)"
FILE_LANDED="$(parse_last_event rows_landed /tmp/bench-ingest-file.out)"
bench_note "file leg: rows_read=${FILE_READ:-?} rows_landed=${FILE_LANDED:-?} wall=${FILE_WALL}s (no rate: the feed's new-file cadence is nightly, not continuous)"
rm -f /tmp/bench-ingest-file.out

echo
echo "=== REST full cursor walk (a true full walk every run) ==="
bench_t0
docker compose run --rm ingest python -m ingest.run --source rest | tee /tmp/bench-ingest-rest.out
RC=${PIPESTATUS[0]}
bench_t1
[ "$RC" = "0" ] || bench_fail "ingest rest exited $RC"
REST_WALL="$(bench_elapsed)"
REST_READ="$(parse_last_event rows_read /tmp/bench-ingest-rest.out)"
REST_LANDED="$(parse_last_event rows_landed /tmp/bench-ingest-rest.out)"
[ -n "$REST_READ" ] && [ "$REST_READ" -ge 7000 ] || bench_fail "rest rows_read='${REST_READ:-?}' — expected >= 7000 (products+promotions full walk)"
bench_rate "$REST_READ" "$REST_WALL" "ingest.rest_full_walk (READ rate; landed=0 by idempotence on unchanged corpus)"
rm -f /tmp/bench-ingest-rest.out

echo
echo "=== ledger after the legs ==="
docker compose run --rm ingest python -m ingest.status || bench_fail "ingest status failed"

echo
echo "[bench] leg 01-ingest DONE"
