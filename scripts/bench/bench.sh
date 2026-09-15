#!/usr/bin/env bash
# make bench — one full pass, legs 00→05 in order (ADR-015 D5/D7).
# Each leg tees its full transcript to EVIDENCE/bench-<label>-<leg>.log; the
# default label is the UTC timestamp so repeated passes never overwrite each
# other (the ≥2-pass variance mandate). Any leg failure aborts the pass loudly.
# Re-runnable by construction: every leg is an idempotent pipeline action the
# platform already runs daily.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"

LABEL="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
export BENCH_LABEL="$LABEL"
mkdir -p EVIDENCE
START="$(date +%s)"

echo "[bench] pass label: $LABEL — transcripts: EVIDENCE/bench-${LABEL}-*.log"
for leg in 00-env 01-ingest 02-cdc 03-dbt 04-dq 05-close; do
  log="EVIDENCE/bench-${LABEL}-${leg}.log"
  echo
  echo "[bench] ======== leg $leg -> $log ($(date -u '+%H:%M:%SZ')) ========"
  bash "scripts/bench/${leg}.sh" 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  if [ "$rc" != "0" ]; then
    echo "[bench] FAIL: leg $leg exited $rc — transcript: $log (pass aborted loudly; a vacuous PASS is worse than a FAIL)"
    exit "$rc"
  fi
done

echo
echo "[bench] pass $LABEL COMPLETE: 6/6 legs green, wall $(( $(date +%s) - START ))s"
