#!/usr/bin/env bash
# make chaos-test (ADR-014): scripted chaos scenarios for HELIOS Phase 5
# item 13. Orchestrates scripts/chaos/NN-*.sh end-to-end; each scenario
# implements the five-movement shape (pre-state -> chaos act -> assert
# degraded safely -> recovery -> convergence proof, ADR-014 D5) and its full
# transcript lands in EVIDENCE/chaos-NN-name.log via tee — a FAILING scenario
# still leaves its log, and the orchestrator aborts loudly on the first
# nonzero exit so a broken scenario can never be followed by another act.
#
# Usage:
#   make chaos-test                          # all seven, in order
#   make chaos-test SCENARIO=kill_worker     # one, by name or number (01..07)
# Re-runnability contract: ADR-014 D7 (idempotent acts, quarantine grows by
# RESOLVED audit rows only, 0 OPEN at every close's end).
set -uo pipefail
cd "$(dirname "$0")/.."
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"

SCENARIO="${1:-${SCENARIO:-all}}"
mkdir -p EVIDENCE

SCENARIOS=(
  "01-kill-worker"
  "02-kill-warehouse-midbuild"
  "03-kill-oltp-midcdc"
  "04-poison-cdc"
  "05-poison-csv"
  "06-schema-drift"
  "07-api-outage"
)

if [ "$SCENARIO" != "all" ]; then
  MATCH=""
  for s in "${SCENARIOS[@]}"; do
    num="${s%%-*}"
    name="$(echo "${s#*-}" | tr '-' '_')"   # kill-worker -> kill_worker
    if [ "$SCENARIO" = "$s" ] || [ "$SCENARIO" = "$name" ] || [ "$SCENARIO" = "$num" ]; then
      MATCH="$s"
      break
    fi
  done
  [ -n "$MATCH" ] || { echo "[chaos-test] FAIL: unknown SCENARIO='$SCENARIO' (known: ${SCENARIOS[*]})"; exit 2; }
  SCENARIOS=("$MATCH")
fi

TOTAL_START="$(date +%s)"
FAILED=""
for s in "${SCENARIOS[@]}"; do
  LOG="EVIDENCE/chaos-${s}.log"
  S_START="$(date +%s)"
  echo ""
  echo "=================================================================="
  echo "[chaos-test] scenario $s -> $LOG"
  echo "=================================================================="
  bash "scripts/chaos/${s}.sh" 2>&1 | tee "$LOG"
  RC="${PIPESTATUS[0]}"
  WALL=$(( $(date +%s) - S_START ))
  if [ "$RC" != "0" ]; then
    echo "[chaos-test] scenario $s FAILED (rc=$RC, wall ${WALL}s) — log: $LOG; aborting before any further act"
    FAILED="$s"
    break
  fi
  echo "[chaos-test] scenario $s PASSED (wall ${WALL}s)"
done

TOTAL=$(( $(date +%s) - TOTAL_START ))
echo ""
if [ -n "$FAILED" ]; then
  echo "[chaos-test] FAILED at scenario $FAILED after ${TOTAL}s (evidence: EVIDENCE/chaos-*.log)"
  exit 1
fi
echo "[chaos-test] ALL ${#SCENARIOS[@]} scenario(s) PASSED — total wall ${TOTAL}s (measured)"
echo "[chaos-test] evidence: EVIDENCE/chaos-*.log"
