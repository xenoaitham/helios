#!/usr/bin/env bash
# make run-etl (ADR-010 D4): unpause → trigger the master daily_close DAG with a
# known run_id → poll to a terminal state → nonzero exit on failure/timeout.
#
# Drives the airflow CLI inside the scheduler container (docker compose exec) —
# control-plane commands the scheduler itself trusts; no REST call, no
# credentials in this script. Every setup step's exit code is checked: a
# vacuous PASS is impossible.
#
# Usage: scripts/run-etl.sh [run_id]     (default: etl-<UTC ts>)
# Overridable: RUN_ETL_TIMEOUT_S (default 2700 — covers the queued case where a
# scheduled daily_close is already active and max_active_runs=1 queues ours).
set -uo pipefail
cd "$(dirname "$0")/.."
# Bind-source parity (ADR-010 D1): compose resolves the scheduler's repo mount
# from this var; the Makefile exports it, default to the repo root otherwise.
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

DAG_ID=daily_close
RUN_ID="${1:-etl-$(date -u +%Y%m%dT%H%M%SZ)}"
EXEC_DATE="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
TIMEOUT_S="${RUN_ETL_TIMEOUT_S:-2700}"
POLL_S=10

fail() { echo "[run-etl] FAIL: $*"; exit 1; }
af()   { docker compose exec -T airflow-scheduler airflow "$@"; }

HEALTH="$(docker inspect -f '{{.State.Health.Status}}' helios-airflow-scheduler 2>/dev/null || echo missing)"
[ "$HEALTH" = "healthy" ] || fail "airflow-scheduler container health=$HEALTH (make up first)"

echo "[run-etl] scheduler healthy; unpausing DAGs (idempotent — compose pauses DAGs at creation)"
for d in "$DAG_ID" ingest_soap ingest_file ingest_rest; do
  af dags unpause "$d" >/dev/null || fail "airflow dags unpause $d (DAG import error? run: airflow dags list-import-errors)"
done

echo "[run-etl] triggering $DAG_ID run_id=$RUN_ID exec_date=$EXEC_DATE"
af dags trigger "$DAG_ID" -r "$RUN_ID" -e "$EXEC_DATE" >/dev/null || fail "airflow dags trigger"

echo "[run-etl] polling every ${POLL_S}s, timeout ${TIMEOUT_S}s ..."
deadline=$(( $(date +%s) + TIMEOUT_S ))
while :; do
  # Dag-run state is read from the metadata DB via SQL, NOT from
  # `airflow dags state` CLI output: CLI stdout interleaves log lines, and
  # during a metrics-stack outage the StatsClient fallback ERROR line
  # ("Could not configure StatsClient ... using NoStatsLogger", ADR-013 D7
  # drill, 2026-09-13) polluted the state parse ('...ERROR...running') and
  # failed the driver while the pipeline itself was healthy. SQL is immune.
  state="$(docker compose exec -T airflow-db psql -U airflow -d airflow -tAc \
    "SELECT state FROM dag_run WHERE dag_id='$DAG_ID' AND run_id='$RUN_ID'" | tail -1)"
  [ -z "$state" ] && state=pending   # row not created yet (trigger committed)
  case "$state" in
    success)
      # Guard against the vacuous-success trap (measured 2026-09-11): a run whose
      # execution_date precedes the DAG start_date (or a taskless DAG) "succeeds"
      # with ZERO task instances. Success counts only if every task ran and passed.
      tis="$(docker compose exec -T airflow-db psql -U airflow -d airflow -tAc \
        "SELECT count(*), count(*) FILTER (WHERE state='success') FROM task_instance WHERE dag_id='$DAG_ID' AND run_id='$RUN_ID'")"
      total="${tis%%|*}"; ok="${tis#*|}"
      if [ "$total" -gt 0 ] && [ "$total" = "$ok" ]; then
        echo "[run-etl] $DAG_ID run $RUN_ID: SUCCESS ($ok/$total tasks green)"
        exit 0
      fi
      fail "$DAG_ID run $RUN_ID reported success but tasks: total=$total succeeded=$ok (vacuous run?)"
      ;;
    failed)
      echo "[run-etl] $DAG_ID run $RUN_ID: FAILED — task instances:"
      docker compose exec -T airflow-db psql -U airflow -d airflow \
        -c "SELECT task_id, state, try_number FROM task_instance WHERE dag_id='$DAG_ID' AND run_id='$RUN_ID' ORDER BY task_id" \
        || true
      echo "[run-etl] task logs: Airflow UI :8080 → $DAG_ID → $RUN_ID, or make airflow-logs"
      exit 1
      ;;
    pending|queued|running)
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "[run-etl] $DAG_ID run $RUN_ID: TIMEOUT after ${TIMEOUT_S}s (last state: $state)"
        echo "[run-etl] (a scheduled run may be occupying max_active_runs=1 — check the UI :8080)"
        exit 1
      fi
      sleep "$POLL_S"
      ;;
    *)
      fail "unexpected dag-run state '$state' for $RUN_ID"
      ;;
  esac
done
