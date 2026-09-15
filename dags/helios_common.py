"""Shared constants for HELIOS DAGs (ADR-010).

Deliberately boring: no SQL, no HTTP, no credential literals — these DAGs
orchestrate the existing one-shot TOOL containers through the host's rootless
docker socket, using the exact commands the make targets use (ADR-010 D1).
SQL lives in dbt files and scripts/smoke-test.sh; credentials are compose/.env
injected into the tool containers, never in DAG code.
"""

import os
from datetime import timedelta

import pendulum

# The repo, bind-mounted read-only into the scheduler at its EXACT host path
# (HELIOS_PROJECT_DIR, exported by the Makefile — bind-source parity, ADR-010
# D1). Same compose project (name: helios), same daemon socket → same one-shot
# containers `make ingest-soap` / `make dbt-build` would create.
HELIOS_PROJECT_DIR = os.environ.get("HELIOS_PROJECT_DIR", "/helios")

COMPOSE_PREFIX = f"cd {HELIOS_PROJECT_DIR} && docker compose"

# Static DAG start; catchup=False everywhere, so this only gates scheduling.
# MUST stay safely in the past: Airflow's DagRun.verify_integrity only creates
# task instances with `task.start_date <= execution_date`, and the scheduler
# skips verify_integrity entirely when the run's dag_hash matches the current
# serialization — so a trigger whose execution_date precedes start_date runs
# ZERO tasks and is marked success (measured 2026-09-11: empty run "succeeded"
# in 0.03 s). A past start_date makes every manual trigger real.
START_DATE = pendulum.datetime(2026, 9, 10, tz="UTC")

# Per-source ingest tasks (ADR-010 D2): the extractors are already retry/backoff
# aware and idempotent by hash ledger; Airflow adds the outer retry loop.
INGEST_DEFAULT_ARGS = {
    "owner": "helios",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

# Master DAG trigger tasks own one retry (a failed trigger call re-triggers a
# child run; the child DAG owns its task retries — no double retry stacking).
MASTER_DEFAULT_ARGS = {
    "owner": "helios",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
