"""Master daily_close DAG (ADR-010 D2/D3): ingest fan-out → one dbt build gate.

Topology: ingest_file → [ingest_soap ∥ ingest_rest] → dbt_build.

- ingest_file first on purpose: every extractor start executes the shared ingest
  DDL (ledgers/quarantine, CREATE TABLE IF NOT EXISTS, no advisory lock) —
  running the smallest feed first warms that DDL before the two heavier walkers
  fan out, structurally removing the cold-start DDL race on a fresh warehouse
  (ADR-010 D2). On a warm warehouse the order is a no-op.
- soap ∥ rest is safe: they touch disjoint raw tables.
- dbt_build is ONE task and the DAG-level DQ gate (ADR-010 D3): `dbt build`
  interleaves tests into the resource DAG — error-severity staging tests block
  every downstream node, marts tests gate the published layer — so a single
  invocation already implements "staging gates marts". No separate dbt step,
  never `--full-refresh` (the snapshot is persistent state; ADR-009 D1).
  Great Expectations (Phase 4 item 10) plugs in downstream of this task at the
  reserved gate seam.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from helios_common import COMPOSE_PREFIX, MASTER_DEFAULT_ARGS, START_DATE

DBT_BUILD_DOC = (
    "The DAG-level DQ gate (ADR-010 D3): rebuild the dbt tool image (cached "
    "no-op when unchanged, so a project edit can never be missed by a run) and "
    "run ONE `dbt build` — staging → snapshot → marts + all data tests in a "
    "single invocation. Error-severity tests block downstream nodes, so staging "
    "tests gate marts inside this task; marts tests gate the published layer. "
    "A quiet rebuild logs `INSERT 0 0` on the snapshot and never touches "
    "history — NEVER `--full-refresh` (ADR-009 D1, RUNBOOK 4b). Retries absorb "
    "the µs-window CDC straddle the strict items→orders test can theoretically "
    "catch (unreached in builds to date)."
)

TRIGGER_DOC_FMT = (
    "Waits for the child DAG `ingest_{source}` run to a terminal state "
    "(wait_for_completion, deferrable=False — LocalExecutor has no triggerer). "
    "{doc}"
)


with DAG(
    dag_id="daily_close",
    schedule="0 5 * * *",  # UTC; the stack's clock story is UTC end-to-end
    start_date=START_DATE,
    catchup=False,  # backfill is replay-based, not date-partitioned (ADR-010 D5)
    max_active_runs=1,  # full-refresh-rebuilt marts + the snapshot are not concurrency-safe
    default_args=MASTER_DEFAULT_ARGS,
    tags=["helios", "master"],
    description="HELIOS nightly close: batch+window ingest → dbt staging/snapshot/marts",
    doc_md=(
        "**daily_close** — the HELIOS pipeline unit (ADR-010 D2).\n\n"
        "ingest_file → (ingest_soap ∥ ingest_rest) → dbt_build.\n\n"
        "Composes the per-source ingest DAGs via TriggerDagRunOperator, then "
        "runs one `dbt build` as the DQ gate. `max_active_runs=1`: a snapshot "
        "plus full-refresh-rebuilt marts must never run concurrently. "
        "Triggered nightly (05:00 UTC) and by `make run-etl` / `make backfill`. "
        "Idempotent end-to-end: hash-ledgered ingest, snapshot-safe dbt build, "
        "bit-stable marts modulo attributed CDC drift (`make cdc-status`)."
    ),
) as dag:
    trigger_ingest_file = TriggerDagRunOperator(
        task_id="trigger_ingest_file",
        trigger_dag_id="ingest_file",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=False,
        execution_timeout=timedelta(minutes=45),
        sla=timedelta(hours=2),
        doc_md=TRIGGER_DOC_FMT.format(source="file", doc="Runs first: warms the shared ingest DDL (ADR-010 D2)."),
    )
    trigger_ingest_soap = TriggerDagRunOperator(
        task_id="trigger_ingest_soap",
        trigger_dag_id="ingest_soap",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=False,
        execution_timeout=timedelta(minutes=45),
        sla=timedelta(hours=2),
        doc_md=TRIGGER_DOC_FMT.format(source="soap", doc="Parallel with rest: disjoint raw tables."),
    )
    trigger_ingest_rest = TriggerDagRunOperator(
        task_id="trigger_ingest_rest",
        trigger_dag_id="ingest_rest",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=False,
        execution_timeout=timedelta(minutes=45),
        sla=timedelta(hours=2),
        doc_md=TRIGGER_DOC_FMT.format(source="rest", doc="Parallel with soap: disjoint raw tables; expects 429 flow."),
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{COMPOSE_PREFIX} build dbt && {COMPOSE_PREFIX} run --rm dbt build",
        retries=2,
        retry_delay=timedelta(minutes=2),
        sla=timedelta(hours=1),
        execution_timeout=timedelta(minutes=20),
        doc_md=DBT_BUILD_DOC,
    )

    trigger_ingest_file >> [trigger_ingest_soap, trigger_ingest_rest] >> dbt_build
