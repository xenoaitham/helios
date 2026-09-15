"""Per-source ingest DAGs (ADR-010 D2): ingest_soap / ingest_file / ingest_rest.

One BashOperator task each — the exact command `make ingest-<source>` runs —
so the DAG adds orchestration (retries, SLA, docs, run history, UI) and ZERO
extraction logic. schedule=None: the master `daily_close` DAG triggers these;
manual single-source runs from the UI/CLI remain possible for ops. Each source
is idempotent by design (ADR-006/007): content-hash-guarded upserts make any
replay a physical no-op for unchanged content.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from helios_common import COMPOSE_PREFIX, INGEST_DEFAULT_ARGS, START_DATE

# (source, task doc). The DAG doc_md is composed from the same facts.
SOURCES = {
    "soap": (
        "Windowed SOAP GetOrders pull ([watermark − 7 d overlap, now]) into "
        "raw.soap_orders (ADR-006 D1). Idempotent: (content_hash, order_id)-guarded "
        "upserts — unchanged rows are physical no-ops, so retries and replays are "
        "always safe. Expected ~4 s incremental after the first-ever full pull "
        "(382,179 orders, ~2.5 min). Old-order status changes older than the "
        "overlap need the documented manual `--full` replay (RUNBOOK 4b)."
    ),
    "file": (
        "Processes new/changed CSV feeds in the SFTP-style drop volume "
        "(customers, products) into raw.file_* (ADR-006/007). Idempotent via the "
        "per-file hash ledger; rejects (ragged rows, encoding, duplicates) go to "
        "raw.ingest_quarantine — loud, never silently dropped. Also warms the "
        "shared ingest DDL (ledgers/quarantine) — daily_close runs this task "
        "first for exactly that reason (ADR-010 D2)."
    ),
    "rest": (
        "Full cursor walk of the Pricing & Promotions API (/products, "
        "/promotions) into raw.rest_* (ADR-006 D1). The mock rate-limits: 429s "
        "are expected flow — the extractor honors Retry-After verbatim (ADR-006, "
        "measured in Phase 2) and deterministic 500s are retried. Idempotent: "
        "content-hash-guarded upserts; reruns converge with zero dupes."
    ),
}


def build_ingest_dag(source: str, task_doc: str) -> DAG:
    dag_id = f"ingest_{source}"
    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,  # extractors are uncoordinated one-shots; never overlap a source
        default_args=INGEST_DEFAULT_ARGS,
        tags=["helios", "ingest"],
        description=f"One-shot {source} extract into raw (the `make ingest-{source}` unit)",
        doc_md=(
            f"**ingest_{source}** — per-source extract DAG (ADR-010 D2).\n\n"
            f"Runs the one-shot `ingest` tool container for source `{source}` "
            "through the host's rootless docker socket — the same command "
            f"`make ingest-{source}` uses. No SQL, no HTTP, no credentials in "
            "DAG code; the container owns its protocol and its idempotency.\n\n"
            f"{task_doc}"
        ),
    ) as dag:
        BashOperator(
            task_id=f"ingest_{source}",
            bash_command=f"{COMPOSE_PREFIX} run --rm ingest python -m ingest.run --source {source}",
            sla=timedelta(hours=1),
            execution_timeout=timedelta(minutes=30),
            doc_md=task_doc,
        )
    return dag


ingest_soap = build_ingest_dag("soap", SOURCES["soap"])
ingest_file = build_ingest_dag("file", SOURCES["file"])
ingest_rest = build_ingest_dag("rest", SOURCES["rest"])
