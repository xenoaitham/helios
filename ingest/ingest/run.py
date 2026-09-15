"""`python -m ingest.run` — one-shot batch extraction runner (ADR-006/007).

One load ledger row per source per run: status 'running' while it works (with
counters as of the last batch commit — the SIGKILL paper trail), 'succeeded' or
'failed' at the end. Failures are loud: nonzero exit, JSON error event, full
traceback on stderr. Airflow takes over scheduling in Phase 3; this runner is
deliberately one-shot.
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid

import psycopg

from ingest import config, loads
from ingest.ensure import ensure_ingest_tables
from ingest.log import log


def _connect() -> psycopg.Connection:
    cfg = config.warehouse_settings()
    return psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)


def run_source(conn, source: str, *, full: bool = False) -> loads.RunStats:
    """Run one extractor under a fresh load ledger row; re-raises on failure."""
    from ingest.extract_file import run_file
    from ingest.extract_rest import run_rest
    from ingest.extract_soap import run_soap

    extractors = {"soap": run_soap, "file": run_file, "rest": run_rest}
    load_id = uuid.uuid4()
    stats = loads.RunStats()
    ensure_ingest_tables(conn)
    loads.start_load(conn, load_id, source, source, 0)
    log("run_started", source=source, load_id=str(load_id), full=full)
    try:
        if source == "soap":
            run_soap(conn, load_id, stats, full=full)
        elif source == "file":
            run_file(conn, load_id, stats)
        elif source == "rest":
            run_rest(conn, load_id, stats)
        else:
            raise RuntimeError(f"unknown source {source!r}")
        loads.update_load(conn, load_id, stats, status="succeeded")
        log("run_succeeded", source=source, load_id=str(load_id), **stats.__dict__)
    except Exception as exc:
        loads.update_load(conn, load_id, stats, status="failed", error=repr(exc)[:500])
        log("run_failed", source=source, load_id=str(load_id), level="error", error=repr(exc))
        raise
    return stats


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="HELIOS batch ingest runner (ADR-006/007)")
    parser.add_argument("--source", choices=["soap", "file", "rest", "all"], default="all", help="which extractor to run (default: all)")
    parser.add_argument("--full", action="store_true", help="soap: re-pull full history from the epoch (hash guard makes it a no-op for unchanged rows)")
    args = parser.parse_args(argv)

    sources = ["soap", "file", "rest"] if args.source == "all" else [args.source]
    conn = _connect()
    failed = 0
    try:
        for source in sources:
            try:
                run_source(conn, source, full=args.full)
            except Exception:
                traceback.print_exc()
                failed += 1
    finally:
        conn.close()
    if failed:
        log("runner_done", level="error", failed=failed, sources=sources)
        return 1
    log("runner_done", failed=0, sources=sources)
    return 0


if __name__ == "__main__":
    sys.exit(main())
