"""Run ledger (ADR-007): one raw.ingest_loads row per ingest run.

Counters are updated INSIDE each batch's landing transaction, so a SIGKILLed run
leaves a forensic status='running' row holding the counters as of the last
commit — the paper trail the crash probe reads. `finished_at` is stamped only
when the status leaves 'running'.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunStats:
    rows_read: int = 0
    rows_landed: int = 0
    rows_unchanged: int = 0
    rows_quarantined: int = 0
    rows_coalesced: int = 0  # same-key rows inside one batch (dupes); logged, not in the table
    units_done: int = 0
    units_total: int = 0  # 0 = unknown until discovered


def start_load(conn, load_id, source: str, batch_ref: str, units_total: int) -> None:
    conn.execute("INSERT INTO raw.ingest_loads (load_id, source, batch_ref, status, units_total) VALUES (%s, %s, %s, 'running', %s)", (load_id, source, batch_ref, units_total))


def update_load(conn, load_id, stats: RunStats, status: str = "running", error: str | None = None) -> None:
    """Write current counters (and optional status transition) for one run."""
    conn.execute("UPDATE raw.ingest_loads SET status = %s, rows_read = %s, rows_landed = %s, rows_unchanged = %s, rows_quarantined = %s, units_done = %s, units_total = COALESCE(NULLIF(%s, 0), raw.ingest_loads.units_total), error = %s, finished_at = CASE WHEN %s <> 'running' THEN now() ELSE raw.ingest_loads.finished_at END WHERE load_id = %s", (status, stats.rows_read, stats.rows_landed, stats.rows_unchanged, stats.rows_quarantined, stats.units_done, stats.units_total, error, status, load_id))
