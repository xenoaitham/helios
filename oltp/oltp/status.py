"""Operational report for the OLTP source: row counts, on-disk sizes, measured
WAL churn (the BACKLOG item-2 DoD instrument) — `make oltp-status`.

Every SQL statement is a single static string literal at its call site.
"""

from __future__ import annotations

import argparse
import time

from oltp.db import connect


def collect_counts(conn) -> dict | None:
    row = conn.execute("SELECT to_regclass('public.orders') IS NOT NULL").fetchone()
    if not row or not row[0]:
        return None
    row = conn.execute("SELECT (SELECT COUNT(*) FROM public.users), (SELECT COUNT(*) FROM public.orders), (SELECT COUNT(*) FROM public.order_items), (SELECT COUNT(*) FROM public.payments)").fetchone()
    return {"users": row[0], "orders": row[1], "order_items": row[2], "payments": row[3]}


def collect_sizes(conn) -> dict:
    row = conn.execute("SELECT pg_total_relation_size('public.users'), pg_total_relation_size('public.orders'), pg_total_relation_size('public.order_items'), pg_total_relation_size('public.payments')").fetchone()
    return {"users": row[0], "orders": row[1], "order_items": row[2], "payments": row[3]}


def wal_probe(conn, window_seconds: float) -> dict:
    """Measure WAL bytes generated during a window (the mutator should be
    ticking throughout) plus cumulative pg_stat_wal counters."""
    lsn_start = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    time.sleep(window_seconds)
    lsn_end = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    delta = conn.execute("SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)", (lsn_end, lsn_start)).fetchone()[0]
    stat = conn.execute("SELECT wal_records, wal_bytes FROM pg_stat_wal").fetchone()
    return {
        "lsn_start": lsn_start,
        "lsn_end": lsn_end,
        "delta_bytes": int(delta or 0),
        "wal_records_total": stat[0],
        "wal_bytes_total": stat[1],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="HELIOS OLTP source status (ADR-002)")
    parser.add_argument("--wal-window", type=float, default=15.0, help="seconds to measure WAL delta over")
    args = parser.parse_args(argv)

    with connect() as conn:
        counts = collect_counts(conn)
        if counts is None:
            print("[status] schema not present yet (run: make seed-oltp)")
        else:
            sizes = collect_sizes(conn)
            for table in ("users", "orders", "order_items", "payments"):
                print(f"[status] {table}: {counts[table]} rows, {sizes[table] / (1024 * 1024):.1f} MB on disk (table+indexes)")
            print(f"[status] total rows: {sum(counts.values())}")
        wal = wal_probe(conn, args.wal_window)
        print(f"[status] WAL delta over {args.wal_window:g}s window: {wal['delta_bytes']} bytes (lsn {wal['lsn_start']} -> {wal['lsn_end']})")
        print(f"[status] pg_stat_wal cumulative: records={wal['wal_records_total']} bytes={wal['wal_bytes_total']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
