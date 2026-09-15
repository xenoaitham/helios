"""`python -m ingest.status` — human-readable control-plane view (ADR-006/007).

Watermarks, per-table landed counts, the file ledger, quarantine summary and the
recent run ledger, straight from SQL. Every statement is a single static literal
at its call site (Mimosa source rule).
"""

from __future__ import annotations

import psycopg

from ingest.ensure import ensure_ingest_tables
from ingest.run import _connect


def main() -> int:
    conn: psycopg.Connection = _connect()
    try:
        ensure_ingest_tables(conn)

        print("== watermarks ==")
        for source, value, kind, updated_at in conn.execute("SELECT source, watermark_value, watermark_kind, updated_at FROM raw.ingest_watermarks ORDER BY source").fetchall():
            print(f"  {source:20s} {value:24s} ({kind}, {updated_at})")

        print("== landed tables ==")
        for row in conn.execute("SELECT 'raw.soap_orders' AS t, count(*) AS n, max(landed_at) AS latest FROM raw.soap_orders UNION ALL SELECT 'raw.file_customers', count(*), max(landed_at) FROM raw.file_customers UNION ALL SELECT 'raw.file_products', count(*), max(landed_at) FROM raw.file_products UNION ALL SELECT 'raw.rest_products', count(*), max(landed_at) FROM raw.rest_products UNION ALL SELECT 'raw.rest_promotions', count(*), max(landed_at) FROM raw.rest_promotions").fetchall():
            print(f"  {row[0]:22s} rows={row[1]:<8} last_effective_landing={row[2]}")

        print("== file ledger (last 10) ==")
        for row in conn.execute("SELECT filename, file_hash, rows_read, rows_landed, rows_quarantined, landed_at FROM raw.ingest_files ORDER BY landed_at DESC LIMIT 10").fetchall():
            print(f"  {row[0]:28s} hash={row[1][:12]}… read={row[2]:<6} landed={row[3]:<6} quarantined={row[4]:<4} at={row[5]}")

        print("== quarantine by reason ==")
        for reason, count in conn.execute("SELECT reason, count(*) FROM raw.ingest_quarantine GROUP BY reason ORDER BY count(*) DESC").fetchall():
            print(f"  {reason:24s} {count}")

        print("== quarantine recent (last 10) ==")
        for row in conn.execute("SELECT source, batch_ref, row_number, reason, left(COALESCE(raw_content, ''), 60), quarantined_at FROM raw.ingest_quarantine ORDER BY quarantined_at DESC LIMIT 10").fetchall():
            print(f"  [{row[5]}] {row[0]} {row[1]} row {row[2]}: {row[3]} :: {row[4]}")

        print("== runs (last 10) ==")
        for row in conn.execute("SELECT load_id, source, status, rows_read, rows_landed, rows_unchanged, rows_quarantined, units_done, units_total, started_at, finished_at, error FROM raw.ingest_loads ORDER BY started_at DESC LIMIT 10").fetchall():
            suffix = f" ERROR={row[11]}" if row[11] else ""
            print(f"  [{row[9]}] {row[1]:6s} {row[2]:9s} read={row[3]:<7} landed={row[4]:<7} unchanged={row[5]:<7} quarantined={row[6]:<4} units={row[7]}/{row[8]} load={row[0]}{suffix}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
