"""Idempotent raw-zone landing: content-hash-guarded natural-key upserts (ADR-007).

Each target table has one fixed INSERT ... SELECT unnest(...) ... ON CONFLICT
statement — the SQL text is a complete single literal and every value is a bound
array parameter (Mimosa source rule: no assembly, no interpolation, no
adjacent-literal joins — cdc/upsert.py precedent). The guard
`WHERE <table>.content_hash <> EXCLUDED.content_hash` makes landing idempotent:
re-landing identical source content is a physical no-op (not even landed_at
moves); cur.rowcount is exactly the rows inserted or updated, so
`unchanged = len - rowcount`.

Postgres refuses an ON CONFLICT that touches one row twice, so same-key rows
within one batch coalesce first (last occurrence wins — deterministic because
batch order is deterministic); the coalesced count is returned for logging.
"""

from __future__ import annotations

import json

from ingest.hashing import content_hash


def coalesce_rows(rows: list[tuple[object, dict]]) -> tuple[list[tuple[object, dict]], int]:
    """Deduplicate (pk, payload) pairs by pk, keeping the LAST occurrence."""
    dedup: dict[object, tuple[object, dict]] = {}
    for row in rows:
        dedup[row[0]] = row
    return list(dedup.values()), len(rows) - len(dedup)


def _arrays(rows: list[tuple[object, dict]]) -> tuple[list, list, list]:
    pks = [pk for pk, _ in rows]
    hashes = [content_hash(payload) for _, payload in rows]
    payloads = [json.dumps(payload) for _, payload in rows]
    return pks, hashes, payloads


def land_soap_orders(conn, rows: list[tuple[int, dict]], load_id, batch_ref: str) -> tuple[int, int]:
    pks, hashes, payloads = _arrays(rows)
    cur = conn.execute("INSERT INTO raw.soap_orders (order_id, load_id, batch_ref, content_hash, payload) SELECT e.pk, %s::uuid, %s::text, e.content_hash, e.payload FROM unnest(%s::bigint[], %s::text[], %s::jsonb[]) AS e(pk, content_hash, payload) ON CONFLICT (order_id) DO UPDATE SET load_id = EXCLUDED.load_id, batch_ref = EXCLUDED.batch_ref, content_hash = EXCLUDED.content_hash, payload = EXCLUDED.payload, landed_at = now() WHERE raw.soap_orders.content_hash <> EXCLUDED.content_hash", (load_id, batch_ref, pks, hashes, payloads))
    return cur.rowcount, len(rows) - cur.rowcount


def land_file_customers(conn, rows: list[tuple[int, dict]], load_id, batch_ref: str) -> tuple[int, int]:
    pks, hashes, payloads = _arrays(rows)
    cur = conn.execute("INSERT INTO raw.file_customers (customer_id, load_id, batch_ref, content_hash, payload) SELECT e.pk, %s::uuid, %s::text, e.content_hash, e.payload FROM unnest(%s::bigint[], %s::text[], %s::jsonb[]) AS e(pk, content_hash, payload) ON CONFLICT (customer_id) DO UPDATE SET load_id = EXCLUDED.load_id, batch_ref = EXCLUDED.batch_ref, content_hash = EXCLUDED.content_hash, payload = EXCLUDED.payload, landed_at = now() WHERE raw.file_customers.content_hash <> EXCLUDED.content_hash", (load_id, batch_ref, pks, hashes, payloads))
    return cur.rowcount, len(rows) - cur.rowcount


def land_file_products(conn, rows: list[tuple[str, dict]], load_id, batch_ref: str) -> tuple[int, int]:
    pks, hashes, payloads = _arrays(rows)
    cur = conn.execute("INSERT INTO raw.file_products (sku, load_id, batch_ref, content_hash, payload) SELECT e.pk, %s::uuid, %s::text, e.content_hash, e.payload FROM unnest(%s::text[], %s::text[], %s::jsonb[]) AS e(pk, content_hash, payload) ON CONFLICT (sku) DO UPDATE SET load_id = EXCLUDED.load_id, batch_ref = EXCLUDED.batch_ref, content_hash = EXCLUDED.content_hash, payload = EXCLUDED.payload, landed_at = now() WHERE raw.file_products.content_hash <> EXCLUDED.content_hash", (load_id, batch_ref, pks, hashes, payloads))
    return cur.rowcount, len(rows) - cur.rowcount


def land_rest_products(conn, rows: list[tuple[int, dict]], load_id, batch_ref: str) -> tuple[int, int]:
    pks, hashes, payloads = _arrays(rows)
    cur = conn.execute("INSERT INTO raw.rest_products (id, load_id, batch_ref, content_hash, payload) SELECT e.pk, %s::uuid, %s::text, e.content_hash, e.payload FROM unnest(%s::bigint[], %s::text[], %s::jsonb[]) AS e(pk, content_hash, payload) ON CONFLICT (id) DO UPDATE SET load_id = EXCLUDED.load_id, batch_ref = EXCLUDED.batch_ref, content_hash = EXCLUDED.content_hash, payload = EXCLUDED.payload, landed_at = now() WHERE raw.rest_products.content_hash <> EXCLUDED.content_hash", (load_id, batch_ref, pks, hashes, payloads))
    return cur.rowcount, len(rows) - cur.rowcount


def land_rest_promotions(conn, rows: list[tuple[int, dict]], load_id, batch_ref: str) -> tuple[int, int]:
    pks, hashes, payloads = _arrays(rows)
    cur = conn.execute("INSERT INTO raw.rest_promotions (id, load_id, batch_ref, content_hash, payload) SELECT e.pk, %s::uuid, %s::text, e.content_hash, e.payload FROM unnest(%s::bigint[], %s::text[], %s::jsonb[]) AS e(pk, content_hash, payload) ON CONFLICT (id) DO UPDATE SET load_id = EXCLUDED.load_id, batch_ref = EXCLUDED.batch_ref, content_hash = EXCLUDED.content_hash, payload = EXCLUDED.payload, landed_at = now() WHERE raw.rest_promotions.content_hash <> EXCLUDED.content_hash", (load_id, batch_ref, pks, hashes, payloads))
    return cur.rowcount, len(rows) - cur.rowcount


def land_quarantine(conn, rows: list[tuple[str, str, str | None, int | None, str, str | None]], load_id) -> int:
    """Insert per-row rejects; the UNIQUE guard makes crash-replay re-quarantine
    nothing. rows: (source, batch_ref, filename, row_number, reason, raw_content)."""
    sources = [r[0] for r in rows]
    batch_refs = [r[1] for r in rows]
    filenames = [r[2] for r in rows]
    row_numbers = [r[3] for r in rows]
    reasons = [r[4] for r in rows]
    contents = [r[5] for r in rows]
    cur = conn.execute("INSERT INTO raw.ingest_quarantine (source, batch_ref, filename, row_number, reason, raw_content, load_id) SELECT q.source, q.batch_ref, q.filename, q.row_number, q.reason, q.raw_content, %s::uuid FROM unnest(%s::text[], %s::text[], %s::text[], %s::bigint[], %s::text[], %s::text[]) AS q(source, batch_ref, filename, row_number, reason, raw_content) ON CONFLICT (source, batch_ref, row_number, reason) DO NOTHING", (load_id, sources, batch_refs, filenames, row_numbers, reasons, contents))
    return cur.rowcount


def files_ledger_hash(conn, filename: str) -> str | None:
    """Current recorded hash for a consumed file (None = never landed)."""
    row = conn.execute("SELECT file_hash FROM raw.ingest_files WHERE filename = %s", (filename,)).fetchone()
    return None if row is None else row[0]


def record_file(conn, filename: str, file_hash: str, size_bytes: int, load_id, rows_read: int, rows_landed: int, rows_quarantined: int) -> None:
    """Upsert the file ledger INSIDE the file's landing transaction (atomic consume)."""
    conn.execute("INSERT INTO raw.ingest_files (filename, file_hash, size_bytes, load_id, rows_read, rows_landed, rows_quarantined) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (filename) DO UPDATE SET file_hash = EXCLUDED.file_hash, size_bytes = EXCLUDED.size_bytes, load_id = EXCLUDED.load_id, rows_read = EXCLUDED.rows_read, rows_landed = EXCLUDED.rows_landed, rows_quarantined = EXCLUDED.rows_quarantined, landed_at = now()", (filename, file_hash, size_bytes, load_id, rows_read, rows_landed, rows_quarantined))
