"""Watermark state: read the cursor, advance it only after landing commits (ADR-006).

`max_created_at` values are 'YYYY-MM-DD HH:MM:SS' strings (the SOAP storage
format, lexicographically comparable — ADR-001), so the never-regress guard is a
plain string comparison in Python around two boring statements. Non-cursor kinds
(`last_full_walk`) are observability stamps and may move freely.
"""

from __future__ import annotations

MAX_CREATED_AT = "max_created_at"


def get_watermark(conn, source: str) -> tuple[str, str] | None:
    """Return (watermark_value, watermark_kind) or None when the source has no cursor yet."""
    row = conn.execute("SELECT watermark_value, watermark_kind FROM raw.ingest_watermarks WHERE source = %s", (source,)).fetchone()
    return None if row is None else (row[0], row[1])


def advance_watermark(conn, source: str, value: str, kind: str) -> bool:
    """Advance the cursor for `source`; returns False when the guard refused.

    Cursors (kind max_created_at) never regress; observability stamps overwrite.
    Callers run this AFTER the landing transaction committed (ADR-006) — a crash
    in between replays the window, which the content-hash guard absorbs as no-ops.
    """
    with conn.transaction():
        row = conn.execute("SELECT watermark_value, watermark_kind FROM raw.ingest_watermarks WHERE source = %s", (source,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO raw.ingest_watermarks (source, watermark_value, watermark_kind) VALUES (%s, %s, %s)", (source, value, kind))
            return True
        current_value, current_kind = row
        if kind == MAX_CREATED_AT and current_kind == MAX_CREATED_AT and value < current_value:
            return False  # cursor guard: a replay must never move backwards
        conn.execute("UPDATE raw.ingest_watermarks SET watermark_value = %s, watermark_kind = %s, updated_at = now() WHERE source = %s", (value, kind, source))
        return True
