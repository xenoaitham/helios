"""Lsn-guarded upserts: the (lsn, pk) idempotency core of the raw zone (ADR-005).

Each source table has one fixed INSERT ... SELECT unnest(...) ... ON CONFLICT
statement — the SQL text is a complete single literal and every value is a
bound array parameter (Mimosa source rule: no assembly, no interpolation, no
adjacent-literal joins). The ON CONFLICT guard `WHERE EXCLUDED.lsn >=
raw.cdc_<t>.lsn` makes application idempotent: replaying events in any order
(a Kafka rebalance, an offset reset, a full re-drain) can never move a row
backwards in the WAL history. Deletes land as tombstone rows (op='d', after
IS NULL) that Phase 3 staging filters.

cur.rowcount after the array upsert is exactly the number of rows that were
inserted or updated; rows rejected by the lsn guard are (len - rowcount).
"""

from __future__ import annotations

import json

from cdc.events import Event


def _json(value: dict | None) -> str | None:
    return None if value is None else json.dumps(value)


def _params(events: list[Event]) -> tuple:
    return (
        [json.dumps(e.pk) for e in events],
        [e.lsn for e in events],
        [e.op for e in events],
        [e.ts_ms for e in events],
        [_json(e.before) for e in events],
        [_json(e.after) for e in events],
    )


def _upsert_users(conn, events: list[Event]) -> int:
    cur = conn.execute("INSERT INTO raw.cdc_users (pk, lsn, op, ts_ms, before, after) SELECT pk, lsn, op, ts_ms, src_before, src_after FROM unnest(%s::jsonb[], %s::bigint[], %s::text[], %s::bigint[], %s::jsonb[], %s::jsonb[]) AS e(pk, lsn, op, ts_ms, src_before, src_after) ON CONFLICT (pk) DO UPDATE SET lsn = EXCLUDED.lsn, op = EXCLUDED.op, ts_ms = EXCLUDED.ts_ms, before = EXCLUDED.before, after = EXCLUDED.after, landed_at = now() WHERE EXCLUDED.lsn >= raw.cdc_users.lsn", _params(events))
    return cur.rowcount


def _upsert_orders(conn, events: list[Event]) -> int:
    cur = conn.execute("INSERT INTO raw.cdc_orders (pk, lsn, op, ts_ms, before, after) SELECT pk, lsn, op, ts_ms, src_before, src_after FROM unnest(%s::jsonb[], %s::bigint[], %s::text[], %s::bigint[], %s::jsonb[], %s::jsonb[]) AS e(pk, lsn, op, ts_ms, src_before, src_after) ON CONFLICT (pk) DO UPDATE SET lsn = EXCLUDED.lsn, op = EXCLUDED.op, ts_ms = EXCLUDED.ts_ms, before = EXCLUDED.before, after = EXCLUDED.after, landed_at = now() WHERE EXCLUDED.lsn >= raw.cdc_orders.lsn", _params(events))
    return cur.rowcount


def _upsert_order_items(conn, events: list[Event]) -> int:
    cur = conn.execute("INSERT INTO raw.cdc_order_items (pk, lsn, op, ts_ms, before, after) SELECT pk, lsn, op, ts_ms, src_before, src_after FROM unnest(%s::jsonb[], %s::bigint[], %s::text[], %s::bigint[], %s::jsonb[], %s::jsonb[]) AS e(pk, lsn, op, ts_ms, src_before, src_after) ON CONFLICT (pk) DO UPDATE SET lsn = EXCLUDED.lsn, op = EXCLUDED.op, ts_ms = EXCLUDED.ts_ms, before = EXCLUDED.before, after = EXCLUDED.after, landed_at = now() WHERE EXCLUDED.lsn >= raw.cdc_order_items.lsn", _params(events))
    return cur.rowcount


def _upsert_payments(conn, events: list[Event]) -> int:
    cur = conn.execute("INSERT INTO raw.cdc_payments (pk, lsn, op, ts_ms, before, after) SELECT pk, lsn, op, ts_ms, src_before, src_after FROM unnest(%s::jsonb[], %s::bigint[], %s::text[], %s::bigint[], %s::jsonb[], %s::jsonb[]) AS e(pk, lsn, op, ts_ms, src_before, src_after) ON CONFLICT (pk) DO UPDATE SET lsn = EXCLUDED.lsn, op = EXCLUDED.op, ts_ms = EXCLUDED.ts_ms, before = EXCLUDED.before, after = EXCLUDED.after, landed_at = now() WHERE EXCLUDED.lsn >= raw.cdc_payments.lsn", _params(events))
    return cur.rowcount


def apply_events(conn, events: list[Event]) -> tuple[int, int, int]:
    """Apply parsed events inside the caller's transaction.

    Returns (applied, coalesced, stale_rejected). A single poll batch can
    contain several events for the same PK (snapshot chunk + a concurrent
    update, or repeated mutator ticks); Postgres rejects an INSERT ... ON
    CONFLICT DO UPDATE that touches the same row twice, so coalescing per
    (table, pk) keeping the LAST event in stream order is required for
    correctness, not just speed. Unknown tables never reach here (the parser
    filters them), so the dispatch is exhaustive over the capture set.
    """
    dedup: dict[tuple[str, str], Event] = {}
    for event in events:
        dedup[(event.table, json.dumps(event.pk, sort_keys=True))] = event
    coalesced = len(events) - len(dedup)

    by_table: dict[str, list[Event]] = {}
    for event in dedup.values():
        by_table.setdefault(event.table, []).append(event)

    applied = 0
    for table, table_events in by_table.items():
        if table == "users":
            applied += _upsert_users(conn, table_events)
        elif table == "orders":
            applied += _upsert_orders(conn, table_events)
        elif table == "order_items":
            applied += _upsert_order_items(conn, table_events)
        elif table == "payments":
            applied += _upsert_payments(conn, table_events)
    return applied, coalesced, len(dedup) - applied
