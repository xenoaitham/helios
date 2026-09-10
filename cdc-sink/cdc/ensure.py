"""Raw-zone DDL for the CDC landing tables (ADR-005).

The warehouse volume predates this phase, so — like the OLTP schema (ADR-002) —
the DDL is ensured by running code against the live instance rather than a
fresh-init script. `infra/warehouse/init/02-cdc.sql` mirrors these statements
for `make clean && make up` parity; this module is the source of truth.

Every SQL statement is a single static string literal at its execution site
(no concatenation, no assembly, no interpolation — Mimosa source rule).
"""

from __future__ import annotations


def ensure_raw_tables(conn) -> None:
    """CREATE TABLE IF NOT EXISTS for raw.cdc_<table>; safe on every start."""
    with conn.transaction():
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.cdc_users (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.cdc_orders (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.cdc_order_items (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.cdc_payments (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("COMMENT ON TABLE raw.cdc_users IS 'CDC landing, source public.users — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)'")
        conn.execute("COMMENT ON TABLE raw.cdc_orders IS 'CDC landing, source public.orders — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)'")
        conn.execute("COMMENT ON TABLE raw.cdc_order_items IS 'CDC landing, source public.order_items — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)'")
        conn.execute("COMMENT ON TABLE raw.cdc_payments IS 'CDC landing, source public.payments — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)'")
