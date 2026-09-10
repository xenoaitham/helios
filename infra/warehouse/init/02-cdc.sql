-- HELIOS warehouse CDC landing tables (ADR-005) — fresh-volume init only.
-- Runs on FIRST start with an empty data volume (docker-entrypoint-initdb.d).
-- MIRRORS cdc/ensure.py (the source of truth): the sink re-ensures this DDL on
-- every startup, so existing volumes (not re-run here) are covered too.

CREATE TABLE IF NOT EXISTS raw.cdc_users (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.cdc_orders (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.cdc_order_items (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.cdc_payments (pk JSONB PRIMARY KEY, lsn BIGINT NOT NULL, op TEXT NOT NULL, ts_ms BIGINT NOT NULL, before JSONB, after JSONB, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());

COMMENT ON TABLE raw.cdc_users IS 'CDC landing, source public.users — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)';
COMMENT ON TABLE raw.cdc_orders IS 'CDC landing, source public.orders — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)';
COMMENT ON TABLE raw.cdc_order_items IS 'CDC landing, source public.order_items — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)';
COMMENT ON TABLE raw.cdc_payments IS 'CDC landing, source public.payments — Debezium envelopes, lsn-guarded upsert by (lsn, pk); op=d is a tombstone row (ADR-005)';
