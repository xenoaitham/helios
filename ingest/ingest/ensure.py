"""Raw-zone DDL for batch landing, control tables and quarantine (ADR-007).

The warehouse volume predates this phase, so — exactly like cdc/ensure.py
(ADR-005) — the DDL is ensured by running code at the start of every ingest
invocation rather than a fresh-init script. `infra/warehouse/init/03-ingest.sql`
mirrors these statements for `make clean && make up` parity; this module is the
source of truth.

Every SQL statement is a single static string literal at its execution site
(no concatenation, no assembly, no interpolation — Mimosa source rule).
"""

from __future__ import annotations


def ensure_ingest_tables(conn) -> None:
    """CREATE TABLE IF NOT EXISTS for the batch raw zone; safe on every run."""
    with conn.transaction():
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.ingest_watermarks (source TEXT PRIMARY KEY, watermark_value TEXT NOT NULL, watermark_kind TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.ingest_loads (load_id UUID PRIMARY KEY, source TEXT NOT NULL, batch_ref TEXT, status TEXT NOT NULL, rows_read BIGINT NOT NULL DEFAULT 0, rows_landed BIGINT NOT NULL DEFAULT 0, rows_unchanged BIGINT NOT NULL DEFAULT 0, rows_quarantined BIGINT NOT NULL DEFAULT 0, units_done BIGINT NOT NULL DEFAULT 0, units_total BIGINT NOT NULL DEFAULT 0, error TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ)")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.ingest_files (filename TEXT PRIMARY KEY, file_hash TEXT NOT NULL, size_bytes BIGINT NOT NULL, load_id UUID NOT NULL, rows_read BIGINT NOT NULL DEFAULT 0, rows_landed BIGINT NOT NULL DEFAULT 0, rows_quarantined BIGINT NOT NULL DEFAULT 0, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.ingest_quarantine (quarantine_id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, batch_ref TEXT NOT NULL, filename TEXT, row_number BIGINT, reason TEXT NOT NULL, raw_content TEXT, load_id UUID NOT NULL, quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (source, batch_ref, row_number, reason))")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.soap_orders (order_id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.file_customers (customer_id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.file_products (sku TEXT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.rest_products (id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.rest_promotions (id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute("COMMENT ON TABLE raw.ingest_watermarks IS 'Incremental cursors per source (ADR-006): advanced only AFTER the landing commit; kinds: max_created_at (soap cursor), last_full_walk (observability only)'")
        conn.execute("COMMENT ON TABLE raw.ingest_loads IS 'Run ledger (ADR-007): one row per ingest run, counters updated inside each batch transaction so a SIGKILL leaves a forensic status=running row'")
        conn.execute("COMMENT ON TABLE raw.ingest_files IS 'File consumption ledger (ADR-006/007): process a file iff present AND (unknown OR hash changed); never date/filename based'")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ingest_quarantine_replay_guard ON raw.ingest_quarantine (source, batch_ref, row_number, reason)")
        conn.execute("COMMENT ON TABLE raw.ingest_quarantine IS 'Rejected rows with context (ADR-007): per-row reason codes ragged_width/encoding_not_utf8/unknown_header/invalid_pk/missing_pk; UNIQUE guard makes crash-replay re-quarantine nothing'")
        conn.execute("COMMENT ON TABLE raw.soap_orders IS 'Batch landing, soap-service GetOrders (ADR-006/007): natural-key upsert guarded by content_hash; watermark=max(created_at)'")
        conn.execute("COMMENT ON TABLE raw.file_customers IS 'Batch landing, file-drop customers CSV (ADR-006/007): natural-key upsert guarded by content_hash; per-file ledger controls increments'")
        conn.execute("COMMENT ON TABLE raw.file_products IS 'Batch landing, file-drop products CSV (ADR-006/007): natural-key upsert guarded by content_hash; per-file ledger controls increments'")
        conn.execute("COMMENT ON TABLE raw.rest_products IS 'Batch landing, rest-mock /products (ADR-006/007): full cursor walk per run, natural-key upsert guarded by content_hash'")
        conn.execute("COMMENT ON TABLE raw.rest_promotions IS 'Batch landing, rest-mock /promotions (ADR-006/007): full cursor walk per run, natural-key upsert guarded by content_hash'")
