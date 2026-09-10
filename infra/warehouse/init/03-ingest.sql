-- HELIOS warehouse batch-ingest landing + control tables (ADR-006/007) —
-- fresh-volume init only. Runs on FIRST start with an empty data volume
-- (docker-entrypoint-initdb.d). MIRRORS ingest/ensure.py (the source of
-- truth): the runner re-ensures this DDL at the start of every invocation, so
-- existing volumes (not re-run here) are covered too.

CREATE TABLE IF NOT EXISTS raw.ingest_watermarks (source TEXT PRIMARY KEY, watermark_value TEXT NOT NULL, watermark_kind TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.ingest_loads (load_id UUID PRIMARY KEY, source TEXT NOT NULL, batch_ref TEXT, status TEXT NOT NULL, rows_read BIGINT NOT NULL DEFAULT 0, rows_landed BIGINT NOT NULL DEFAULT 0, rows_unchanged BIGINT NOT NULL DEFAULT 0, rows_quarantined BIGINT NOT NULL DEFAULT 0, units_done BIGINT NOT NULL DEFAULT 0, units_total BIGINT NOT NULL DEFAULT 0, error TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS raw.ingest_files (filename TEXT PRIMARY KEY, file_hash TEXT NOT NULL, size_bytes BIGINT NOT NULL, load_id UUID NOT NULL, rows_read BIGINT NOT NULL DEFAULT 0, rows_landed BIGINT NOT NULL DEFAULT 0, rows_quarantined BIGINT NOT NULL DEFAULT 0, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.ingest_quarantine (quarantine_id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, batch_ref TEXT NOT NULL, filename TEXT, row_number BIGINT, reason TEXT NOT NULL, raw_content TEXT, load_id UUID NOT NULL, quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (source, batch_ref, row_number, reason));
CREATE TABLE IF NOT EXISTS raw.soap_orders (order_id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.file_customers (customer_id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.file_products (sku TEXT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.rest_products (id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS raw.rest_promotions (id BIGINT PRIMARY KEY, load_id UUID NOT NULL, batch_ref TEXT NOT NULL, content_hash TEXT NOT NULL, payload JSONB NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE UNIQUE INDEX IF NOT EXISTS ingest_quarantine_replay_guard ON raw.ingest_quarantine (source, batch_ref, row_number, reason);

COMMENT ON TABLE raw.ingest_watermarks IS 'Incremental cursors per source (ADR-006): advanced only AFTER the landing commit; kinds: max_created_at (soap cursor), last_full_walk (observability only)';
COMMENT ON TABLE raw.ingest_loads IS 'Run ledger (ADR-007): one row per ingest run, counters updated inside each batch transaction so a SIGKILL leaves a forensic status=running row';
COMMENT ON TABLE raw.ingest_files IS 'File consumption ledger (ADR-006/007): process a file iff present AND (unknown OR hash changed); never date/filename based';
COMMENT ON TABLE raw.ingest_quarantine IS 'Rejected rows with context (ADR-007): per-row reason codes ragged_width/encoding_not_utf8/unknown_header/invalid_pk/missing_pk; UNIQUE guard makes crash-replay re-quarantine nothing';
COMMENT ON TABLE raw.soap_orders IS 'Batch landing, soap-service GetOrders (ADR-006/007): natural-key upsert guarded by content_hash; watermark=max(created_at)';
COMMENT ON TABLE raw.file_customers IS 'Batch landing, file-drop customers CSV (ADR-006/007): natural-key upsert guarded by content_hash; per-file ledger controls increments';
COMMENT ON TABLE raw.file_products IS 'Batch landing, file-drop products CSV (ADR-006/007): natural-key upsert guarded by content_hash; per-file ledger controls increments';
COMMENT ON TABLE raw.rest_products IS 'Batch landing, rest-mock /products (ADR-006/007): full cursor walk per run, natural-key upsert guarded by content_hash';
COMMENT ON TABLE raw.rest_promotions IS 'Batch landing, rest-mock /promotions (ADR-006/007): full cursor walk per run, natural-key upsert guarded by content_hash';
