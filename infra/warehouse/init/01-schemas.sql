-- HELIOS warehouse bootstrap — runs ONLY on first start with an empty data volume
-- (docker-entrypoint-initdb.d). To re-run from scratch: `make clean && make up`
-- (destructive: wipes all platform data).

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw     IS 'Immutable landing zone: batch extracts + CDC change events. Append-only, source-shaped.';
COMMENT ON SCHEMA staging IS 'Typed, cleaned models. PII hashed/masked here (see docs/DATA_DICTIONARY.md).';
COMMENT ON SCHEMA marts   IS 'Dimensional star schema (fct_orders, fct_order_items, dim_customer SCD2, dim_product, dim_date).';
