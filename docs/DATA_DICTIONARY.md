# DATA DICTIONARY - HELIOS

Complete - the platform shipped in full. Column lists below were verified against the LIVE
schemas (`information_schema` on `oltp-db` and `warehouse-db`) on 2026-09-15 -
not from memory. Living row counts are labeled "as of" with their date; the
mutator moves them hourly, so measure rather than trust any count.

## Policy

- Every column recorded here gets: source system, raw landing table, staging model,
  mart destination(s), data type, nullability, and a **PII classification**:
  - `none` - business keys, amounts, dates, statuses.
  - `pseudonymized` - direct identifiers (email, phone, full names) replaced in
    **staging** by `SHA-256(lower(trim(value)) || salt)`; salt comes from `.env`
    (`PII_HASH_SALT`, from `.env`). Raw keeps the original (landing-zone contract);
    marts NEVER receive cleartext PII.
  - `masked` - quasi-identifiers kept analytically useful but coarsened
    (e.g. birth year only, email domain only).
- Rationale (compliance-adjacent design note): hashing preserves
  joinability across feeds at exact-match granularity while removing cleartext PII from
  all consumer-facing schemas; salt lives outside the repo; determinism is required for
  SCD2 change detection on identity fields.
- Marts PII-freedom is enforced by CONSTRUCTION (the staging models select only
  `*_hash` - the cleartext column never enters the dbt DAG) and PROVEN by SQL
  audits run at build time (0 cleartext columns, 0 non-hex hashes)
  plus a smoke assertion. The GE gate owns business semantics (ADR-011 D2: 6
  suites / 10 expectations - money sign policy, line sanity, feed sanity,
  temporal coherence, published money), NOT PII pattern-matching.

## Tables

### Source system: SOAP OrderManagement (`helios-soap-service`)

Legacy order system (ADR-001). SQLite store on the `soap_data` volume, table/column
shape below; the SOAP contract (`soap-service/contract/OrderManagement.wsdl`) exposes a
subset. All timestamps are naive UTC `YYYY-MM-DD HH:MM:SS`. Money is stored as integer
cents and presented as `Decimal(2dp)` in the SOAP contract - no float money.
Raw, staging and mart destinations are listed per table below. The
**CDC landing tables** landed via ADR-005; the batch tables via the
ingest lib (ADR-007); staging mappings via dbt (ADR-008).

`orders` (382,179 rows - seeded once at first boot, never reseeded; invariant):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_id | INTEGER PK | no | server-assigned, sequential | none |
| customer_id | INTEGER | no | references an external customer registry (1..5000) | none (pseudonymous key; no direct identifiers exist in this system) |
| status | TEXT | no | NEW/PROCESSING/SHIPPED/DELIVERED/CANCELLED, state machine per ADR-001 | none |
| total_cents | INTEGER | no | sum(quantity × unit_price_cents) | none |
| currency | TEXT | no | always 'USD' | none |
| client_reference | TEXT UNIQUE (partial) | yes | client idempotency key | none |
| client_ref_hash | TEXT | yes | sha256 of canonical payload; conflict detection for replays | none |
| created_at / updated_at | TEXT | no | UTC; updated_at advances with each status change (the SOAP extractor's watermark column) | none |

`order_items` (~785k rows):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_id, line_no | INTEGER composite PK | no | FK to orders | none |
| product_id | INTEGER | no | external catalog reference (1..2000) | none |
| quantity | INTEGER | no | 1..3 in seeded data | none |
| unit_price_cents | INTEGER | no | stable per product | none |

`order_status_history` (~1.46M rows):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| history_id | INTEGER PK | no | | none |
| order_id | INTEGER FK | no | | none |
| from_status / to_status | TEXT | from nullable | first row is NULL→NEW | none |
| note | TEXT | yes | free text from UpdateOrderStatus | none (not present in seeded data) |
| changed_at | TEXT | no | UTC | none |

PII statement for this source: the OrderManagement system holds **no direct
identifiers** - only numeric customer keys. Nothing to hash at staging; the PII policy
applies to sources that carry emails/names (the OLTP schema and the
batch customer feed carry them).

### Source system: OLTP Postgres (`helios-oltp-db`, ADR-002)

The "modern" operational source: `users / orders / order_items / payments` on the
`oltp_data` volume, seeded deterministically (5.4M rows) and mutated continuously by
`oltp-mutator`. Money is `NUMERIC(*,2)` (never floats). Timestamps are `timestamptz`
UTC. This is the source that *does* carry synthetic direct identifiers - the users
table is the staging PII-hashing demo target. All values are synthetic;
emails use RFC-2606 `example.com` domains on purpose.

`users` (50,000 rows + mutator-neutral):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| user_id | BIGINT identity PK | no | | none |
| email | TEXT UNIQUE | no | `first.last.<uid>@example.com` | **pseudonymized** (staging: SHA-256 + salt) |
| full_name | TEXT | no | synthetic first/last | **pseudonymized** (staging: SHA-256 + salt) |
| country_code | TEXT | no | 2-letter ISO-ish from a 10-value pool | masked (quasi-identifier) |
| created_at / last_login_at | TIMESTAMPTZ | last_login nullable | last_login is the mutator's touch column | none |
| is_active | BOOLEAN | no | 95% true | none |
| loyalty_tier | TEXT | yes | **ALL VALUES NULL.** Not in the seed schema: added by the chaos-06 Act-A probe (`ALTER TABLE users ADD COLUMN loyalty_tier text`, 2026-09-14, ADR-014) and left in place BY DESIGN - dropping it would itself be a drift event, and a NULL column is inert: `staging.stg_users` does not select it, Debezium ships it as `"loyalty_tier": null` in every after-image, and the JSONB raw landing absorbs it. It stands as the queryable, live exhibit of the additive-drift gap (found standing by the final docs sweep, ADR-016 D4) | none |

`orders` (762,261 rows as of 2026-09-15 - grows/shrinks with the mutator):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_id | BIGINT identity PK | no | sequential; mutator inserts beyond seed max | none |
| user_id | BIGINT FK → users | no | | none (pseudonymous key) |
| status | TEXT | no | NEW/PROCESSING/SHIPPED/DELIVERED/CANCELLED - same vocabulary as the SOAP source; mutator walks it forward | none |
| currency | TEXT | no | 'USD' | none |
| total_amount | NUMERIC(12,2) | no | derived from the same item specs as order_items - invariant: equals SUM(quantity × unit_price) per order | none |
| placed_at | TIMESTAMPTZ | no | uniform over 3 years; `idx_orders_placed_at` = the extractor watermark index | none |
| updated_at | TIMESTAMPTZ | no | advances on mutator status walks (CDC-friendly) | none |

`order_items` (5,568,905 rows as of 2026-09-15):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_item_id | BIGINT identity PK | no | | none |
| order_id | BIGINT FK → orders (CASCADE) | no | | none |
| product_sku | TEXT | no | `SKU-#####`, 100k distinct values | none |
| quantity | INTEGER | no | 1..5 | none |
| unit_price | NUMERIC(10,2) | no | $1.99–$151.98 | none |

`payments` (854,184 rows as of 2026-09-15):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| payment_id | BIGINT identity PK | no | | none |
| order_id | BIGINT FK → orders (CASCADE) | no | | none |
| method | TEXT | no | card/paypal/bank_transfer/gift_card | none |
| amount | NUMERIC(12,2) | no | order total, or its **negation** for REFUNDED rows | none |
| status | TEXT | no | CAPTURED / PENDING (~1%) / REFUNDED (~20% of orders have a second, negative row) | none |
| paid_at | TIMESTAMPTZ | no | placed_at + 0..71 h | none |

### Source system: REST Pricing & Promotions (`helios-rest-mock`, ADR-003)

Mock pricing edge API over an **in-memory, static** deterministic catalog (no runtime
writes - read flakiness is the modeled behavior; change capture comes from oltp/soap).
Cursor-paginated (`next_cursor`, keyset by id). Money crosses the API as integer cents
(`price_cents` + `currency`) - never floats. SKUs live in the same `SKU-#####` space as
the OLTP source and use the same price formula, so warehouse joins are coherent.

`products` (5,000 rows): id, sku (unique), name, category, `price_cents`
(199 + (sku_num × 613) mod 14999), currency - PII class: none.

`promotions` (2,500 rows): id, product_sku, discount_percent (5–50), starts_at/ends_at
(ISO dates, windows anchored to a fixed base date), description - PII class: none.

PII statement for this source: **no identifiers of any kind** - product/pricing data
only. Nothing to hash at staging.

### Source system: file-drop CSV feeds (`filedrop_data` volume, ADR-004)

"Nightly" vendor exports landing in an SFTP-style drop volume
(`customers-<YYYYMMDD>.csv`, `products-<YYYYMMDD>.csv`). Deliberately dirty: verbatim
duplicate rows (~2%), ragged columns (short/long), one cp1252 row inside UTF-8 files,
late-arriving backdated batches. The CSV headers below are the *contract*; the
extractor must enforce it against the documented dirt.

`customers-<date>.csv` (5,000 rows per batch) - **the PII-carrying batch feed**:

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| customer_id | INTEGER | no | 1..5000, stable across batches | none (pseudonymous key) |
| email | TEXT | no | `first.last.<id>@example.com` (synthetic, RFC-2606 domain) | **pseudonymized** (staging: SHA-256 + salt) |
| full_name | TEXT | no | synthetic first/last | **pseudonymized** (staging: SHA-256 + salt) |
| country_code | TEXT | no | 2-letter from a 10-value pool | masked (quasi-identifier) |
| signup_date | DATE | no | within ~4 years before the batch date | none |
| tier | TEXT | no | bronze/silver/gold/platinum | none |

`products-<date>.csv` (2,000 rows per batch): sku (unique, `SKU-#####` in the shared
OLTP/rest-mock space), name, category, `price_cents` (same formula as the other
sources), supplier_code (SUP-A..D) - PII class: none.

PII statement for this source: the **customers feed is the batch-feed PII surface** -
email + full_name must be hashed at staging exactly like the OLTP `users` columns; the
products feed carries no identifiers.

## Warehouse raw zone - CDC landing tables (ADR-005)

`raw.cdc_users`, `raw.cdc_orders`, `raw.cdc_order_items`, `raw.cdc_payments` - one per
OLTP source table, populated by `cdc-sink` from Debezium envelopes (topics
`helios.public.<table>`). Raw stays *source-shaped and as-landed*: money arrives as
strings (`decimal.handling.mode=string`), timestamps as ISO-8601 strings; casting to
typed columns is staging's job.

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| pk | JSONB PRIMARY KEY | no | Debezium key payload, e.g. `{"user_id": 42}` - uniform for any future composite key | inherits the source PK (none) |
| lsn | BIGINT | no | WAL log sequence number of the change (snapshot rows carry the snapshot LSN); the sink's idempotency guard is `WHERE EXCLUDED.lsn >= table.lsn` | none |
| op | TEXT | no | `r` snapshot read / `c` create / `u` update / `d` delete-tombstone | none |
| ts_ms | BIGINT | no | source commit timestamp (epoch ms) | none |
| before / after | JSONB | yes | Debezium before/after images; `after` IS NULL exactly when `op='d'` | same as the source columns they mirror (see source tables above; users email/full_name are the PII surface) |
| landed_at | TIMESTAMPTZ | no | when the sink applied the event (defaults to `now()`) | none |

Row-count semantics: one row per PK ever captured - deletes stay as `op='d'`
tombstone rows (staging filters them), so
`count(*) WHERE op <> 'd'` tracks the live source row count (verified equal in
`make cdc-verify` stage [2]).

## Warehouse raw zone - batch landing + ledgers (ADR-007)

The batch extractors land into per-source natural-key tables; the shared shape
is `⟨natural_key, load_id, batch_ref, content_hash, payload JSONB, landed_at⟩`
(content-hash-guarded upserts = a physical no-op on replay). Columns verified
against the live warehouse 2026-09-15:

| Table | Natural key | Payload PII |
|---|---|---|
| `raw.soap_orders` | `order_id` | none (SOAP holds no identifiers) |
| `raw.file_customers` | `customer_id` | **cleartext `email` / `full_name` inside `payload`** - the landing-zone contract: raw is source-shaped; hashing happens at staging (ADR-008) |
| `raw.file_products` | `sku` | none |
| `raw.rest_products` | `id` | none |
| `raw.rest_promotions` | `id` | none |

Ledger tables (the replay/idempotence bookkeeping):

| Table | Columns | Role |
|---|---|---|
| `raw.ingest_watermarks` | source, watermark_value, watermark_kind, updated_at | per-source incremental cursors (ADR-006) |
| `raw.ingest_files` | filename, file_hash, size_bytes, load_id, rows_read, rows_landed, rows_quarantined, landed_at | file hash ledger - a file is re-processed only when its BYTES change, never on filename dates |
| `raw.ingest_loads` | load_id, source, batch_ref, status, rows_read, rows_landed, rows_unchanged, rows_quarantined, units_done, units_total, error, started_at, finished_at | run ledger - the zero-work proof for every replay |
| `raw.ingest_quarantine` | quarantine_id, source, batch_ref, filename, row_number, reason, raw_content, load_id, quarantined_at | row-level rejects with physical row numbers (dirty-CSV triage) |

## Warehouse staging schema (ADR-008) - dbt's first tenant

Nine dbt models, materialized as tables and rebuilt full on every
`make dbt-build`; CDC current state = `op <> 'd'` over the raw envelopes; batch
payloads extracted + typed. Provenance: CDC models carry `_cdc_lsn`, batch models
`_batch_ref` (joins to the ADR-007 run ledger). Models with their live column
lists (verified 2026-09-15; `⟨k⟩_hash` = the ADR-008 D1 formula):

| Model | Columns (beyond provenance) |
|---|---|
| `stg_users` | user_id, **email_hash, full_name_hash**, country_code, is_active, created_at, last_login_at (`_cdc_lsn`) |
| `stg_orders` | order_id, user_id, status, currency, total_amount, placed_at, updated_at (`_cdc_lsn`) |
| `stg_order_items` | order_item_id, order_id, product_sku, quantity, unit_price (`_cdc_lsn`) |
| `stg_payments` | payment_id, order_id, method, amount, status, paid_at (`_cdc_lsn`) |
| `stg_soap_orders` | order_id, customer_id, status, total_amount, currency, created_at, updated_at (`_batch_ref`) |
| `stg_file_customers` | customer_id, **email_hash, full_name_hash**, country_code, signup_date, tier (`_batch_ref`) |
| `stg_file_products` | sku, name, category, price_cents, supplier_code (`_batch_ref`) |
| `stg_rest_products` | product_id, sku, name, category, currency, price_cents (`_batch_ref`) |
| `stg_rest_promotions` | promotion_id, product_sku, discount_percent, starts_at, ends_at, description (`_batch_ref`) |

PII mechanics (ADR-008 D1): `email` and `full_name` (OLTP `users` + file
`customers` feeds) appear in staging **only** as
`email_hash` / `full_name_hash` =
`SHA-256(lower(trim(value)) || PII_HASH_SALT)` (lowercase hex, pgcrypto; salt
from `.env`, never committed). Determinism is the point: the same cleartext
hashes identically across feeds and runs (7 cross-feed email pairs verified
equal across feeds at build time), which is what SCD2 needs. Rotating the salt
invalidates every hash at once. Raw keeps cleartext; marts must never receive
it (re-checked by the DQ gate).

Known namespace skew (measured, not a defect): OLTP items reference 100,000
distinct `product_sku` values while the product catalogs (REST ∪ file) cover
6,895 of them - the catalog is a subset of the SKU namespace by seed design.
`stg_rest_promotions.product_sku → catalog` IS total (0 missing) and is
asserted by a dbt test; items→catalog is deliberately not asserted.

## Warehouse marts schema + snapshots (ADR-009)

Star schema built from staging only (`ref()` - never raw). Facts and dims are
tables rebuilt on every `make dbt-build`; the ONE persistent relation is the
dbt snapshot `snapshots.customers_snapshot` (check strategy over
`[email_hash, full_name_hash, country_code, is_active]`; `last_login_at`
deliberately excluded - the mutator touches it ~40 users/tick and would
fabricate versions). `make dbt-build` is never run with `--full-refresh`
(a snapshot full-refresh wipes history); `make clean` is the only sanctioned
wipe.

- `snapshots.customers_snapshot` - SCD2 state, one row per (user_id, version):
  hashed attrs + dbt metadata (`dbt_scd_id`, `dbt_valid_from/to`,
  `dbt_updated_at`). History begins at first run (2026-09-11).
- `marts.dim_customer` - one row per customer version; `customer_sk` =
  `dbt_scd_id`, `customer_id` = user_id, half-open `[valid_from, valid_to)`,
  `is_current` = (`valid_to IS NULL`). email/full_name → `*_hash`
  (`pseudonymized`, same ADR-008 formula). Everything else `none`.
- `marts.dim_product` - SCD1, natural key `sku`; columns: sku, name, category,
  price_cents, currency, supplier_code, source_feed; rest∪file union (REST
  precedence on the 105-SKU overlap), `price_cents` stays integer cents,
  `source_feed` ∈ {rest, file}. PII `none`.
- `marts.dim_date` - generated calendar, `date_key` = YYYYMMDD int, day grain
  spanning the order timelines. PII `none`.
- `marts.fct_orders` - grain `(source_type, order_id)` (namespaces collide:
  350,876 OLTP rows measured sharing an id with SOAP - composite uniqueness is
  tested). OLTP rows resolve `customer_sk` by SCD2 point-in-time window
  (orders older than snapshot history fall back to the earliest known version
  - documented limitation); SOAP rows carry the Kimball unknown member
  (`customer_sk IS NULL` - SOAP has no identity attribute to resolve). PII
  `none`.
- `marts.fct_order_items` - line grain `order_item_id`; columns:
  order_item_id, source_type, order_id, customer_sk, order_ts, order_date_key,
  product_sku, product_source_feed, quantity, unit_price, line_revenue. The
  dim join is LEFT: ~6.89 % of rows match the catalog by seed design;
  unmatched = NULL product attributes, never dropped.
  `line_revenue = quantity * unit_price`. PII `none`.

## Warehouse dq schema (ADR-011) - the semantic gate's dead-letter

Created and owned by the `dq` tool (Great Expectations 1.22.0, one-shot
container) at every gate start; the `dq_gate` task runs it downstream of
`dbt_build` in `daily_close`.

- `dq.dq_quarantine` - one row per open DQ incident; identity
  `(data_asset, check_digest, source_pk_hash)` among OPEN rows (partial unique
  index `WHERE resolved_at IS NULL`), so re-detection refreshes
  `run_id`/`landed_at` instead of duplicating, and a re-poisoned row after
  resolution opens a NEW incident. Columns: `quarantine_id` (serial PK),
  `suite`, `data_asset`,
  `expectation` (GX type), `check_digest` (rule identity = type + semantic
  kwargs), `column_name`, `row_condition`, `source_pk` (JSONB), `payload`
  (JSONB - the offending row), `failure_reason` (rule + observed values +
  shortfall beyond `DQ_QUARANTINE_MAX_ROWS`), `run_id`, `status`
  (`open`/`resolved`), `landed_at`, `resolved_at`, `resolved_run_id`.
  Resolution is set by `make dq-replay` after a source fix + rebuild;
  resolved history is never deleted (audit trail). PII: payloads are
  staging/marts rows - `*_hash` only, no cleartext (the gate never reads
  `raw`).

