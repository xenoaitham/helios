# DATA DICTIONARY — HELIOS

Filled from Phase 1 onward; the structure below is the contract every future table entry
follows.

## Policy

- Every column recorded here gets: source system, raw landing table, staging model,
  mart destination(s), data type, nullability, and a **PII classification**:
  - `none` — business keys, amounts, dates, statuses.
  - `pseudonymized` — direct identifiers (email, phone, full names) replaced in
    **staging** by `SHA-256(lower(trim(value)) || salt)`; salt comes from `.env`
    (`PII_HASH_SALT`, Phase 3). Raw keeps the original (landing-zone contract);
    marts NEVER receive cleartext PII.
  - `masked` — quasi-identifiers kept analytically useful but coarsened
    (e.g. birth year only, email domain only).
- Rationale (compliance-adjacent, documented for interviews): hashing preserves
  joinability across feeds at exact-match granularity while removing cleartext PII from
  all consumer-facing schemas; salt lives outside the repo; determinism is required for
  SCD2 change detection on identity fields.
- The DQ gate (Phase 4) includes a test that marts contain no cleartext PII patterns.

## Tables

### Source system: SOAP OrderManagement (`helios-soap-service`, Phase 1)

Legacy order system (ADR-001). SQLite store on the `soap_data` volume, table/column
shape below; the SOAP contract (`soap-service/contract/OrderManagement.wsdl`) exposes a
subset. All timestamps are naive UTC `YYYY-MM-DD HH:MM:SS`. Money is stored as integer
cents and presented as `Decimal(2dp)` in the SOAP contract — no float money.
Raw/staging/mart destinations are filled in when Phase 2/3 land the extraction. The
**CDC landing tables** below arrived with Phase 2 (ADR-005); the rest follow with the
ingest lib (Phase 2) and dbt staging (Phase 3).

`orders` (~382k rows seeded):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_id | INTEGER PK | no | server-assigned, sequential | none |
| customer_id | INTEGER | no | references an external customer registry (1..5000) | none (pseudonymous key; no direct identifiers exist in this system) |
| status | TEXT | no | NEW/PROCESSING/SHIPPED/DELIVERED/CANCELLED, state machine per ADR-001 | none |
| total_cents | INTEGER | no | sum(quantity × unit_price_cents) | none |
| currency | TEXT | no | always 'USD' | none |
| client_reference | TEXT UNIQUE (partial) | yes | client idempotency key | none |
| client_ref_hash | TEXT | yes | sha256 of canonical payload; conflict detection for replays | none |
| created_at / updated_at | TEXT | no | UTC; updated_at advances with each status change (watermark candidate for Phase 2 extraction) | none |

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
identifiers** — only numeric customer keys. Nothing to hash at staging; the PII policy
applies to sources that carry emails/names (arriving with later Phase 1 components and
the OLTP schema).

### Source system: OLTP Postgres (`helios-oltp-db`, Phase 1, ADR-002)

The "modern" operational source: `users / orders / order_items / payments` on the
`oltp_data` volume, seeded deterministically (5.4M rows) and mutated continuously by
`oltp-mutator`. Money is `NUMERIC(*,2)` (never floats). Timestamps are `timestamptz`
UTC. This is the source that *does* carry synthetic direct identifiers — the users
table is the staging PII-hashing demo target for Phase 3. All values are synthetic;
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

`orders` (500,000+ rows, grows/shrinks with the mutator):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_id | BIGINT identity PK | no | sequential; mutator inserts beyond seed max | none |
| user_id | BIGINT FK → users | no | | none (pseudonymous key) |
| status | TEXT | no | NEW/PROCESSING/SHIPPED/DELIVERED/CANCELLED — same vocabulary as the SOAP source; mutator walks it forward | none |
| currency | TEXT | no | 'USD' | none |
| total_amount | NUMERIC(12,2) | no | derived from the same item specs as order_items — invariant: equals SUM(quantity × unit_price) per order | none |
| placed_at | TIMESTAMPTZ | no | uniform over 3 years; `idx_orders_placed_at` = Phase-2 watermark index | none |
| updated_at | TIMESTAMPTZ | no | advances on mutator status walks (CDC-friendly) | none |

`order_items` (~4.25M rows):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| order_item_id | BIGINT identity PK | no | | none |
| order_id | BIGINT FK → orders (CASCADE) | no | | none |
| product_sku | TEXT | no | `SKU-#####`, 100k distinct values | none |
| quantity | INTEGER | no | 1..5 | none |
| unit_price | NUMERIC(10,2) | no | $1.99–$151.98 | none |

`payments` (600k+ rows):

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| payment_id | BIGINT identity PK | no | | none |
| order_id | BIGINT FK → orders (CASCADE) | no | | none |
| method | TEXT | no | card/paypal/bank_transfer/gift_card | none |
| amount | NUMERIC(12,2) | no | order total, or its **negation** for REFUNDED rows | none |
| status | TEXT | no | CAPTURED / PENDING (~1%) / REFUNDED (~20% of orders have a second, negative row) | none |
| paid_at | TIMESTAMPTZ | no | placed_at + 0..71 h | none |

### Source system: REST Pricing & Promotions (`helios-rest-mock`, Phase 1, ADR-003)

Mock pricing edge API over an **in-memory, static** deterministic catalog (no runtime
writes — read flakiness is the modeled behavior; change capture comes from oltp/soap).
Cursor-paginated (`next_cursor`, keyset by id). Money crosses the API as integer cents
(`price_cents` + `currency`) — never floats. SKUs live in the same `SKU-#####` space as
the OLTP source and use the same price formula, so Phase-3 joins are coherent.

`products` (5,000 rows): id, sku (unique), name, category, `price_cents`
(199 + (sku_num × 613) mod 14999), currency — PII class: none.

`promotions` (2,500 rows): id, product_sku, discount_percent (5–50), starts_at/ends_at
(ISO dates, windows anchored to a fixed base date), description — PII class: none.

PII statement for this source: **no identifiers of any kind** — product/pricing data
only. Nothing to hash at staging.

### Source system: file-drop CSV feeds (`filedrop_data` volume, Phase 1, ADR-004)

"Nightly" vendor exports landing in an SFTP-style drop volume
(`customers-<YYYYMMDD>.csv`, `products-<YYYYMMDD>.csv`). Deliberately dirty: verbatim
duplicate rows (~2%), ragged columns (short/long), one cp1252 row inside UTF-8 files,
late-arriving backdated batches. The CSV headers below are the *contract*; the Phase-2
extractor must enforce it against the documented dirt.

`customers-<date>.csv` (5,000 rows per batch) — **the PII-carrying batch feed**:

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
sources), supplier_code (SUP-A..D) — PII class: none.

PII statement for this source: the **customers feed is the batch-feed PII surface** —
email + full_name must be hashed at staging exactly like the OLTP `users` columns; the
products feed carries no identifiers.

## Warehouse raw zone — CDC landing tables (Phase 2, ADR-005)

`raw.cdc_users`, `raw.cdc_orders`, `raw.cdc_order_items`, `raw.cdc_payments` — one per
OLTP source table, populated by `cdc-sink` from Debezium envelopes (topics
`helios.public.<table>`). Raw stays *source-shaped and as-landed*: money arrives as
strings (`decimal.handling.mode=string`), timestamps as ISO-8601 strings; casting to
typed columns is Phase-3 staging's job.

| Column | Type | Null | Notes | PII class |
|---|---|---|---|---|
| pk | JSONB PRIMARY KEY | no | Debezium key payload, e.g. `{"user_id": 42}` — uniform for any future composite key | inherits the source PK (none) |
| lsn | BIGINT | no | WAL log sequence number of the change (snapshot rows carry the snapshot LSN); the sink's idempotency guard is `WHERE EXCLUDED.lsn >= table.lsn` | none |
| op | TEXT | no | `r` snapshot read / `c` create / `u` update / `d` delete-tombstone | none |
| ts_ms | BIGINT | no | source commit timestamp (epoch ms) | none |
| before / after | JSONB | yes | Debezium before/after images; `after` IS NULL exactly when `op='d'` | same as the source columns they mirror (see source tables above; users email/full_name are the PII surface) |
| landed_at | TIMESTAMPTZ | no | when the sink applied the event (defaults to `now()`) | none |

Row-count semantics: one row per PK ever captured — deletes stay as `op='d'`
tombstone rows (staging filters them in Phase 3), so
`count(*) WHERE op <> 'd'` tracks the live source row count (verified equal in
`make cdc-verify` stage [2]).
