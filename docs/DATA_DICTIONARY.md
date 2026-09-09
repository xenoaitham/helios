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
Raw/staging/mart destinations are filled in when Phase 2/3 land the extraction.

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
