# ADR-008 — dbt staging: PII pseudonymization, materialization & freshness

Date: 2026-09-11 · Status: accepted · Phase: 3 (item 7)
Decides: dbt project shape on the warehouse; how raw becomes typed staging; where
PII dies; materialization; consistency semantics on live CDC; freshness thresholds;
dbt version pinning.

## Context

Phase 2 left a landed, live raw zone on `warehouse-db` (Postgres 16, one instance,
schemas `raw / staging / marts` — the latter two empty since init; dbt is their
first tenant):

- **CDC tables** `raw.cdc_users / cdc_orders / cdc_order_items / cdc_payments`
  (ADR-005): envelope `pk JSONB PK, lsn, op r|c|u|d, ts_ms, before, after JSONB,
  landed_at`. The sink keeps **one row per pk** — the latest event wins via the
  lsn-guarded upsert; deletes stay as `op='d'` tombstones with `after IS NULL`.
  Current state is therefore a plain `WHERE op <> 'd'` filter — no dedupe window.
  Money/timestamps arrive as strings (`decimal.handling.mode=string`, ISO-8601).
  `oltp-mutator` writes every ~2 s, so these tables churn continuously.
- **Batch tables** `raw.soap_orders / file_customers / file_products /
  rest_products / rest_promotions` (ADR-006/007): envelope
  `<natural pk>, load_id, batch_ref, content_hash, payload JSONB, landed_at`;
  payload keys per `docs/DATA_DICTIONARY.md`. Stable between ingest runs.
- PII contract (already written in `docs/DATA_DICTIONARY.md`): email + full_name
  on `users` (CDC) and `file_customers` (batch) are `pseudonymized`; raw keeps the
  original; **marts never receive cleartext PII**. This ADR formalizes the
  mechanics. All data is synthetic (RFC-2606 `example.com`); salt is a local-dev
  secret managed like every other credential in this repo.

Measured facts this ADR relies on (see EVIDENCE/phase-3-staging.md):
- SKU space: oltp items reference 100,000 distinct SKUs; the rest∪file catalog
  covers 6,895 of them. **`promotions.product_sku` has 0 SKUs missing from the
  catalog** → promotions→products is a total relationship. items→products is
  partial *by seed design* (the catalog is a subset of the namespace) — it is NOT
  tested as a referential constraint, only documented.
- `raw.cdc_*` PK constraint: `PRIMARY KEY (pk)` — one row per pk (queryable).

## Decisions

### D1 — PII pseudonymization mechanics (the contract)

`staging` exposes `email_hash` / `full_name_hash`, computed as:

```
encode(digest(lower(trim(value)) || '<PII_HASH_SALT>', 'sha256'), 'hex')
```

- Implemented once in macro `macros/pii_hash.sql`; every model calls the macro —
  the formula exists in exactly one place.
- Salt comes from env `PII_HASH_SALT` via `env_var()` at parse time. It is never
  committed: `.env` only; `.env.example` carries the documented local-dev default
  `helios_local_dev_pii_salt` (same honesty pattern as `*_local_dev` passwords).
- `pgcrypto` is installed idempotently by an `on-run-start` hook
  (`create extension if not exists pgcrypto`) — the warehouse volume predates
  Phase 3, so init SQL cannot be relied on; the hook keeps `make dbt-build`
  self-sufficient and safe to rerun.
- Determinism is the point: the same value hashes identically across feeds and
  runs (SCD2 change detection on identity fields needs this — item 8). Salted
  SHA-256 is pseudonymization, not encryption: brute-forcing low-entropy emails is
  feasible given the salt — accepted because raw (which holds cleartext) is the
  system of record anyway; staging's job is to keep cleartext out of every
  consumer-facing surface, and it does.
- Concatenation without a separator follows the documented dictionary formula
  verbatim; theoretical `('ab','c') vs ('a','bc')` ambiguity is accepted (values
  are emails/names; the contract text wins over an encoder change).
- Enforced in-repo: schema tests assert 64-hex format on all four hash columns;
  singular tests fail staging if any hash column contains `@` or a space
  (cleartext leftover detector); VERIFIER additionally proves absence via
  information_schema + content queries (rubric §6.6 needs a queryable proof).

### D2 — Materialization: **tables, full-refresh each build** (no incremental)

- The raw CDC tables churn every ~2 s; downstream consumers (item 8 marts, Phase 4
  DQ gates, dbt tests) need a stable read set. Views would re-scan raw per read
  (`stg_order_items` reads ~4.4M JSONB envelope rows) — tables pay that once per
  build, then every read is plain typed SQL.
- Volume is modest: ~4.3M typed rows for `stg_order_items` (the live item count),
  ~0.5M orders, ~600k payments, 50k users, ~392k batch rows — hundreds of MB;
  full refresh per build is minutes. Measured in EVIDENCE.
- **Incremental staging is the documented 100× lever, deliberately deferred**:
  lsn-watermarked incremental models would cut the items rebuild, but add
  watermark state and out-of-order handling to the simplest layer. Staging stays
  dumb; marts (item 8) decide where incrementality pays.

### D3 — Consistency on live CDC: read current state, warn-guarded relationships

Problem: models built sequentially from live tables can straddle a mutator event
(delete: order tombstone visible, its item-cascade tombstones not yet landed;
insert: order visible, concurrent touches not) → transient orphans that would
flake referential tests.

**Rejected design (measured, same day): a shared per-run `landed_at` cut.** An
`on-run-start` upserted `staging._dbt_cdc_cut` = max(landed_at) across the four
raw.cdc_* tables; every CDC model filtered `landed_at <= cut`. First real build:
**842 orphan orders / 80 missing users**, all 80 present in raw with `op<>'d'`.
Root cause: raw.cdc_* is a **state table** (ADR-005: one row per pk, sink
overwrites on every event). A `landed_at` ceiling is as-of-cut semantics, but a
state table cannot serve old reads — any pk updated after the cut has its only
row stamped post-cut, so the filter drops it entirely (the mutator touches
users' `last_login_at` continuously, hence 80 users). A cut is only sound over
*history*; raw keeps *state*. Cut removed.

Decision: CDC staging models read current state directly — `WHERE op <> 'd'`,
no time bound. `raw.cdc_*` is by construction a consistent-enough current-state
view at each read instant; cross-model skew is bounded by the interval between
model SELECTs (items, the long pole, ~22 s measured) and can only manifest as a
mutator delete transaction straddling two SELECTs (order+item tombstones land
within µs of each other from one source tx). Consequences:

- dbt tests in the same run read the **built tables**, so tests are
  deterministic regardless of raw churn during the run.
- Referential tests between **live** CDC models (`stg_order_items →
  stg_orders`, `stg_payments → stg_orders`) ship with `severity: warn` — a
  fired warn is a real cut-straddle event to record, not noise.
  `stg_orders.user_id → stg_users` stays `error`: users are mutator-neutral
  (no inserts/deletes; the single raw tombstone is the cdc-verify marker), so
  no straddle mode exists for that edge.
- promotions→products is `error` (batch tables are stable).
- If a portfolio-grade consistency cut is ever required (point-in-time
  staging), the prerequisite is history-shaped raw (append per (lsn, pk)) — an
  ADR-005 revision, out of scope here; item 8's marts ADR owns fct-level
  freshness instead.

### D4 — Freshness: per-source thresholds, no global default

`loaded_at_field: landed_at` everywhere; thresholds follow source cadence:

| source group | warn | error | rationale |
|---|---|---|---|
| `helios_oltp` (4 cdc tables) | 2 min | 10 min | steady-state freshness ≈2 s (measured); a 2-min silence means mutator/sink/connect trouble |
| `helios_soap` | 26 h | 50 h | daily-by-design batch; one missed day warns |
| `helios_file` | 26 h | 50 h | same |
| `helios_rest` | 26 h | 50 h | same |

A naive global threshold (e.g. 5 min) would fail all batch sources every run —
that is crying wolf by construction. `make dbt-freshness` runs `dbt source
freshness`; the scream is proven by deliberately pausing `oltp-mutator` (EVIDENCE).

### D5 — Runner: dumb one-shot image, Airflow stays out

- Image `helios/dbt:latest` built on `python:3.11-slim` (repo base convention),
  `dbt-core` + `dbt-postgres` **pinned to the current 1.9.x line**
  (exact pins in `dbt/requirements.txt` + EVIDENCE; matching minor versions).
  No dbt packages (`dbt_utils` etc.) — the project is hermetic: no runtime
  network fetch, so there is no `dbt deps` step to forget in item 9.
- Compose service `dbt`: one-shot TOOL (`profiles: ["tools"]`, `restart: "no"`),
  same pattern as `ingest`; `ENTRYPOINT ["dbt"]`, code baked into the image
  (rebuild after any change — the Session-4 rule; `make dbt-*` targets depend on
  `dbt-image` so this can't be forgotten).
- `profiles.yml` uses `env_var()` for **everything**; host/port carry in-net
  defaults (`warehouse-db:5432` — topology, not credentials); user/password/dbname
  have no defaults — they must come from the environment.
- Targets: `make dbt-build` (= `dbt build`: models + tests, the real pipeline
  unit), `make dbt-test` (= `dbt test` standalone), `make dbt-freshness`,
  `make dbt-image`. Airflow orchestrates these in item 9; the runner stays dumb.
- `make dbt-build` **never touches `raw`**: models read sources; the only writes
  are the idempotent `create extension if not exists pgcrypto` hook. Warehouse
  volume is never rebuilt.

### D6 — Model set (9) and contracts

| model | source | grain | typed columns of note |
|---|---|---|---|
| stg_users | cdc_users | user_id | email_hash, full_name_hash (D1), is_active bool, timestamptz |
| stg_orders | cdc_orders | order_id | user_id, status, total_amount numeric(12,2), placed/updated timestamptz |
| stg_order_items | cdc_order_items | order_item_id | order_id, product_sku, quantity, unit_price numeric(10,2) |
| stg_payments | cdc_payments | payment_id | order_id, method, amount numeric(12,2), status, paid_at |
| stg_soap_orders | soap_orders | order_id | customer_id, status, total_amount numeric(12,2), naive-UTC ts → timestamptz |
| stg_file_customers | file_customers | customer_id | email_hash, full_name_hash, signup_date date, tier |
| stg_file_products | file_products | sku | price_cents int (cents stay cents), supplier_code |
| stg_rest_products | rest_products | id | sku, price_cents int, currency |
| stg_rest_promotions | rest_promotions | id | product_sku, discount_percent int, starts/ends date |

Casting happens here (raw stays verbatim): strings→numeric, ISO strings and
naive `YYYY-MM-DD HH:MM:SS` (SOAP: interpreted as UTC)→timestamptz, JSON
bools/ints→native. Provenance: CDC models keep `_cdc_lsn`; batch models keep
`_batch_ref` (joins to the ADR-007 run ledger). Cleartext PII columns are never
projected.

Tests (schema.yml + singular, all dbt-native):
- unique + not_null on all 9 keys; not_null on FKs, money, statuses, skus.
- accepted_values: order status ×2 (oltp + soap share the vocabulary),
  payments.status/method, customers.tier, supplier_code.
- relationships: orders→users (error), items→orders (warn, D3),
  payments→orders (warn, D3), promotions.product_sku→catalog via singular test
  over the rest∪file union (error; measured total).
- hash format (`^[0-9a-f]{64}$`) + cleartext-absence singular tests (D1).
- batch row parity: each batch staging model's count must equal its raw table's
  (1:1 key upserts; stable by construction). CDC parity is *evidenced*, not
  asserted — the live mutator makes strict raw↔staging count equality racy by
  design; VERIFIER records parity + drift attribution per build.
- envelope contract on sources: `op='d' ⇔ after IS NULL` across all four cdc
  tables (catches sink/Debezium contract breaks at the boundary).

## Consequences / known limits

- `make dbt-build` twice in a row: batch counts bit-stable; CDC counts drift by
  real mutator activity (inserts/deletes) — that drift is evidence the pipeline is
  live, not a defect; updates never change counts. VERIFIER diffs and attributes.- `stg_order_items` full rebuild is the build's long pole (~4.4M-row scan); fine
  at this scale, the named 100× lever is D2's deferred incremental.
- No indexes on staging tables (dbt CTAS); item 8 adds them only if marts need.
- Salt rotation invalidates all hashes at once (by design; SCD2 would see
  mass-"changes" — noted for the item 8 ADR).
