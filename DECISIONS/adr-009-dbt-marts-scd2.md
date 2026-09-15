# ADR-009 — dbt marts: star schema, SCD2 customer, SOAP fact scope

Date: 2026-09-11 · Status: accepted · Phase: 3 (item 8)
Decides: the `marts` layer on top of staging (ADR-008) — star schema shape; the
`dim_customer` SCD2 mechanism and its change-detection contract; the
fact→dimension point-in-time join; whether SOAP orders join `fct_orders` and
whether `dim_customer` unifies identities; `dim_product` versioning; `dim_date`
span; materialization and idempotency; PII at the mart boundary.

## Context

Staging (ADR-008) ships 9 typed tables in schema `staging`, rebuilt full-refresh
per `make dbt-build` (~33 s; items ~4.34 M rows the long pole). CDC staging reads
current state of continuously churned raw (mutator ticks every ~2 s); batch
staging is stable between ingest runs. The warehouse has an empty `marts` schema
since Phase-0 init. Marts run after staging in the same `dbt build` (Airflow
serializes staging → marts in item 9).

Measured fact base this ADR relies on (all re-measurable; see
EVIDENCE/phase-3-marts.md; counts drift with the mutator — attribute via
`make cdc-status` first, as in ADR-008):

- **Mutator column truth** (`oltp/oltp/mutate.py`, load-bearing): the only users
  UPDATE is `SET last_login_at = now()` for ~40 users/tick. The mutator never
  touches `email / full_name / country_code / is_active`, never inserts or
  deletes users (the single raw tombstone is the cdc-verify marker). It inserts
  1–3 orders/tick (with items + payments) and deletes the oldest CANCELLED
  orders — so FACT counts drift, USER counts do not.
- **Identity landscapes**: OLTP `user_id` 1..50,000 (50,000 rows); SOAP
  `customer_id` 1..5,000 (5,000 distinct across 382,179 orders); file
  `customer_id` 1..5,000. `stg_users`⋈`stg_file_customers` on `email_hash`:
  **7 equal pairs**. The SOAP store has **no customers table** — the seeder drew
  `customer_id = randint(1, 5000)` independently of the file-drop generator
  (`file-drop/filedrop/generate.py` draws its own 1..5,000). SOAP payload keys
  are exactly `order_id, customer_id, status, total_amount, currency,
  created_at, updated_at` — **no identity attribute exists on a SOAP order**.
  The numeric coincidence of the three id ranges is seed arithmetic, not
  identity.
- **Product landscape**: catalog = `stg_rest_products` ∪ `stg_file_products` on
  the shared `SKU-#####` space: union **6,895** SKUs, overlap **105**; on the
  overlap, prices agree **105/105**, names agree **0/105** (independent naming
  generators, identical price formula). `fct_order_items` references **100,000**
  distinct SKUs; the catalog covers 6,895 of them, which is **6.89 % of item
  ROWS (299,638 / 4,347,003)** — partial *by seed design* (ADR-008 D6).
- **Order timelines**: OLTP `placed_at` 2023-09-10 → now (mutator keeps pushing
  the max); SOAP `created_at` 2023-09-09 → 2026-09-09. ~3 years of history.
- **User attributes**: 10 country codes (~5 k each); `is_active`: 47,493 true /
  2,507 false — real attribute cardinality, worth versioning.
- Id collision warning: OLTP `order_id` (live range ≈ 1..560 k) and SOAP
  `order_id` (1..382,179) are **independent namespaces that overlap numerically**.

## Decisions

### D1 — `dim_customer` SCD2 mechanism: **dbt snapshot, check strategy**, on a documented attribute subset

Snapshot table `snapshots.customers_snapshot` (snapshot nodes run inside the
ordinary `dbt build` — no extra dbt step, nothing new for item 9):

```
unique_key = user_id
strategy   = check
check_cols = [email_hash, full_name_hash, country_code, is_active]
invalidate_hard_deletes = true
```

- **Change detection is defined ONLY over the identity subset**
  `email_hash, full_name_hash, country_code, is_active` — the business attributes
  whose history matters. `last_login_at` is **deliberately excluded**: the
  mutator updates ~40 users every ~2 s, so including it would fabricate ~1.7 M
  versions/day of pure noise. `created_at` is immutable; `_cdc_lsn` is
  provenance. Deterministic salted hashes (ADR-008 D1) make the check-hash
  stable across builds — hash-based detection is exact, not heuristic.
- `is_active` is **included**: it is a real customer-lifecycle attribute
  (measured 47,493/2,507 split) whose flip deserves history; the mutator never
  touches it, so it costs nothing in churn.
- `invalidate_hard_deletes = true`: a vanished user closes its window instead of
  silently staying "current". Users are mutator-neutral today, so this is
  correctness insurance, not a live path.
- **Why not a custom incremental merge**: it would reimplement the snapshot's
  hash-compare + merge mechanics in project SQL (more surface, harder to prove
  correct, no capability we need). dbt-native snapshots are auditable,
  battle-tested, and give `dbt_valid_from/to` + `dbt_scd_id` for free. This is a
  portfolio — the idiomatic mechanism wins.
- **Snapshot history starts at first run** (2026-09-11). Orders placed before
  that cannot know the customer's as-of state; they resolve to the **earliest
  known version** (D2 fallback). Documented limitation, not hidden — day one the
  fallback path serves ~100 % of OLTP orders and the point-in-time window share
  grows as history accumulates.
- **History protection**: `make dbt-build` stays `dbt build` with **no
  `--full-refresh`** (a full refresh on a snapshot drops and re-creates it from
  current state — instant history wipe). This is now a standing rule: never wire
  full-refresh into the marts build target. `snapshots` is persistent state; the
  only sanctioned wipe is `make clean`. RUNBOOK carries the warning.
- Salt rotation (inherited ADR-008 caveat, now operational): rotating
  `PII_HASH_SALT` mass-invalidates every hash → the snapshot would record ~50 k
  "changes" in one build. Treat salt rotation as a destructive, planned event
  (rebuild snapshot from scratch consciously).

`dim_customer` is a **table rebuilt every build from the snapshot** (≤ ~50 k+
rows — trivial): surrogate key `customer_sk = dbt_scd_id`, natural key
`customer_id`, hashed attributes, half-open validity window
`[valid_from, valid_to)` (`valid_to IS NULL ⇔ is_current`). Rebuilding the
projection each run re-derives all invariants from protected state — the
snapshot is the only persistent node in the project.

### D2 — Point-in-time fact→dim join (the SCD2 contract)

`fct_orders` carries the customer **natural key** (`customer_id`) and resolves
the surrogate key whose validity window contains the order timestamp:

```
customer versions (dim_customer)          orders (fct_orders, OLTP only)
─ v1 [t₀ ──────── t₁)                     ● placed_at ∈ [t₀,t₁) → v1.sk
─ v2 [t₁ ──────── t₂)                     ● placed_at ∈ [t₁,t₂) → v2.sk
─ v3 [t₂ ──────── ∞ )  is_current         ● placed_at ≥ t₂      → v3.sk
─ pre-history: placed_at < t₀ → fallback → v1.sk (earliest known version)
```

Implementation: LEFT JOIN on `customer_id AND order_ts >= valid_from AND
(valid_to IS NULL OR order_ts < valid_to)`, then COALESCE with a second LEFT
JOIN to each customer's **earliest version** (`DISTINCT ON (customer_id) …
ORDER BY valid_from`). Every OLTP order's user has ≥ 1 snapshot version (users
are never deleted and the snapshot seeds all 50 k), so resolution is total; a
singular test enforces it (`customer_sk IS NULL` for OLTP rows = failure).
Half-open intervals `[from, to)` guarantee exactly one match inside the window.

### D3 — Fact scope: **one `fct_orders`, `source_type` discriminator, SOAP customer = Kimball unknown member**

Integrated: `fct_orders = stg_orders (source_type='oltp') ∪ stg_soap_orders
(source_type='soap')` — conformed status vocabulary (tested equal since ADR-008),
both money numeric(12,2) USD, both timelines typed timestamptz. Grain is the
**composite** `(source_type, order_id)` — the numeric id namespaces collide
(measured overlap), so a new package-free generic test `unique_combination`
enforces it.

**`dim_customer` does NOT unify identities.** Measured: SOAP orders carry no
identity attribute at all; SOAP's `customer_id` comes from an independent
seeded `randint(1,5000)` with no customer table behind it; file customers are a
second independent 1..5,000 draw; the only measurable cross-feed identity link
anywhere is 7 `email_hash` pairs (OLTP↔file). A union dim would either assert
SOAP `customer_id n` ≡ file `customer_id n` (pure fiction — zero supporting
data) or join OLTP users by id collision (worse). **The honest star keeps
`dim_customer` OLTP-only (SCD2 per D1) and marks SOAP customers with the
Kimball unknown member**: `customer_sk IS NULL` exactly when
`source_type='soap'` (tested in both directions). SOAP customer analytics is a
future business decision (identity-resolution workstream), not something to
smuggle in via an unmeasured join.

- Rejected: separate `fct_soap_orders` — duplicates the conformed schema, and
  cross-source order analytics would require a UNION anyway; "legacy SOAP +
  modern OLTP in one governed star" is exactly the ESB-modernization story the
  portfolio claims.
- Rejected: unified `dim_customer` — unsound on the measured evidence above.
- `fct_order_items` is OLTP-only by construction (SOAP has no items feed);
  its `order_id` refers to `fct_orders` rows **with `source_type='oltp'`** — the
  id collision makes a bare id join against `fct_orders` WRONG (a straddled item
  could false-match a SOAP order id); the orphan test joins on the composite.

### D4 — `dim_product`: **SCD1**, natural key `sku`, REST precedence

- SCD1 is defensible and chosen: the only measurable cross-feed variance is
  naming; prices are formula-stable and agree 105/105 on the overlap. No
  versioning payoff; SCD2 here would be ceremony.
- Precedence on the 105-SKU overlap: **REST wins** (fresher cadence, carries
  `currency`, API-of-record); file-only columns (`supplier_code`) are NULL on
  REST rows; `source_feed` column documents provenance ('rest' | 'file').
- **No surrogate key**: SCD1 means the natural key *is* the stable key; a
  surrogate would add a join and zero information.
- `fct_order_items.product_sku` joins `dim_product` **LEFT** — the catalog
  covers 6,895 of 100,000 item SKUs ≈ **6.89 % of item rows by seed design**.
  Unmatched rows keep `NULL` product attributes; row parity is tested exactly
  (below), so the partial join can never silently drop rows. The unmatched share
  is a documented constant of the seed, not a defect.

### D5 — `dim_date`: generated, day grain, spanning the fact range

`generate_series(min→max)` over the two order timelines in staging (OLTP
`placed_at`, SOAP `created_at`, UTC day grain). `date_key = YYYYMMDD int`
(classic), plus year/quarter/month/month_name/day/day_of_week/is_weekend. Built
from staging (not from facts) so the DAG stays `staging → dims → facts`; the
coverage test proves every fact `order_date_key` exists in `dim_date`.

### D6 — Materialization, idempotency, and the test spine

- All five marts are **tables, full-refresh per build** (ADR-008 D2 logic
  applies verbatim: hundreds of MB, minutes; **incremental facts are the
  documented 100× lever, deferred**). `snapshots.customers_snapshot` is the only
  persistent state in the project.
- Idempotency contract: a normal `make dbt-build` must never change `dim_customer`
  history (snapshot detects no changes → zero new versions) and must produce
  stable fact counts under a quiesced upstream; count drift under live CDC is
  real churn, attributed via `make cdc-status` (ADR-008 protocol).
- Tests (all dbt-native schema tests + package-free generic/singular SQL):
  - *Referential integrity*: `fct_orders.customer_sk → dim_customer` (built-in
    relationships ignores NULLs — the unknown member rides through; error);
    `order_date_key → dim_date` (error); **singular** orphan test for items →
    orders on the composite `(source_type='oltp', order_id)` (error — the mart
    publishes its contract; a µs-window CDC straddle fails the build loudly and
    Airflow retries in item 9, which is the governed behavior, unlike staging's
    warn; ADR-008 D3 semantics do not carry into the published layer).
  - *SCD2 invariants*: exactly one `is_current` per `customer_id`; windows
    half-open, non-overlapping, contiguous (`prev.valid_to = next.valid_from`
    when a next exists; `valid_to > valid_from` always); `customer_sk` unique +
    not_null; current-row count per user = 1 ⇔ `stg_users` membership.
  - *Row-count deltas* (deterministic — tests read built relations in-run):
    `fct_orders` = `stg_orders` + `stg_soap_orders` exactly;
    `fct_order_items` = `stg_order_items` exactly; `dim_product` = catalog
    union exactly; `dim_customer` current customers = `stg_users` exactly.
  - *Scope discipline*: `customer_sk IS NULL ⇔ source_type='soap'` (both
    directions).
  - *PII*: every mart (and the snapshot) receives `*_hash` columns only; a
    singular cleartext-pattern sweep (`@`/space detectors + hex-format tests)
    covers all mart hash columns; VERIFIER proves absence via
    information_schema + content queries (rubric §6.6).

### D7 — PII at the mart boundary

Inherited contract, now enforced one layer further out: marts and the snapshot
project hashed columns only; no cleartext PII column may exist in `marts` or
`snapshots` schemas (proven by query in EVIDENCE). The snapshot stores hashes
plus dbt metadata (`dbt_scd_id`, `dbt_valid_from/to`, `dbt_updated_at`) — no new
PII surface.

## Consequences / known limits

- SCD2 history begins 2026-09-11: pre-snapshot orders resolve to the earliest
  known version by documented fallback (D1/D2). Point-in-time correctness
  improves as the snapshot ages; there is no backfill path (the past was never
  captured) — honest limitation, not hidden.
- A normal build never resets history; a `--full-refresh`-style wipe of
  `snapshots.customers_snapshot` would (protected by convention + RUNBOOK; dbt
  build without the flag never touches it).
- `fct_order_items` → `dim_product` coverage is 6.89 % of rows by seed design;
  rising coverage would indicate catalog growth, falling coverage a feed
  regression — EVIDENCE tracks it per build.
- Fact counts drift with live CDC between builds (attributed via
  `make cdc-status`); under quiesced upstream, reruns are count-stable.
- SOAP customer identity remains unresolved by design (D3) — the unknown member
  is the honest placeholder until a business-registry decision exists.
- `make dbt-build` unchanged as the one pipeline unit (snapshot runs inside
  `dbt build`); item 9 serializes ingest → staging → marts with no new dbt
  step.
