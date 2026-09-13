# ADR-011 — Great Expectations gates: version pin, dbt-vs-GE division of labor, quarantine & replay

Date: 2026-09-13 · Status: accepted · Phase: 4 (item 10)
Decides: the GE version pin; the dbt-vs-GE division of labor (the contract);
gate placement and execution shape at the seam ADR-010 D3 reserved; the
drift-tolerance policy for suites; the quarantine (dead-letter) schema and its
idempotent writes; replay semantics per feed class; the honest alerting story;
the GX data-context / credential mechanism.

## Context

Phase 3 closed with `dbt build` as the single DAG-level DQ gate (ADR-010 D3):
ONE invocation interleaving 144 error-severity tests into the resource DAG, so
staging tests gate marts and marts tests gate the published layer. ADR-010 D3
explicitly reserved the GE seam: "a dedicated nonzero-exit task downstream of
`dbt_build` (staging+marts suites), or between the ingest fan-in and
`dbt_build` for raw-side suites — one gate = one task = one nonzero exit."

Fact base this ADR relies on (all measured or verifiable; evidence in
EVIDENCE/phase-4-dq.md):

- **dbt already owns the structural layer.** Full inventory read of
  `dbt/project/models/staging/_staging_models.yml`, `.../marts/_marts_models.yml`
  and `dbt/project/tests/*.sql`: not_null/unique on every key; relationships
  (warn-severity on live-CDC edges, error in marts); accepted_values on every
  vocabulary (order status ×2, currency ×3, payment method/status, tier,
  supplier_code, source_type, source_feed); `values_match_regex ^[0-9a-f]{64}$`
  on every `*_hash` column; composite-grain uniqueness
  (`unique_combination(source_type, order_id)`); singular tests for same-run
  row parity (staging↔marts AND raw↔batch-staging), SCD2 window integrity,
  unknown-member discipline, PII absence, promo-sku coverage, CDC envelope
  contract.
- **The mutator churns OLTP continuously** (~2 s ticks: order status machine,
  user logins, new orders+items+payments, deletes of the oldest CANCELLED
  orders). Raw is CDC-live and never count-stable; **staging and marts are
  frozen tables** between builds (staging = TABLE full-refresh per build,
  ADR-008 D2; marts rebuilt per run).
- **A measured money-sign policy exists in live data** (probed 2026-09-13,
  `staging.stg_payments`, 719,145 rows): CAPTURED/PENDING amounts are strictly
  positive (min +34.51), REFUNDED amounts are strictly negative (max −235.82).
  dbt asserts only `not_null` on `amount` — the sign policy is a business rule
  no dbt test encodes.
- **GX version landscape (researched 2026-09-13):** PyPI latest =
  `great-expectations 1.23.0` (released 2026-09-10, requires py ≥3.10,<3.14);
  previous minor 1.22.0 is the current documented stable. The 0.18.x line is
  **archived and unmaintained** (last release 0.18.22; official docs state
  "no longer actively maintained"); GX Core 1.0 shipped Aug 2024 and 1.x has
  been the only supported line since, under Semantic Versioning with a formal
  deprecation policy; the repo now lives under fivetran/great_expectations and
  1.18+ removed the GX Cloud coupling. Both lines run on Python 3.11.
- **No notification channel exists in this stack** (no SMTP, no Slack) — and
  none may be faked.
- **Mimosa canon** (cumulative, Sessions 2–9): source files via Write/Edit
  only; SQL in Python = single-line static literals at call sites, fully
  parameterized; no credential-looking literals anywhere (env only); all
  filesystem writes through containment — and GX's classic data context is a
  filesystem animal (`great_expectations.yml`, `uncommitted/`, data docs).

## Decisions

### D1 — Version pin: `great-expectations==1.22.0` (exact), Python 3.11, telemetry off

Pin **1.22.0** — the latest *settled* minor (1.23.0 is 3 days old at session
time; same N-1 discipline as the dbt 1.9.x pin in ADR-008 D5). Exact `==` pin,
hermetic one-shot image: upstream churn cannot move a shipped gate.

- **Why not 0.18.x (classic):** it is EOL with no security fixes. Pinning an
  archived line as the project's *trust layer* is indefensible to an
  interviewer and worse in substance — DQ code that itself stops receiving
  fixes. "API stability" is preserved the way this stack already does it
  (dbt 1.9, Airflow 2.10.5): exact pin + baked image, not by choosing a dead
  version.
- **Why the 1.x API risk is contained:** the gate uses ~5 expectation types
  from 1.x core (`ExpectColumnValuesToBeBetween`,
  `ExpectColumnValuesToMatchRegex`, `ExpectColumnPairValuesAToBeGreaterThanB`,
  `ExpectColumnValuesToBeInSet`), an **ephemeral data context** (D9), and a
  Postgres SQL datasource. All expectations live as Python objects in ONE
  module (`dq/suites.py`); a future GX migration touches exactly that file.
- **Telemetry off** via env in the compose service: `GX_ANALYTICS_ENABLED=false`
  (no usage-stats egress; the tool also makes no HTTP calls at all, D3).

### D2 — Division of labor: dbt owns structure, GE owns business semantics + the row-level dead-letter loop

The design question is not "what can GE check" but "what does GE add that 144
dbt tests don't already enforce". The contract:

| Class | Owner | Concrete example (real, from this repo) |
|---|---|---|
| Key structure: nulls, uniqueness, grain | dbt | `not_null`/`unique` on `stg_orders.order_id`; `unique_combination(source_type, order_id)` on `fct_orders` |
| Referential integrity | dbt | `relationships stg_orders.user_id → stg_users`; singular `fct_items_order_integrity` (composite-grain join) |
| Vocabulary / enums | dbt | `accepted_values` on order status, payment method/status, tier, source_type |
| Format of derived security columns | dbt | `values_match_regex ^[0-9a-f]{64}$` on every `*_hash` |
| Same-run row parity staging↔marts | dbt | singular `marts_row_parity`, `batch_row_parity` |
| SCD2 structural invariants | dbt | `scd2_current_uniqueness`, `scd2_window_integrity` |
| PII absence | dbt | singular `pii_absence_staging` / `pii_absence_marts` |
| **Business semantics (money, time, format of business attributes)** | **GE** | captured/pending payments positive & refunds negative (the measured sign policy); `signup_date` never in the future; `country_code` ISO-3166 alpha-2 *shape*; `updated_at ≥ created_at` on SOAP orders |
| **Published-layer plausibility** | **GE** | `fct_orders.total_amount ≥ 0` (both sources, the mart as published) |
| **Identify the offending ROWS + dead-letter with provenance** | **GE only** | `dq.dq_quarantine`: dbt tests say "147 rows failed"; GE hands over *which* rows, as data, with suite/expectation/pk/payload/run_id provenance |
| **Post-build gate + resolution/replay loop** | **GE only** | `dq_gate` task (nonzero exit) + `make dq-replay` resolution — dbt tests have no dead-letter path and no resolution concept |

Hard rules: (1) GE **never** re-asserts a dbt-owned class — no not_null/unique/
enum/hash-regex/parity/SCD2 duplicates (CRITIC-probed by diffing the suite set
against the dbt inventory); in particular the mandate's suggested "relational
parity staging↔marts" is **already dbt-owned** (`marts_row_parity`) and is
deliberately NOT in a GE suite. (2) "SCD2 invariants in GE" only where GE adds
a capability dbt lacks — today it doesn't (the snapshot strategy plus dbt's two
SCD2 singular tests already gate them, and an SCD2 violation is a build bug,
not a row you'd dead-letter), so GE carries **no** SCD2 suite; recorded as an
explicit exclusion, revisitable if a use case appears where quarantining
offending *rows* beats failing the *build*.

### D3 — Placement & execution shape: ONE `dq_gate` task downstream of `dbt_build` (the reserved seam, option A)

`daily_close`: ingest_file → (ingest_soap ∥ ingest_rest) → **dbt_build** →
**dq_gate**. `dq_gate` is a BashOperator exactly like its siblings:

    cd $HELIOS_PROJECT_DIR && docker compose build dq && docker compose run --rm dq gate

— the ADR-010 D1 invocation mechanism verbatim (same compose, same rootless
socket, same one-shot container make runs), with the dbt-image convention
(ADR-008 D5): build-then-run so a code edit can never be missed by a run
(cached no-op when unchanged). One gate = one task = one nonzero exit: the
gate runs every suite, dead-letters every offending row, then exits nonzero if
anything failed — the DAG goes red **at the gate**.

Why option A (post-build, staging+marts) and not option B (raw-side, pre-build):
1. **Drift:** raw is CDC-live; raw-side suites flake by construction (D4).
2. **PII:** raw keeps cleartext (ADR-008 D1); a raw-side dead-letter would ship
   cleartext PII into a new table. Staging/marts rows are hashed — the
   dead-letter inherits that guarantee.
3. **Subject:** dbt already validates the *movement*; GE's highest-signal
   surface is the *typed, frozen* staging/marts output the warehouse publishes.

Honest alternatives considered and rejected:
- **GE inside dbt (dbt-expectations /GX dbt package):** entangles two tools'
  lifecycles and still has no dead-letter/replay path; dbt tests are pass/fail,
  not row-catching.
- **Airflow GX provider / PythonOperator in the scheduler process:** puts GE in
  the scheduler's Python (dependency bleed into the airflow image) and breaks
  the one-shot-tool symmetry (ADR-010 D1) — every other pipeline unit is the
  same container make runs.
- **Long-running GX service:** one nightly gate does not need a resident
  process; this stack runs no services it cannot justify.

No new long-running service; the `dq` image speaks **no HTTP** (D9: it talks to
the warehouse over Postgres only).

### D4 — Drift-tolerance policy: suites attach only to frozen relations; time-relative rules are computed at suite construction

1. Every suite reads **staging/marts relations frozen by the `dbt_build` task
   in the same DAG run** — never `raw.*`, never OLTP. The CDC sink keeps
   writing raw between build and gate; that write surface is invisible to the
   gate, so a gate re-run (Airflow retry) re-evaluates the *same bytes* and
   returns the same verdict.
2. **No absolute count baselines against anything CDC-live.** No suite
   compares raw↔staging counts (the `batch_row_parity` singular test documents
   why: "live mutator makes strict raw↔staging equality racy by construction"),
   no cross-run deltas, no prior-day counts. The gate has no memory of
   yesterday's numbers, so it cannot flake on drift.
3. **Coverage assertions excluded by design:** items→catalog coverage is
   6.89 % *by seed design* (ADR-009); any "enough coverage" rule would encode a
   generator constant, not a business invariant.
4. The only time-dependent rule (future `signup_date`) compares against
   **"today" computed at suite construction** — a future-dated signup is wrong
   on every run regardless of churn; the rule cannot go stale in the other
   direction (nothing asserts "data must be recent", which WOULD flake).
5. Determinism is proven, not assumed: VERIFIER runs the full gate twice
   against the live, churning stack and requires identical verdicts.

A suite that fails intermittently is a suite an operator learns to ignore —
under this policy the only way a suite fires is a real semantic violation.

### D5 — Suite set: 6 suites, 10 expectations, every bound justified

| Suite (asset) | Expectation | Encodes | Bounds provenance |
|---|---|---|---|
| `stg_payments.money_sanity` (staging.stg_payments) | amount ≥ 0.01 WHERE status ∈ {CAPTURED, PENDING} | captured money is positive | live policy probe 2026-09-13: min +34.51 across 628,222 rows |
| 〃 | amount ≤ −0.01 WHERE status = REFUNDED | refunds carry the negated amount (ADR-008 model doc) | live: max −235.82 across 91,923 rows |
| `stg_orders.order_sanity` (staging.stg_orders) | total_amount ≥ 0 | an order total is never negative | seed derives totals from Σ(qty·price), all positive |
| `stg_order_items.line_sanity` (staging.stg_order_items) | quantity ∈ [1, 10000] | a line item is ≥1 unit; >10k units is implausible for this catalog | floor is semantic (seed 1–5); ceiling deliberately loose so the gate doesn't echo the generator |
| 〃 | unit_price ≥ 0 | no negative unit prices | seed range 1.99–151.97 |
| `stg_file_customers.feed_sanity` (staging.stg_file_customers) | signup_date ≤ today (suite-construction time) | a customer cannot sign up in the future | temporal, D4.4; **the poison drill's target** |
| 〃 | country_code matches `^[A-Z]{2}$` | ISO-3166 alpha-2 *shape* (dbt has only not_null; seed alphabet is DE/GB/US/FR/NL/PL/SE/ES/JP/BR) | seed lists + file generator share the alphabet |
| `stg_soap_orders.temporal_coherence` (staging.stg_soap_orders) | updated_at ≥ created_at | an update cannot precede creation | SOAP UpdateOrderStatus semantics |
| `fct_orders.published_money_sanity` (marts.fct_orders) | total_amount ≥ 0 | the published fact, both sources, is non-negative | staging rule + SOAP totals |

Conditional expectations use GX row_conditions (compiled by GX to SQL WHERE
clauses — no SQL authored by us); `today` is a Python value injected at suite
build. All rows in the dead-letter are staging/marts rows — hashed PII only (D6).

Suite-level policy: every expectation is error-severity; one failing
expectation fails the gate (nonzero exit) after ALL suites have run and all
offending rows have been dead-lettered — the gate reports completely, then
blocks. Quarantine writes are capped per expectation (`DQ_QUARANTINE_MAX_ROWS`,
default 10 000, see D6) so a catastrophic violation cannot explode the
dead-letter; truncation is recorded in the row's failure_reason.

### D6 — Quarantine schema: `dq.dq_quarantine` in the warehouse, append-only incidents with resolution columns

Schema `dq` (documented in DATA_DICTIONARY alongside raw/staging/marts):

```sql
CREATE SCHEMA IF NOT EXISTS dq;
CREATE TABLE IF NOT EXISTS dq.dq_quarantine (
    quarantine_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    suite           TEXT        NOT NULL,  -- e.g. stg_payments.money_sanity
    data_asset      TEXT        NOT NULL,  -- e.g. staging.stg_payments
    expectation     TEXT        NOT NULL,  -- GX type, e.g. ExpectColumnValuesToBeBetween
    check_digest    TEXT        NOT NULL,  -- sha256 of type+column+row_condition+kwargs
    column_name     TEXT,                  -- NULL for pair/table expectations
    row_condition   TEXT,                  -- the conditional clause, for audit
    source_pk       JSONB       NOT NULL,  -- {payment_id: 12345}
    payload         JSONB       NOT NULL,  -- the offending row, hashed PII only
    failure_reason  TEXT        NOT NULL,  -- human-readable observed-vs-expected
    run_id          TEXT        NOT NULL,  -- dq run that (re)detected it
    status          TEXT        NOT NULL DEFAULT 'open',  -- open | resolved
    landed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ,           -- set by make dq-replay
    resolved_run_id TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dq_quarantine_open
    ON dq.dq_quarantine (data_asset, check_digest, source_pk_hash)
    WHERE status = 'open';
```

(`source_pk_hash` = deterministic digest of the source_pk JSON, generated in
Python, so the partial unique index can dedupe without a JSONB expression
index; `check_digest` = digest of the full expectation identity — type,
column, row_condition, kwargs — so two distinct rules failing the same row
open two distinct incidents.)

- **Idempotent writes:** re-detecting the same violation upserts the open
  incident (`ON CONFLICT … DO UPDATE SET run_id, landed_at` — refreshes
  last-seen; `rowcount` distinguishes new vs refreshed). Crash-replay writes
  nothing twice. Resolved incidents are immutable history; a re-poisoned row
  opens a NEW incident (partial index only covers `open`).
- **PII:** dead-letter rows are staging/marts projections — `*_hash` columns
  only, no cleartext (dbt's `pii_absence_*` singular tests define that
  guarantee; the DQ gate's suites read the same relations).
- **Cap:** at most `DQ_QUARANTINE_MAX_ROWS` rows dead-lettered per failing
  expectation (env knob, default 10 000); `failure_reason` records
  `"... +N more beyond cap"` so the count is honest.
- The DDL runs at every gate start (`CREATE SCHEMA/TABLE IF NOT EXISTS` — the
  `ingest/ensure.py` idempotent-DDL pattern); the gate creates nothing else.
- **`dq` speaks SQL only over psycopg with single-line static literals at call
  sites** (ingest precedent); GX generates its own validation SQL internally —
  we author no SQL for validation.

### D7 — Replay: `make dq-replay` re-validates open incidents against the current build and resolves what now passes

"Fixed source" per feed class (the fix always happens at the source; replay
never edits warehouse data by hand):

| Feed class | What "fixed source" means | Mechanism |
|---|---|---|
| File batch | regenerate/correct the CSV — **same filename, new content** → new hash → the ingest hash-ledger re-lands it (ADR-006 D3 fix-and-reland) | corrected file in the drop volume |
| Batch REST | source-side correction, then the normal full-walk refresh re-reads and content-hash-upserts | `make ingest-rest` / next daily_close |
| CDC (OLTP) | a corrected row **upserts through the normal CDC path** (new LSN, same pk) | `UPDATE` on oltp → Debezium → raw.cdc_* → next build |
| SOAP | corrected orders replay via the documented `--full` epoch→now pull | `make backfill` semantics (ADR-010 D5) |

`make dq-replay` (and the in-DAG gate's replay subcommand,
`docker compose run --rm dq replay`):
1. re-runs the full gate against the **current** frozen staging/marts (i.e.
   after a rebuild that absorbed the source fix);
2. for every `open` incident whose (data_asset, check_digest, source_pk) no
   longer appears in that run's failures — or whose row no longer exists —
   sets `status='resolved'`, `resolved_at=now()`, `resolved_run_id`;
3. prints a structured JSON report (`resolved: N, remaining: M`) and exits
   **nonzero if any incident remains open** (an operator tool that can't
   silently half-succeed), 0 when the quarantine is fully resolved.

"Quarantine empty" (the DoD demo's end state) = zero `open` incidents.
Resolved history is never deleted — it is the audit trail of what the gate
caught and when it cleared.

### D8 — Alerting honesty: the alert IS the gate

No SMTP/Slack exists in this stack and none is faked. The gate failure surfaces
as, exactly: (1) the `dq_gate` task failing in Airflow after retries — red in
the UI and in `make run-etl`'s nonzero exit; (2) a structured JSON log event
per run (`event=dq.gate_failed|dq.gate_passed` with per-suite counts — the
same stdout-JSON convention as ingest, grep-able via `make airflow-logs` /
`make dq-run` output); (3) the `dq.dq_quarantine` table itself (queryable,
SQL-first). Real push-alerting arrives with item 12 (Prometheus alerting
rules); this ADR records that dependency honestly.

### D9 — GX data context: ephemeral, env-injected, zero filesystem writes

- **Ephemeral data context** built in Python at run time (`gx.get_context(mode="ephemeral")`
  in 1.x) with a Postgres SQL datasource whose connection string is assembled
  **only** from `WAREHOUSE_POSTGRES_USER/PASSWORD/DB` env vars (host
  `warehouse-db`, port 5432 — in-compose topology defaults, same policy as the
  dbt profile). No `great_expectations.yml`, no `uncommitted/`, no data-docs
  site, no checkpoints-on-disk: **the dq tool performs zero filesystem
  writes** — validation results live in the warehouse (`dq.dq_quarantine`) and
  stdout JSON. This dissolves the Mimosa containment concern by construction
  (nothing to contain) and keeps credentials out of any config file.
- **Telemetry:** `GX_ANALYTICS_ENABLED=false` (D1).
- Validation requests use `result_format=COMPLETE` with
  `include_unexpected_rows=True` and explicit `unexpected_index_column_names`
  (the asset's pk), which is what turns a failed expectation into dead-letter
  rows (D6).

## Consequences

**Positive.** The trust layer is a real gate: a semantically-broken row that
passes 144 structural dbt tests is caught, *individually identified*, parked
with full provenance, and blocks the close; the fix-and-replay loop is
operator-executable (`make dq-replay`) and auditable (resolved history
retained). The dbt/GE boundary is a written contract with a zero-duplication
guarantee. The gate is drift-proof by construction (frozen relations only) and
deterministic (proven ×2). The stack gains exactly one task and one image — no
new service, no HTTP, no credentials outside env, no filesystem writes.

**Negative / costs.** +1 one-shot image to build (`helios/dq`); the gate adds
its suite-runtime to every close (measured, see evidence; budgeted in
`dq_gate.execution_timeout=15 min` vs ~10 s warm suites). GX is a heavyweight
dependency (~100+ transitive packages) inside a pinned image — accepted, as
with dbt. Conditional/pair expectations are GX-API-coupled; a GX major bump
touches `dq/suites.py` (isolated by design, D1).

**Residual risks (honest).** (1) The gate covers *semantic* rules we chose;
it is not a schema-drift detector (dbt's contract tests + Phase 5 chaos cover
that axis). (2) REFUNDED-positive is enforced on `stg_payments` only via the
status conditional; a new payment status value would silently fall outside
both conditionals — accepted: dbt's accepted_values on status fails first and
louder. (3) The 10k-row cap means a catastrophic violation dead-letters a
sample, not the full population — by design (the gate's job is to block, the
cap's job is to survive the block). (4) GX 1.x minor churn: pin + isolated
suite module; upgrades are a deliberate image rebuild with the ×2 determinism
proof re-run. (5) The nightly 05:00 UTC schedule fires during development —
gate runs interleave with drills; all drills attribute via `make cdc-status`
first (Session-8 protocol).
