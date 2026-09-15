# ADR-007 — Raw-zone layout for batch loads: per-source natural-key tables with content-hash-guarded upserts

Status: accepted (2026-09-11, Phase 2, Session 5)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-005 (CDC raw layout), ADR-006 (watermarks), BACKLOG item 6

## Context

Batch extractors (SOAP / file / REST) need landing tables in the warehouse `raw`
schema. The raw zone already hosts CDC landing tables `raw.cdc_*` (ADR-005: pk JSONB,
lsn guard, append-as-upsert). Batch loads have different semantics: they carry a
natural key, no WAL ordering column, and a per-run identity (`load_id`) plus a
per-batch identity (`batch_ref`). The layout must not collide with `raw.cdc_*`, must
make re-landing identical content a true no-op, and must give rejected rows an
inspectable quarantine surface. The warehouse volume predates this phase, so DDL
arrives via an idempotent `ensure()` — never a volume rebuild.

## Options considered

**Landing shape**
1. Append-only event log (one row per source row per run) + dedupe views — honest
   history, but every re-run grows the table and "no dupes" becomes a query-time
   property. For sources with no change feed, the history value is low and the
   storage cost is per-run-linear.
2. CDC-style pk-JSONB tables with an lsn guard — wrong guard: there is no lsn.
3. Per-source **natural-key tables** with a uniform envelope and a
   **content-hash-guarded upsert** (`ON CONFLICT (pk) DO UPDATE ... WHERE
   table.content_hash IS DISTINCT FROM EXCLUDED.content_hash`).

Decision: **3**. Identical source content re-lands as a physical no-op (not even a
`landed_at` bump — the guard skips the update entirely), while genuinely changed
content refreshes the row and stamps the new `load_id`. `cur.rowcount` cleanly
separates `rows_landed` (inserted/updated) from `rows_unchanged` (hash-equal),
which is exactly what the evidence runs report.

**Envelope columns** (uniform across all five tables):

```
<pk column>    natural PK of the source (order_id BIGINT, customer_id BIGINT,
               sku TEXT, id BIGINT) — plain column, PRIMARY KEY
load_id        UUID   — per RUN (one ingest-soap / ingest-file / ingest-rest run)
batch_ref      TEXT   — per BATCH: file name, SOAP window, REST endpoint walk
content_hash   TEXT   — sha256 of the row's canonical JSON (sort_keys, tight
                       separators) — the idempotency token
payload        JSONB  — the row verbatim in source terms (strings stay strings;
                       typing is staging's job, Phase 3)
landed_at      TIMESTAMPTZ DEFAULT now() — last *effective* landing (unchanged
                       re-lands do not bump it)
```

**Tables** (names chosen to never collide with `raw.cdc_*`): `raw.soap_orders`,
`raw.file_customers`, `raw.file_products`, `raw.rest_products`,
`raw.rest_promotions`.

**Quarantine surface**
1. `.rejects` files next to the drop — pollutes the source-managed volume and is
   un-queryable.
2. A `raw.ingest_quarantine` table.

Decision: **2** — queryable, assertable in tests, and consistent with Phase 4's
dead-letter-table story. Columns: `(quarantine_id, source, batch_ref, filename,
row_number, reason, raw_content, load_id, quarantined_at)` with `UNIQUE (source,
batch_ref, row_number, reason)` so a crash-replay re-quarantines nothing. Rejects
are per-ROW with the physical row number (maps 1:1 back to the file) and a stable
reason code (`ragged_width`, `encoding_not_utf8`, `unknown_header`); clean rows in
the same file still land — quarantine never poisons the batch.

**Control tables** (the ADR-006 state, plus run/file ledgers):

```
raw.ingest_watermarks (source PK, watermark_value, watermark_kind, updated_at)
raw.ingest_loads     (load_id PK, source, batch_ref, status running|succeeded|failed,
                      rows_read/landed/unchanged/quarantined, units_done/units_total,
                      error, started_at, finished_at)
raw.ingest_files     (filename PK, file_hash, size_bytes, load_id,
                      rows_read/landed/quarantined, landed_at)
```

`raw.ingest_loads` is updated *inside* each batch's landing transaction, so a
SIGKILLed run leaves a forensic `status='running'` row with the counters as of the
last commit — the crash probe's paper trail. The watermark advance itself follows
ADR-006: its own transaction, only after the landing commit.

**DDL delivery** — `ingest/ensure.py` is the source of truth (runs at the start of
every ingest invocation, `CREATE TABLE IF NOT EXISTS`), mirrored verbatim in
`infra/warehouse/init/03-ingest.sql` for `make clean && make up` parity — the
ADR-005 precedent (its 02-cdc.sql ↔ cdc/ensure.py).

## Consequences

+ Idempotency is a *physical* property (hash-guarded upsert): replays, overlap
  windows, OFFSET-shift double reads and crash-replays all converge with zero
  duplicates and zero wasted writes for unchanged rows.
+ Per-source tables keep natural keys typed and queryable; Phase 3 staging reads
  `payload` JSONB the same way it reads CDC `after` envelopes.
+ Quarantine and run/file ledgers make every rejection and every run inspectable
  from SQL (`make ingest-status`), not from grep.
− No batch history: the table holds latest-landed state per key (CDC tables hold
  the change history for the OLTP source). For sources without change feeds this
  is the honest trade; re-deriving "what changed between runs" is possible from
  `content_hash` deltas if ever needed.
− Same-key-different-content rows within one batch coalesce client-side (Postgres
  refuses ON CONFLICT touching one row twice); last-in-batch wins and the coalesced
  count is logged — deterministic because batch order is deterministic.
