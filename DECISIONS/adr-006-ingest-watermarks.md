# ADR-006 — Ingest watermarks: per-source incremental cursors, advanced only after landing commits

Status: accepted (2026-09-11, Phase 2, Session 5)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-001 (SOAP contract), ADR-003 (rest-mock), ADR-004 (file-drop), ADR-007 (raw-zone layout), BACKLOG item 6

## Context

The ingest lib extracts incrementally from three Phase-1 sources with *different*
change-detection capabilities:

- **soap-service**: `GetOrders` filters on `created_at` only (`date_from/date_to`),
  pages with 1-based `page`/`page_size` (OFFSET under the hood, max 500). It cannot
  filter on `updated_at`, and it has no per-row change feed. Status transitions do
  bump `updated_at`, but the API offers no way to *ask* for rows by it.
- **rest-mock**: a static catalog with opaque-cursor keyset pagination. There is no
  time column and no change feed at all — every "change" is a full re-read.
- **file-drop**: nightly CSVs. Row content has no change timestamp (`signup_date` is
  an event date, not a mutation time), and the filename date is transport metadata —
  Phase 1's `--late-offset` exists precisely to prove that a *backdated* file arrives
  late and must still land.

The DoD: crash-replay leaves no duplicates; re-landing identical source content is a
no-op. Watermark state must therefore compose with idempotent landing (ADR-007) —
the watermark bounds *work*, and idempotency makes the bounded work safe to repeat.

## Options considered

**Where watermark state lives**
1. A file/state blob in the ingest container — dies with the container; two runners
   race invisibly.
2. The source systems themselves — not ours to write to (and the SOAP one is a
   legacy system; that's the point).
3. One small control table in the warehouse raw schema, keyed by source.

Decision: **3** — `raw.ingest_watermarks(source PK, watermark_value, watermark_kind,
updated_at)`. It transactionally co-locates with the landing zone, `make
ingest-status` can join it against landed counts, and a fresh runner has zero state
of its own.

**Cursor semantics per source**
1. One generic "high-water timestamp" for all sources — dishonest: none of the three
   sources shares a comparable notion of "newer".
2. Per-source cursor *kind*, each chosen against what the source can actually
   guarantee (below).

Decision: **2**:

- **SOAP — `max(created_at)` cursor.** `created_at` is immutable and the only
  server-side filterable time column, which makes it a strictly safe cursor: a new
  order can never appear *behind* the watermark. Each run pulls the window
  `[watermark − overlap, now]` (`overlap` default 7 days). The overlap exists because
  of ADR-001's OFFSET-shift residual: rows inserted mid-pagination shift page
  boundaries, so boundary pages can be read twice or miss late commits — the overlap
  plus hash-guarded landing (ADR-007) makes both cases harmless. `updated_at` is
  *observed* per row (kept in the payload and tracked as run metadata) but cannot be
  a pull filter. Residual, documented honestly: a status change on an order *older*
  than the overlap window is invisible to the incremental run; `--full` re-pulls
  everything and lands unchanged rows as no-ops (the hash guard), so a periodic full
  refresh is cheap and side-effect-free. This is exactly the constraint real legacy
  SOAP APIs impose, and the interview answer for "what if the source can't filter on
  updated_at".
- **REST — no cursor; full walk per run.** A static catalog (ADR-003 residual) has
  no increments to subscribe to. Each run walks all pages of `/products` and
  `/promotions` through the opaque cursors; hash-guarded natural-key upserts make
  the re-walk a no-op for unchanged rows (measured in EVIDENCE/phase-2-ingest.md:
  second run lands 0 rows). A `last_full_walk` watermark row is recorded for
  observability only — it never gates anything.
- **FILE — per-file consumption ledger, never a date.** The decision to process a
  file is "present in the drop dir AND (not in `raw.ingest_files` OR content hash
  changed)". The filename date is *not* consulted: a backdated LATE batch
  (`--late-offset`) has a new name and lands normally, and a same-name regeneration
  (ADR-004 residual) is detected by hash change and re-landed. Files land atomically
  (rows + ledger row in one transaction), so a crash mid-file replays the whole file
  with zero effect.

**When the watermark advances**
1. Inside the landing transaction (atomic with the data) — tempting, but the mandate
   of this ADR's DoD is proving the *other* case.
2. Only *after* the load's landing transaction commits, in its own small transaction.

Decision: **2**. A crash between land and advance means the next run re-pulls the
window; every re-pulled row hits the content-hash guard and no-ops, then the
watermark advances. The SIGKILL probe in EVIDENCE/phase-2-ingest.md demonstrates
exactly this: kill mid-landing, rerun, zero duplicates, correct final counts.

**Retry policy (extraction-time resilience, applies to all HTTP sources)**
- `429`: sleep exactly the `Retry-After` header value (verbatim; fallback 5 s if the
  header is missing/unparsable), then retry.
- `5xx` and network errors: exponential backoff with full jitter —
  `uniform(0, min(cap, base · factor^(attempt−1)))`, base 0.5 s, factor 2, cap 8 s.
- Other `4xx`: fail immediately, no retry (a 404 is a contract break, not noise).
- Bounded attempts (default 8) then a loud `RetryExhausted` carrying the full
  attempt history; every attempt is a structured JSON log line (status, retry-after,
  slept seconds) so honoring Retry-After is *provable from logs*, not asserted.

## Consequences

+ One control table, three honest cursor strategies — each matched to what its
  source can actually guarantee, with the mismatches documented rather than papered
  over.
+ Crash-replay is bounded to one window (SOAP) / one file (FILE) / one walk (REST)
  and always converges through the hash guard; no dupe surface exists anywhere.
+ Late/backdated and regenerated files land correctly because no data decision is
  ever derived from a filename.
− SOAP incremental runs cannot see status changes on older-than-overlap orders;
  mitigation is `--full` (no-op for unchanged rows), surfaced in `make
  ingest-status` and the RUNBOOK.
− REST full walks always consume the rate budget (429s are *expected* on a walk,
  not an anomaly); the retry policy treats them as normal flow.
