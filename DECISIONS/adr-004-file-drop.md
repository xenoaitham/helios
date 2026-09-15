# ADR-004 — file-drop: nightly CSV feed generator with realistic dirt

Status: accepted (2026-09-09, Phase 1, Session 3)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-002 (oltp), ADR-003 (rest-mock), BACKLOG item 4

## Context

Phase 1 needs the flat-file source: "nightly" customer/product CSV exports landing in an
SFTP-style drop directory, carrying realistic dirt (duplicate rows, ragged columns,
encoding issues, late files) so Phase-2's file extractor and Phase-4's DQ gates meet
real-world mess. DoD: the generator emits corruptible files **on demand**, and unit
tests classify each dirt mode. No new daemon, no database, near-zero dependencies
(disk at ~2.0 GB free).

## Options considered

**Delivery shape**
1. Always-on container emitting files every 24 h — rejected: a wall-clock daemon in a
   compose lab either does nothing for hours or lies about "nightly"; untestable.
2. **One-shot CLI** (`python -m filedrop.generate --for-date D [--late-offset N]`)
   writing into a named volume mounted at an SFTP-style drop point — chosen. "Nightly"
   is the *contract of the files* (one batch per date in the filename), not of a
   process; Phase-3 Airflow (or an operator) invokes it per batch date, which is
   exactly how real feed drops are replayed/backfilled. `--late-offset N` backdates the
   filename so a file for an older batch date lands *now* — the late-arrival scenario.
3. Host-side script writing into the repo tree — rejected: repo tree is versioned
   source, not data; the drop point is a volume (`filedrop_data`), like real SFTP
   storage outside the app.

**Dirt implementation**
1. Corrupt at the byte level randomly — unreproducible, untestable.
2. **Seeded, classified dirt applied as named modes** (`--dirt dupes,ragged,encoding`,
   `--clean` for the baseline) — chosen. Each mode is a pure function over the rendered
   CSV lines with a fixed-seed RNG, and returns per-file counts the report prints:
   - `dupes` — re-inserts ~2% of data lines verbatim after their original;
   - `ragged` — some data lines lose their last field, others gain a trailing empty
     field (safe: generated fields contain no commas/quotes; asserted);
   - `encoding` — one injected row carries cp1252-encoded accents inside the UTF-8
     file, so strict UTF-8 readers fail exactly like real vendor exports do.
3. Dirt embedded in "upstream system" fixtures — rejected: the generator *is* the
   upstream; fixtures would hide the parameterization the DoD asks for.

## Decision (as built)

- Image `helios/filedrop-tools:latest` (python:3.11-slim, **stdlib only** at runtime +
  pytest for tests), compose service `filedrop-tools` under `profiles: ["tools"]` —
  NOT part of `make up` (a drop tool appearing in every boot would spam the drop point);
  invoked via `make drop-generate / drop-generate-late / drop-ls / test-drop`.
- Feeds: `customers-YYYYMMDD.csv` (5,000 rows: customer_id, email, full_name,
  country_code, signup_date, tier — a *PII-carrying* feed: the staging hashing demo's
  third surface, recorded in the data dictionary) and `products-YYYYMMDD.csv`
  (2,000 rows: sku/name/category/price_cents/supplier_code — SKUs and prices reuse the
  OLTP/rest-mock space for warehouse coherence).
- Tests classify every mode on real rendered files (tmp dirs): clean parses with exact
  counts; dupes create verbatim repeated lines; ragged lines have wrong field counts
  (both short and long variants); the encoding file fails strict UTF-8 and the offending
  line decodes cleanly as cp1252; late files carry the backdated batch date; the CLI
  writes both feeds and reports counts.

## Consequences

- The drop point accumulates files across invocations (like real SFTP) — `drop-ls`
  inspects it; cleanup is `docker compose down -v` (destroys the volume) or manual
  `rm` inside a one-shot container. Documented in the RUNBOOK.
- Phase-2's extractor must therefore handle: re-delivered duplicates, ragged rows,
  non-UTF-8 bytes, and out-of-order batch dates — that is the point of this component.
- Determinism: same seed + same date → byte-identical feeds (except the report
  timestamp), so extractor bugs reproduce exactly.
