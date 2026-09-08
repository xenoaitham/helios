# DATA DICTIONARY — HELIOS

Phase 0 stub. Filled from Phase 1 onward; the structure below is the contract every
future table entry follows.

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

_(empty until Phase 1 delivers the first real schemas)_

| Column | Source | Raw table | Staging model | Marts | Type | PII class |
|---|---|---|---|---|---|---|
