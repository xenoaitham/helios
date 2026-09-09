# ADR-002 — OLTP source database: schema-by-seeder, deterministic COPY load, continuous mutation loop

Status: accepted (2026-09-09, Phase 1, Session 3)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-000 (compose baseline), ADR-001 (soap-service), BACKLOG item 2

## Context

Phase 1 needs a *modern* OLTP source (users, orders, order_items, payments) with 5M+ rows
and continuous updates+deletes so Debezium CDC (Phase 2) sees real WAL churn. Hard
constraints from earlier sessions:

- `helios-oltp-db` (postgres:16-alpine, volume `oltp_data`) already exists and its volume
  must NOT be rebuilt — no `/docker-entrypoint-initdb.d/` mount for this database; the
  schema has to be applied by a seeder/CLI against the running instance.
- Host disk is the #1 risk: **3.0 GB free** at session start (100% used). Row budget and
  WAL peaks must be sized and measured, and a failed load must not leave partial data.
- Mimosa source rules: SQL in Python is inline at the call site, single static string,
  fully parameterized; no module-level SQL constants; no credential literals.

## Options considered

**Schema application**
1. Init-script mount + `docker compose down -v` rebuild — rejected: destroys the existing
   volume (mandated not to) and makes "recover the stack" include a surprise.
2. Alembic/migrations tooling — rejected: this is a *source simulator*, not an app with
   evolving schema; migration machinery is weight without story.
3. Idempotent DDL applied by a seeder container (`CREATE TABLE IF NOT EXISTS`, guarded
   `ADD CONSTRAINT`, `CREATE INDEX IF NOT EXISTS`) — chosen. Rerunnable, self-healing
   (constraints are re-ensured on every run, not only on first load), testable.

**Load mechanics**
1. Row-by-row INSERTs — ~5M round trips; minutes to tens of minutes; rejected.
2. `COPY ... FROM STDIN` streaming via psycopg3 (`copy.write_row`) — chosen: Postgres'
   native bulk path, constant memory, one transaction so the load is atomic (crash ⇒
   rollback ⇒ rerun starts clean; `--if-empty` can never see half a load).
3. Generate-then-load via CSV files on disk — rejected: doubles disk usage at 100%-full
   host.

PK / unique / FK constraints and indexes are created **after** the COPY (fast path), with
identity sequences re-synced via `setval` so post-seed INSERTs (the mutator) work.
Determinism: all business attributes are pure functions of `order_id`/`user_id` plus two
fixed-seed `random.Random` streams; money is integer cents rendered into `NUMERIC(*,2)`
(never floats). Order timestamps spread uniformly over exactly 3 years ending at seed time.

**Row budget** (sized against 3.0 GB free, to be *measured* at verification):

| table | rows (default scale) | est. heap+indexes |
|---|---|---|
| users | 50,000 | ~15 MB |
| orders | 500,000 | ~120 MB |
| order_items | ~4.25 M (3–14 per order, avg 8.5) | ~400 MB |
| payments | ~600 K (1.2 per order) | ~110 MB |
| **total** | **≥ 5.15 M** | **~650 MB + transient WAL** |

**Mutator shape**
1. Random ad-hoc SQL from a host script — rejected: not reproducible, not service-managed.
2. Long-running compose service (`oltp-mutator`, same image as the seeder) with a fixed
   2 s tick inside one transaction per tick — chosen. Per tick: walk a batch of orders
   forward through the status machine (`NEW→PROCESSING→SHIPPED→DELIVERED`, occasionally
   `NEW→CANCELLED`), touch `users.last_login_at`, INSERT 1–3 new orders (items via one
   `unnest($1::text[], …)` statement) + payments, DELETE the oldest CANCELLED orders
   (cascades items/payments, bounding growth). Exposes `GET /health` on :8081 (container
   healthcheck: 503 if no successful tick recently); retries politely if the schema is
   missing so it never crash-loops a fresh stack.
3. Cron-in-container — rejected: compose + restart policy + healthcheck is the platform's
   existing idiom (ADR-000).

**Status vocabulary** reuses the SOAP service's (`NEW/PROCESSING/SHIPPED/DELIVERED/
CANCELLED`) so Phase-2/3 can reconcile the two sources in the warehouse narrative.

## Decision (as built)

- One image `helios/oltp-tools` (python:3.11-slim + psycopg3, tests included), two compose
  services:
  - `oltp-seed` — one-shot (`restart: "no"`), `python -m oltp.seed --if-empty --report`;
    runs on every `make up`, no-op when already seeded.
  - `oltp-mutator` — always-on, `python -m oltp.mutate`, `depends_on: oltp-seed
    service_completed_successfully`, healthcheck via its own :8081 `/health`.
- `make seed-oltp / reseed-oltp (DESTRUCTIVE, oltp only) / oltp-status / test-oltp /
  mutator-logs`. `oltp-status` prints per-table counts, on-disk sizes, and a measured WAL
  delta (`pg_wal_lsn_diff` over a window) + `pg_stat_wal` counters — the DoD's
  "measurable WAL churn" instrument.
- Tests run against a dedicated `oltp_test` database (created+dropped per pytest session)
  so the 5M-row production-ish `oltp` database is never touched by tests.
- `wait-healthy.sh` gains `oltp-mutator` (8 long-running containers; a healthy mutator
  transitively proves the seed completed). The `smoke-test` Stage-1 script is left
  untouched at its 15/15 contract; oltp depth is covered by `oltp-status` + tests.
- Scale knobs `OLTP_SEED_USERS` / `OLTP_SEED_ORDERS` / `OLTP_MUTATE_TICK_SECONDS` in
  `.env(.example)` with 50,000 / 500,000 / 2 defaults.

## Consequences

- Fresh `make up` on an empty volume takes ~2–4 min extra while `oltp-seed` loads; the
  wait gate covers it (WAIT_TIMEOUT raised to 900 s).
- The whole load is one transaction: peak WAL during first seed can transiently approach
  `max_wal_size` (1 GB default) — sized to fit the 3 GB envelope, verified by `df`
  before/after in EVIDENCE.
- Mutator adds ~8 MB/day of heap churn to `orders` (updates + bounded insert/delete mix);
  autovacuum holds the line for a portfolio horizon — stated as a known limit, not fixed.
- `TRUNCATE`-based `--reset` is the only destructive path and is isolated behind
  `make reseed-oltp`, mirroring `reseed-soap`.
- If the seeder crashes between data load and constraint creation, the next
  `make seed-oltp` self-heals (constraints are ensured unconditionally on every run).
