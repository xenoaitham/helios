# ADR-005 — CDC: Debezium on the OLTP WAL → Kafka → custom raw-zone sink with (lsn, pk)-guarded upserts

Status: accepted (2026-09-10, Phase 2, Session 4)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-002 (oltp source + mutator), ADR-000 (compose baseline), BACKLOG item 5

## Context

Phase 2 moves data. The OLTP source (`helios-oltp-db`, 5.4M rows, volume must not be
rebuilt) is being churned continuously by `oltp-mutator` (~232 KB/s WAL, measured in
Phase 1). Changes must reach the warehouse `raw` schema so Phase 3 staging sees a
faithful, replayable history of the source. DoD: INSERT/UPDATE/DELETE visible in raw
within seconds; replay-safe.

Constraints carried from Phase 1:

- `oltp-db` runs default `wal_level=replica` — logical decoding requires
  `wal_level=logical`, applied as a compose `command` override + container recreate
  (volume preserved). Verified with `SHOW wal_level` BEFORE building downstream.
- Host disk was 1.7 GB free at Phase-1 close (the stated risk behind this ADR);
  re-measured at Session-4 start: **183 GB free** (`df -h /`, 76% used). The large
  JVM image decision is unblocked; numbers recorded in EVIDENCE/phase-2-cdc.md.
- Mimosa source rules: SQL inline static literals, parameterized; HTTP clients use
  literal/allowlisted URLs; secrets via `.env` only.

## Decision

**Log-based CDC with Debezium, custom Python sink into the raw zone.**

```
oltp-db (wal_level=logical, pgoutput slot) ──► cdc-connect (Kafka Connect, Debezium)
        │                                             │  topics: helios.public.<table>
        │                                             ▼
        └── mutator churn ────────────────► kafka (KRaft, existing)
                                                      │
                                                      ▼
                                    cdc-sink (Python consumer, this repo)
                                                      │  lsn-guarded upsert
                                                      ▼
                                    warehouse-db raw.cdc_<table> (pk JSONB PK)
```

1. **CDC vs dual-write** — Debezium chosen. Dual-write (mutator writes Postgres AND
   Kafka in the same tick) couples the source app to the transport, has no atomicity
   guarantee across the two systems (crash between the two writes loses or duplicates
   events), and would need the mutator rewritten. Log-based capture reads the WAL
   after the fact: zero source-app changes, complete op coverage (UPDATEs AND DELETEs),
   per-PK ordering by LSN, and a native replay point (the replication slot / Kafka
   offsets). Polling with `updated_at` was rejected: it misses DELETEs, gives no
   per-row ordering, and re-scans 5.4M rows per tick. Triggers were rejected:
   write amplification on the source and the same dual-write atomicity hole.

2. **Connect deployment** — stock `quay.io/debezium/connect` (Kafka Connect runtime +
   Postgres connector preinstalled) as `cdc-connect`, talking to the existing
   `apache/kafka:3.9.0` KRaft broker over the in-network listener (`kafka:9092`).
   `pgoutput` plugin (built into Postgres; no WAL2JSON extension install).
   Debezium ≥ 3.1 is aligned with Kafka 3.9. A lighter deployment (Debezium Server,
   ~600 MB, no Connect REST) was considered but rejected: Kafka Connect's REST API is
   the industry-standard control plane and gives us connector status/offsets for free.

3. **Raw-zone sink is OURS** — a custom Python consumer (`cdc-sink`) instead of the
   Confluent JDBC sink connector:
   - The DoD demands idempotent upsert keyed **(lsn, pk)** with stale-event rejection.
     A `WHERE excluded.lsn >= table.lsn` guard on `ON CONFLICT` is not expressible in
     the stock JDBC sink (it upserts by PK only, last-write-wins by arrival order —
     a replayed older event would clobber newer state).
   - Deletes must land as inspectable tombstone rows (`op='d'`), not silent row
     removal — staging filters them in Phase 3.
   - In-repo Python = unit-testable against a dedicated scratch DB (the established
     pattern), ~80 MB image, structured JSON logs, no connector-jar downloads.
   - Trade-off accepted: the sink is at-least-once, not exactly-once; the lsn guard
     makes the *effect* idempotent, which is the property the DoD names.

4. **Raw landing shape** (source-shaped, per ADR-000 raw policy) — `raw.cdc_users`,
   `raw.cdc_orders`, `raw.cdc_order_items`, `raw.cdc_payments`:

   ```
   pk        JSONB PRIMARY KEY   -- Debezium key payload; uniform for composite PKs
   lsn       BIGINT NOT NULL     -- Debezium source.lsn (0 for snapshot-read rows)
   op        TEXT NOT NULL       -- r|c|u|d  (d = tombstone row, after IS NULL)
   ts_ms     BIGINT NOT NULL     -- source.ts_ms
   before    JSONB, after JSONB  -- Debezium envelopes, verbatim (raw = as-landed)
   landed_at TIMESTAMPTZ DEFAULT now()
   ```

   Upsert: `ON CONFLICT (pk) DO UPDATE ... WHERE excluded.lsn >= <table>.lsn` —
   replaying any suffix or shuffle of the event stream converges to the same state.
   Money arrives as strings (`decimal.handling.mode=string`), timestamps as ISO-8601
   strings — raw stays exact; typing/casting is staging's job (Phase 3).

5. **Source grants** — dedicated role `helios_cdc` (`REPLICATION`, `LOGIN`,
   `SELECT` on the four tables). The pgoutput `publication` is created by the
   TABLE OWNER in `make cdc-setup` (`CREATE PUBLICATION ... FOR TABLE` requires
   owning the listed tables — a lesson this session's first connector start
   taught: Debezium's filtered auto-create as the replication role fails with
   "must be owner of table"), and the connector runs with
   `publication.autocreate.mode=disabled` + `publication.name=helios_publication`.
   Created idempotently via psql from `.env` values — never literals in the
   repo. Slot: `helios_cdc_slot`; heartbeat every 10 s so the slot advances
   even when only some tables change. Slot retention risk (stopped consumer
   ⇒ WAL growth) is surfaced by `make cdc-status` (restart/confirmed LSNs +
   `pg_wal`).

6. **Delivery semantics** — one topic per table (Debezium default naming under
   `topic.prefix=helios`), single partition ⇒ per-PK order preserved. The sink uses a
   consumer group (`helios-cdc-sink`), `enable_auto_commit=False`, commits offsets
   only after a batch is applied ⇒ at-least-once; the lsn guard makes replays no-ops.
   Snapshot (`snapshot.mode=initial`) reads the 5.4M-row baseline through the same
   topics and upsert path as streaming events — one mechanism, no bulk-copy special
   case. On restart, connector offsets in Kafka skip the snapshot (measured: re-up
   does not resnapshot).

7. **Self-healing wiring** — `cdc-sink` registers the connector idempotently at
   startup (`PUT /connectors/helios-oltp/config`) before consuming, waits for all
   four topics to exist, then subscribes. `make up` therefore brings the whole
   capture path up without manual steps; `make cdc-setup` remains the explicit
   one-time grant/wal_level helper.

## Consequences

- + True DELETE capture, per-PK LSN ordering, replay-safe raw state — the Phase 3
  staging layer gets a faithful source history.
- + No source-app changes; the mutator remains an independent churn generator.
- − One more always-on JVM container (~1.2–1.5 GB image, ~700 MB RAM heap default);
  acceptable at the re-measured disk/RAM levels, recorded in evidence.
- − Sink is Python per-message (JSON parse + executemany): snapshot throughput is
  the bottleneck, not the broker. Measured in EVIDENCE/phase-2-cdc.md; sized so the
  5.4M-row baseline drains in minutes, not hours.
- − Replication slot holds WAL if the sink/connect is down for long; runbook entry +
  `make cdc-status` make this observable.
- Raw `cdc_*` DDL is ensured by the sink at startup and mirrored in
  `infra/warehouse/init/02-cdc.sql` for fresh-rebuild parity (`make clean && make up`).
