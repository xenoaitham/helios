# RUNBOOK — operating HELIOS

Grows each phase (currently Phase 3). Audience: a stranger with the repo and Docker.

## 1. Daily driver commands

| Command | Effect |
|---|---|
| `make up` | Start platform; block until every container is healthy (fails loudly with logs otherwise) |
| `make ps` | Container status |
| `make logs` | Follow all service logs |
| `make smoke-test` | Stage 1: infra health incl. SOAP WSDL + auth checks (must pass). Stage 2: E2E ETL — fails loudly until Phase 3 |
| `make down` | Stop platform; named data volumes preserved |
| `make clean` | **DESTRUCTIVE**: stop + delete all data volumes (warehouse re-inits schemas; SOAP store re-seeds on next `make up`) |

Phase 1 additions (soap-service):

| Command | Effect |
|---|---|
| `make seed-soap` | Seed SOAP order history if empty; prints row counts + revenue (idempotent) |
| `make reseed-soap` | **DESTRUCTIVE to the SOAP store only**: drop + reseed its SQLite file |
| `make smoke-soap` | zeep round-trip against the running service (create → replay → status → legal/illegal transition → pagination) |
| `make test-soap` | pytest suite (35 tests) in a throwaway container |
| `make contract-freeze` | Re-capture `soap-service/contract/OrderManagement.wsdl` after a deliberate contract change |

Phase 1 additions (oltp source, ADR-002):

| Command | Effect |
|---|---|
| `make seed-oltp` | Apply schema + seed 5.4M rows via one COPY transaction if empty (idempotent; re-ensures constraints/indexes/sequences on every run) |
| `make reseed-oltp` | **DESTRUCTIVE to the OLTP source only**: `TRUNCATE` all four tables + reload |
| `make oltp-status` | Row counts, per-table on-disk size, and a 15 s measured WAL-delta report |
| `make test-oltp` | pytest suite (26 tests) in a throwaway container against a dedicated `oltp_test` db |
| `make mutator-logs` | Follow the continuous mutation loop's log |

Notes: fresh clones seed automatically on `make up` (the one-shot `oltp-seed` service
runs before `oltp-mutator` starts; ~3.5 min at default scale — `WAIT_TIMEOUT` defaults
to 900 s to cover it). The mutator writes continuously (updates + bounded inserts/
deletes) precisely so Phase-2 CDC sees WAL churn; `make oltp-status` is the instrument.

Phase 1 additions (rest-mock, ADR-003):

| Command | Effect |
|---|---|
| `make test-rest` | pytest suite (34 tests) in a throwaway container |
| `make smoke-rest` | Walks every `/promotions` page against the running service; retries real 429s/500s; asserts no dupes/gaps |

Tuning knobs in `.env`: `REST_MOCK_FLAKE_PERCENT` (default 5; set 100 to force failures
while debugging an extractor, 0 to silence), `REST_MOCK_RATE_CAPACITY` /
`REST_MOCK_RATE_REFILL_PER_SEC` (default 30 / 10 per second). Send an `X-API-Key`
header to get an isolated rate bucket.

Phase 1 additions (file-drop, ADR-004):

| Command | Effect |
|---|---|
| `make drop-generate` | Emit today's `customers-`/`products-<date>.csv` with all dirt modes into the drop volume |
| `make drop-generate-late` | Late-arrival simulation: backdated batch (2 days) lands now |
| `make drop-ls` | List drop volume: files, sizes, arrival timestamps |
| `make test-drop` | 15 dirt-classification + CLI tests |

Notes: the drop point is the `filedrop_data` volume (SFTP-style, outside the repo);
same inputs give byte-identical files; re-running the same batch date overwrites in
place. The drop volume grows until cleaned — targeted `docker compose run --rm
filedrop-tools sh -c "rm /data/drop/<file>"` or `make down -v` (destroys ALL data).

Phase 2 additions (ingest lib, ADR-006/007):

| Command | Effect |
|---|---|
| `make ingest-soap` | Windowed GetOrders pull (`[watermark − 7d, now]`), lands into `raw.soap_orders` |
| `make ingest-file` | Processes new/changed CSVs in the drop volume (hash ledger); rejects go to `raw.ingest_quarantine` |
| `make ingest-rest` | Full cursor walk of `/products` + `/promotions` (expects 429s; honors `Retry-After`) |
| `make ingest-all` | All three extractors, one shot |
| `make ingest-status` | Watermarks, landed counts, file ledger, quarantine summary, run ledger |
| `make test-ingest` | 54 pytest tests in a throwaway container (dedicated `ingest_test` db) |

Notes: extractors are one-shot tools (Airflow schedules them in Phase 3). Re-landing
identical content is a physical no-op (content-hash-guarded upserts) — rerunning
anything is always safe. SOAP first-ever run pulls full history (~382k orders,
~2.5 min measured); subsequent runs pull only the overlap window (~4 s measured).
Old-order status changes need `--full` (the API filters on `created_at` only).
Quarantine triage: `make ingest-status` shows reason + physical row number; fix the
source file and regenerate — the hash change re-lands it on the next run.

Phase 3 additions (dbt staging, ADR-008):

| Command | Effect |
|---|---|
| `make dbt-build` | Rebuild image + `dbt build`: 9 typed staging models over raw + 81 data tests (one unit) |
| `make dbt-test` | Rebuild image + `dbt test` standalone (same 81 tests) |
| `make dbt-freshness` | `dbt source freshness` — CDC warn 2 min / error 10 min; batch warn 26 h / error 50 h |
| `make dbt-image` | Build `helios/dbt:latest` only (dbt-core 1.9.11 + dbt-postgres 1.9.1 pinned) |

Notes: models are TABLES rebuilt full each run (~33 s; `stg_order_items` ~4.3M rows
is the long pole) — CDC current state is `op <> 'd'` over the raw envelopes, and
staging keeps `_cdc_lsn`/`_batch_ref` provenance. PII (`users` and `file_customers`
email/full_name) becomes `*_hash` = SHA-256(lower(trim(v)) || `PII_HASH_SALT`)
here; raw keeps cleartext, marts must never receive it (item 8). Rotating
`PII_HASH_SALT` invalidates every staging hash at once (ADR-008). Two gotchas a
stranger will hit: (1) code is baked into the image — the make targets rebuild it
first, never run a stale image; (2) when staging/raw counts "disagree", check
`make cdc-status` lag first — the sink draining a backlog (or simply applying
events mid-build) moves raw underneath you; a mutator pause is NOT a raw freeze.

Phase 3 additions (Airflow orchestration, ADR-010):

| Command | Effect |
|---|---|
| `make airflow-image` | Build `helios/airflow:2.10.5` (base + the host's compose plugin, staged at build time) and idempotently re-own the logs volume |
| `make run-etl` | Unpause → trigger master `daily_close` → poll to terminal state; nonzero exit on failure/timeout (a run counts as success only with all task instances green) |
| `make backfill` | Honest replay-based backfill: semantics banner + full `daily_close` replay + run-ledger tail (ADR-010 D5) |
| `make airflow-logs` | Follow scheduler + webserver logs |
| `make smoke-test` | Stage 1 (26 infra checks) + Stage 2: a real orchestrated run + mart parity + SCD2 + dead-letter + lineage + metrics assertions (exit 0) |

Orchestrator notes a stranger needs:

- **Topology**: `daily_close` (05:00 UTC, catchup=False, max_active_runs=1) =
  `trigger_ingest_file → (trigger_ingest_soap ∥ trigger_ingest_rest) →
  dbt_build`. The per-source DAGs (`ingest_soap/file/rest`, schedule=None) own
  their retries; the master composes them via TriggerDagRunOperator. `dbt_build`
  is the DAG-level DQ gate (one `dbt build`; staging tests gate marts inside it;
  never `--full-refresh`).
- **Backfill semantics (honest)**: the extract CLIs have no historical window
  parameters; the 3 years of history were backfilled at first landing and
  watermarks + the hash ledger keep them correct. `make backfill` therefore
  replays the whole pipeline (provably zero-work: `rows_landed=0`), and the one
  real window replay is the manual
  `docker compose run --rm ingest python -m ingest.run --source soap --full`
  (epoch→now, ~2.5 min, hash-guarded no-op for unchanged rows).
- **Idempotency triage stays the same**: count drift between runs is live-CDC
  churn — `make cdc-status` FIRST. Under a deliberately quiesced upstream, two
  consecutive `make run-etl` runs are bit-identical (proven in
  EVIDENCE/phase-3-airflow.md).
- **Do not** run `make dbt-build` by hand while a `daily_close` run is active:
  `max_active_runs=1` serializes DAG-initiated builds only; a manual build can
  still race the DAG's (two full-refresh-rebuilt marts layers = wasted work, and
  the snapshot is not concurrency-safe). Check the UI :8080 first.
- **DAG edits**: DAGs are bind-mounted (`./dags`) — the scheduler re-parses
  within ~30 s; no rebuild needed for DAG-file changes. Ingest/dbt code changes
  go through the images (`docker compose build ingest` / `make dbt-build`); the
  DAG's dbt task rebuilds its image on every run by design.
- **Hand-running compose** (not via `make`): export
  `HELIOS_PROJECT_DIR="$(pwd)"` first — the compose file requires it (the
  scheduler's repo mount + DAG tasks `cd` there; bind-source parity, ADR-010
  D1). Missing var = loud interpolation error by design.

Phase 4 additions (metrics, ADR-013):

| Command | Effect |
|---|---|
| `make metrics-verify` | Assert MEASURED metrics via the Prometheus/Grafana APIs (targets up, rules loaded, mapped metric names in the exporter, real query values, provisioned datasources + dashboard) and dump evidence to `EVIDENCE/phase-4-metrics/`; nonzero on any missing surface |
| `make metrics-drill` | Alert fire-drill: stop `statsd-exporter` (a genuinely scraped target) → assert `HeliosScrapeTargetDown` FIRES via `/api/v1/alerts` → restart → assert recovery; dumps `EVIDENCE/phase-4-metrics/drill/` |

Metrics operations a stranger needs:

- **Topology**: Airflow 2.10.5 → StatsD UDP :9125 (fire-and-forget:
  dropped-not-queued — the scheduler logs
  `using NoStatsLogger instead` and carries on) → `statsd-exporter`
  (mapping as code in `observability/statsd-exporter/`; static IP
  `172.31.0.9` on the `metrics-net` network — the airflow client caches the
  resolved destination for its process lifetime, so the exporter must keep
  that address across restarts; ADR-013 D7 amended) → Prometheus :9091
  (scrape + rules from `observability/prometheus/`) → Grafana :3001
  (dashboards + datasources provisioned from `observability/grafana/`).
- **Alerting ceiling (honest)**: three Prometheus rules —
  `HeliosScrapeTargetDown` (`up==0`), `HeliosAirflowTaskFailure`
  (`increase(airflow_task_finish_total{state="failed"}[10m]) > 0`),
  `HeliosDailyCloseStale` (`absent_over_time(...success...[26h])` — fires
  legitimately on a fresh stack until the first successful close). They
  surface as the ALERTS series, Prometheus `/alerts`, and the Grafana
  firing-alerts panel. NO Alertmanager / push channel exists — nothing
  notifies anyone; that is the documented ceiling, not a fake.
- **Changing the exporter mapping / Grafana dashboards**: edit the files
  under `observability/` — dashboards re-provision automatically (file
  watcher); an exporter mapping change is a `docker compose restart
  statsd-exporter` away. The static IP makes BOTH safe for the metric flow.
- **Wipe story**: dashboards/rules/config are code; the TSDB
  (`prometheus_data`) and Grafana state (`grafana_data`) are derived
  EPHEMERAL volumes — metrics history is NOT re-derivable (unlike lineage);
  `make clean` deletes them by design.
- **Row counts / task durations in the dashboard** are read-only SQL pulls
  (warehouse-db / airflow-db datasources, credentials via env interpolation
  into the grafana container). If the Postgres panels show "No data": check
  datasource health (`curl -su admin:… :3001/api/datasources/uid/helios-warehouse/health`)
  — and remember Grafana 13 wants the DB name in `jsonData.database`.

## 2. Endpoints & credentials

All credentials live in `.env` (defaults in `.env.example`). Currently surfaced:

- **Airflow UI**: http://localhost:8080 — `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD`
- **OLTP Postgres**: `localhost:${OLTP_PORT}` db `oltp`
- **Warehouse Postgres**: `localhost:${WAREHOUSE_PORT}` db `warehouse`,
  schemas `raw` / `staging` / `marts`
- **Kafka (from host)**: `localhost:${KAFKA_HOST_PORT}` (in-network: `kafka:9092`)
- **SOAP OrderManagement**: `http://localhost:${SOAP_PORT}/?wsdl` — HTTP basic auth
  (`SOAP_BASIC_AUTH_USER` / `SOAP_BASIC_AUTH_PASSWORD`) required on every path including
  the WSDL; `/health` is the only unauthenticated endpoint (container healthcheck).
  401 + `WWW-Authenticate` on missing/bad credentials.
- **Marquez lineage UI** (ADR-012): `http://localhost:${MARQUEZ_WEB_PORT}` (UI),
  `http://localhost:${MARQUEZ_API_PORT}` (lineage REST), admin `:5001/healthcheck` —
  unauthenticated, local dev.
- **Grafana** (ADR-013): `http://localhost:${GRAFANA_PORT}` —
  `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` (basic auth).
- **Prometheus** (ADR-013): `http://localhost:${PROMETHEUS_PORT}` — unauthenticated,
  local dev (targets, rules, `/alerts`, `/api/v1/*`).

Quick SOAP checks from host:

```bash
make smoke-soap                                        # full zeep round trip
curl -su "$SOAP_BASIC_AUTH_USER:$SOAP_BASIC_AUTH_PASSWORD" \
  "http://localhost:${SOAP_PORT}/?wsdl" | head -5      # peek at the WSDL
```

## 3. Failure playbook

| Symptom | Diagnosis | Recovery |
|---|---|---|
| `make up` timeout on a container | `docker logs helios-<svc>` (wait-healthy.sh prints tail) | Fix env/port, `make down && make up` |
| Port already in use | `ss -ltn \| grep <port>` | Change `*_PORT` in `.env`, `make down && make up` |
| `smoke-test` Stage 1 fails | Some container unhealthy or not answering | `make down && make up`; if persists, `docker logs helios-<svc>` |
| Warehouse missing schemas | Volume was created before init script existed | `make clean && make up` (destroys data — Phase 0 has none worth keeping) |
| Airflow UI 502 / not up yet | webserver start_period ~30-60s | Re-run `make ps`; check `helios-airflow-init` exited 0 |
| First `make up` slow on soap-service | First boot seeds ~382k orders (~45 s); healthcheck `start_period` 150 s covers it | Nothing — subsequent boots skip (store non-empty) |
| First `make up` slow on oltp-seed | Fresh volume: schema + 5.4M-row COPY seed (~3.5 min); `make up` waits via oltp-mutator's dependency chain | Nothing — subsequent boots hit the `--if-empty` skip (~1 s) |
| `oltp-mutator` unhealthy / crash-looping | `make mutator-logs`; it retries politely while the schema is missing | After a schema/db fix it self-recovers; `docker compose restart oltp-mutator` to force |
| `oltp-seed` fails mid-load (e.g. disk full) | Seed is one transaction — a crash rolls back to an empty schema | Free disk (`docker builder prune`), re-run `make seed-oltp` |
| Need OLTP rows/types reference | `docs/DATA_DICTIONARY.md` (OLTP section) | — |
| SOAP 401 in scripts | Credentials missing in env; the service refuses to boot without `SOAP_BASIC_AUTH_*` | Set them in `.env`, `make up` |
| `test_served_wsdl_matches_golden` fails | The contract changed (spyne type set) | Review the WSDL diff like an API change, then `make contract-freeze` and commit both |

## 4. Recovery-from-scratch drill (Phase 0)

```bash
make clean && make up && make ps   # expect: all healthy, schemas re-created
```

This is the exact drill `EVIDENCE/phase-0.md` records.

## 4b. CDC operations (Phase 2, ADR-005)

The capture path is `oltp-db (wal_level=logical) → cdc-connect (Debezium, pgoutput)
→ Kafka topics helios.public.<table> → cdc-sink → warehouse raw.cdc_<table>`.
The sink registers the connector idempotently on every start, so a plain
`make up` brings the whole path up; `make cdc-setup` is only needed when the
role/publication don't exist yet (fresh volume) or `wal_level` was reset.

| Task | Command | Notes |
|---|---|---|
| One-time bootstrap (wal_level, role, publication) | `make cdc-setup` | idempotent; recreates oltp-db container (volume preserved) |
| Control-plane report | `make cdc-status` | connector state, per-topic lag, slot retention, raw counts |
| DoD verification (baseline, marker latency, replay safety) | `make cdc-verify` | stops `oltp-mutator` for a controlled window, restarts it after |

**Replication slot retention — the one thing that can hurt this stack.** While
the sink or Connect is down, the `helios_cdc_slot` pins WAL on `oltp-db` and
`pg_wal` grows. `make cdc-status` shows `retained_wal`; if it grows past a few
hundred MB, bring the sink back (`docker compose start cdc-sink`) and let it
drain. In a genuine emergency (source disk at risk), the slot can be dropped
(`SELECT pg_drop_replication_slot('helios_cdc_slot')`) — the connector then
resnapshots on next start; data captured in between is lost (batch sources are
unaffected). This is a documented manual intervention, not a routine step.

Replay semantics: raw.cdc_<table> rows are guarded by
`WHERE EXCLUDED.lsn >= table.lsn`, so re-consuming any Kafka range (offset
reset, group deletion, full re-drain after `TRUNCATE raw.cdc_*`) converges to
the same state without dupes. `make cdc-verify` stage [4] proves it live.

Schema drift: adding a column to an oltp table lands in the Debezium envelopes
automatically (JSONB `after` in raw); dropping/renaming needs the publication
member list refreshed (`make cdc-setup` re-adds the four known tables) and is a
Phase-5 chaos scenario.

### Snapshot protection (Phase 3 item 8, ADR-009)

`snapshots.customers_snapshot` is the SCD2 history store — the only dbt
relation whose state persists across builds.

- NEVER run the marts build with `--full-refresh`: a snapshot full-refresh
  drops and re-creates it from current state = instant history wipe.
  `make dbt-build` is deliberately a plain `dbt build` — keep it that way.
  The DAG's `dbt_build` task is the same plain build (verified by grep in
  EVIDENCE/phase-3-airflow.md).
- NEVER `drop schema snapshots` / truncate the table outside `make clean`
  (which wipes everything and reseeds).
- History starts at the snapshot's first run; a normal `dbt build` only ever
  appends versions (quiet run logs `INSERT 0 0`). Verify with:
  `select min(dbt_valid_from), count(*) from snapshots.customers_snapshot;`
  (min must never move forward between builds).
- Rotating `PII_HASH_SALT` invalidates every staging hash at once → the next
  build would record ~50 k "changes". Treat salt rotation as a destructive,
  planned event: rotate AND consciously rebuild the snapshot from scratch.

### Airflow operations (Phase 3 item 9, ADR-010)

| Task | Command | Notes |
|---|---|---|
| Trigger the pipeline and watch it | `make run-etl` | nonzero exit on DAG failure/timeout; run_id `etl-<ts>` |
| Replay-based backfill | `make backfill` | run_id `backfill-<ts>`; zero-work proof in the ledger tail |
| Scheduler/webserver logs | `make airflow-logs` | task logs live in the UI (or `airflow_logs` volume) |
| DAG health | `airflow dags list-import-errors` (in-scheduler) | also asserted in smoke Stage 2 |

- **Trigger-before-start_date trap** (hit for real, 2026-09-11): Airflow marks
  a run "success" with ZERO task instances when the execution date precedes the
  DAG start_date (verify_integrity filters tasks by start_date and the scheduler
  skips verify_integrity when the run's dag_hash matches the serialization).
  `START_DATE` is pinned in the past and `run-etl.sh` refuses zero-task
  "successes" — do not raise START_DATE to "now".
- **Rootless socket**: DAG tasks run `docker compose` inside the scheduler
  against the host's rootless daemon (`/run/user/<AIRFLOW_UID>/docker.sock`).
  The airflow containers run as the daemon's userns root (= your user); any
  nonzero container uid maps into the subuid range and gets `permission denied`
  (measured). `AIRFLOW_UID` in `.env` is your host uid and feeds the socket
  path only. After changing it: `make airflow-image` (re-owns the logs volume).
- **SOAP WSDL address quirk** (hit for real, 2026-09-12): the served WSDL's
  `soap:address` alternates `localhost`/`soap-service` across requests; the
  ingest extractor pins its zeep endpoint to the validated base URL (contract
  from the WSDL, routing never). Hand-rolled clients should do the same.
- SLA misses surface in the UI only (no SMTP in this stack; alerting is
  Phase 4 item 12).

### DQ gate operations (Phase 4 item 10, ADR-011)

The semantic gate (`dq_gate` task, after `dbt_build` in `daily_close`) runs
Great Expectations suites over the FROZEN staging/marts relations every build
produces, and dead-letters offending rows to `dq.dq_quarantine` with full
provenance (suite, expectation, source pk, payload, run_id). One failing
expectation → nonzero exit → the close is blocked AT THE GATE. It never reads
`raw`/live CDC, so it cannot flake on mutator drift (ADR-011 D4).

| Task | Command | Notes |
|---|---|---|
| Run the gate manually | `make dq-run` | same container the DAG task runs; nonzero on failure |
| Dead-letter status | `make dq-status` | open/resolved counts + recent incidents |
| Resolve after a source fix | `make dq-replay` | resolves open incidents that no longer reproduce; nonzero if any remain |
| dq unit tests | `make test-dq` | 32 tests, throwaway container |

**When the gate goes red** (event `dq.gate_failed` in `make airflow-logs` or
`make dq-run` output):

1. `make dq-status` — see which suite/rows are dead-lettered; each row's
   `failure_reason` states the rule and the observed values.
2. Fix the SOURCE (never hand-edit the warehouse): file feed → drop a
   corrected CSV with the SAME filename (the hash ledger re-lands it); batch
   REST → source-side fix, next walk refreshes; CDC/OLTP → corrected row
   upserts through the normal CDC path; SOAP → the documented `--full` replay.
3. Rebuild (`make run-etl`, or `make dbt-build` + `make dq-run`) — the gate
   must go green on the fixed data.
4. `make dq-replay` — re-runs the gate and resolves every open incident whose
   row no longer violates. "Quarantine empty" = zero open incidents; resolved
   history is kept forever as the audit trail.

The alert IS the gate: DAG task failure (UI + `make run-etl` nonzero), the
structured `dq.gate_failed` JSON event, and the `dq.dq_quarantine` table
itself. No SMTP/Slack exists and none is faked; real push-alerting arrives
with item 12 (Prometheus rules).

### 4c. Lineage operations (Phase 4 item 11, ADR-012)

Marquez is the OpenLineage backend: `marquez-db` (dedicated Postgres, volume
`marquez_db_data`), `marquez-api` (lineage REST :5000, admin :5001 with
`/healthcheck`), `marquez-web` (UI :3000). The emitters are the Airflow
provider (in the 2.10.5 base image, env-wired in compose) and the `dbt-ol`
wrapper (the dbt image's ENTRYPOINT — the same `docker compose run --rm dbt
build` command `make dbt-build` and the DAG task have always used).

| Task | Command | Notes |
|---|---|---|
| Verify measured lineage | `make lineage-verify` | API-asserts the dataset graph, daily_close jobs (incl. dq_gate) and column-level lineage into `fct_orders`; dumps JSON/CSV to `EVIDENCE/phase-4-lineage/` |
| Look at the graph | http://localhost:3000 | pick a dataset → lineage graph; the **column-level** page renders per-column edges (e.g. `dim_customer.customer_sk ─→ fct_orders.customer_sk`) |
| Wipe the lineage store | `make down -v` deletes it (or full `make clean`) | lineage is DERIVED state: one `make dbt-build` + `make run-etl` re-derives the graph; no source data touched |
| Backend outage | `docker compose stop marquez-api` | the pipeline is non-fatal by contract (ADR-012 D6): dbt build and daily_close stay green (emission retries add ~9 s/event ≈ up to ~12 min to dbt_build under a total outage — inside the task's 20-min timeout); events resume on `docker compose start marquez-api` + the next build/run. Emissions attempted during an outage are dropped by the client (no queueing/backfill). |

What the graph shows (the emit boundary, ADR-012 D2): dbt's **raw sources →
staging → marts** with column-level lineage where the SQL parser resolves it;
the ingest→raw hop is job-level only (the true sources are outside SQL), and
`dq_gate` appears as an Airflow job without dataset edges. Measured naming:
Airflow jobs live in namespace `helios` (`daily_close.<task_id>`); dbt
datasets live in namespace `postgres://warehouse-db:5432`
(`<db>.<schema>.<model>`). Column-level API: `GET /api/v1/column-lineage?nodeId=dataset:<ns>:<dataset>&withDownstream=true`
(the `dataset:`-prefixed shape — a `datasetField:`-prefixed nodeId 500s on
these namespaces).

## 5. Environment notes (this repo's dev machine)

The baseline was built on a host where the system Docker daemon is disabled and sudo is
unavailable. Docker runs **rootless** under the dev user; the bootstrap (already applied)
was:

1. Static `slirp4netns` + `fuse-overlayfs` binaries into `~/.local/bin`.
2. User systemd unit `~/.config/systemd/user/docker.service` running
   `dockerd-rootless.sh` (Type=notify, NotifyAccess=all).
3. `systemctl --user enable --now docker.service`.
4. `docker context create rootless --docker host=unix:///run/user/1000/docker.sock &&
   docker context use rootless`.

On any normal machine with the system daemon running, none of this is needed —
`make up` works unchanged (that is the point of the context indirection).

Rootless caveats: published ports bind on host loopback (localhost URLs only), no cgroup
resource limits, images live in `~/.local/share/docker` (watch disk — this host is tight).

Rootless + Airflow (item 9, ADR-010): the scheduler invokes the one-shot tool
containers through the rootless socket. The socket's group is an unnamed
rootlesskit allocation (not restart-stable) and any nonzero container uid maps
into the subuid range — so the airflow services run as `user: "0:0"`, which in
the daemon's user namespace IS your host user (no privilege beyond what the
daemon user already holds). `AIRFLOW_UID` (host uid) feeds the socket's
host-side path; `make airflow-image` re-owns the logs volume after a uid change.
