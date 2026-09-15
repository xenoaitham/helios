# HELIOS - Legacy-to-Lakehouse ETL Modernization Platform

> A containerized enterprise data platform that modernizes legacy SOAP services, flat-file
> batch feeds, REST APIs, and OLTP database changes (CDC) into a governed dimensional
> warehouse - with orchestrated ETL, data-quality gates, lineage tracking, and full
> observability.

**This is a personal project** (not employment work). Every number below was measured on
the machine it was built on, and every claim maps to a `make` target you can run.

| Milestone | What it covers | State |
|---|---|---|
| 0 | Scaffold: compose baseline, Makefile, health gating | done |
| 1 | Sources: SOAP service, REST mock, dirty file feeds, OLTP seeder + mutator | done |
| 2 | Movement: Debezium CDC -> Kafka -> raw zone; batch extractors | done |
| 3 | Warehouse & dbt: star schema, SCD2, Airflow DAGs, `make run-etl` | done |
| 4 | Trust & observability: Great Expectations gates, Marquez lineage, Prometheus + Grafana | done |
| 5 | Chaos & performance: `make chaos-test` (7/7 scenarios), `make bench` (three measured passes) | done |
| 6 | Package: RUNBOOK, data dictionary, design notes, screenshots | done |

Want to see it without running it? Real captures of the live UIs are in
[`screenshots/`](screenshots/) (Airflow grid, Grafana dashboard, Marquez lineage).

## Quickstart

```bash
make up          # start + wait until every container is healthy
                 # (fresh volume: ends non-zero on cdc-sink by design until the one-time
                 #  `make cdc-setup && docker compose restart cdc-sink`, then `make up` again)
make ps          # all containers healthy
make smoke-test  # Stage 1: 26 infra checks + Stage 2: a real orchestrated close + parity/SCD2/dead-letter/lineage/metrics assertions (exit 0)
make down        # stop (data volumes preserved)
```

First run creates `.env` from `.env.example` (local-dev defaults) automatically, pulls
the images once (sizes visible via `docker images` - Airflow is the big one), and seeds
the sources (SOAP ~382k orders ~44 s; OLTP 5.4M rows ~3.5 min). The full fresh-clone walk -
including the one-time CDC bootstrap and first close - is the
[recover-from-scratch drill](docs/RUNBOOK.md#4-recovery-from-scratch-drill--the-honest-version-adr-016-d1)
in the RUNBOOK.

### What is running now

| Service | URL / conn | Credentials (.env) |
|---|---|---|
| Airflow UI | http://localhost:8080 | `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD` |
| OLTP Postgres (source) | localhost:5432, db `oltp` | `OLTP_POSTGRES_USER` / `OLTP_POSTGRES_PASSWORD` |
| Warehouse Postgres | localhost:5433, db `warehouse` (schemas `raw`, `staging`, `marts`) | `WAREHOUSE_POSTGRES_USER` / `WAREHOUSE_POSTGRES_PASSWORD` |
| Kafka (host listener) | localhost:29092 | n/a (PLAINTEXT, local dev) |
| SOAP OrderManagement | http://localhost:8000/?wsdl (WSDL), `/health` (unauthenticated) | `SOAP_BASIC_AUTH_USER` / `SOAP_BASIC_AUTH_PASSWORD` (HTTP basic auth; required on every SOAP path incl. WSDL) |
| Marquez lineage UI | http://localhost:3000 (graph + column-level views); lineage REST http://localhost:5000, admin :5001 | n/a (unauthenticated, local dev) |
| Grafana dashboards | http://localhost:3001 (dashboard uid `helios-pipeline`) | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| Prometheus | http://localhost:9091 (targets, rules, `/alerts`); scrape + alert config as code in `observability/` | n/a (unauthenticated, local dev) |
| OLTP seeder / mutator | one-shot + long-running containers; mutator `/health` is internal (:8081, healthcheck only) | reuses `OLTP_POSTGRES_*` |

`make smoke-test` runs 26 Stage-1 infra checks and a full Stage-2 E2E
verification (an orchestrated daily_close run + mart parity, SCD2, dead-letter,
lineage and metrics assertions) and exits non-zero on any mismatch.

### Lineage

`daily_close` tasks and the wrapped dbt build emit OpenLineage to Marquez. The
graph is **dbt's raw sources -> staging -> marts** (the ingest->raw hop is
job-level only - the emit boundary is stated verbatim in ADR-012 D2), with
column-level lineage into marts (e.g. `dim_customer.customer_sk ->
fct_orders.customer_sk`). Verify it yourself:

```bash
make lineage-verify   # API-asserted: datasets, daily_close jobs incl. dq_gate, column lineage
make run-etl          # a full close emits fresh events (dbt_build runs dbt-ol; every task emits via the provider)
```

The pipeline is non-fatal when Marquez is down (drilled: full close green with
the api stopped; events resume on return).

### Metrics & alerting

Airflow 2.10.5 emits StatsD (the only metrics egress the installed version
has - verified in the running image; names verified against the installed
source, not blogs) -> `statsd-exporter` maps the name-encoded legacy metrics
(`airflow_task_finish_total{dag_id,task_id,state}`,
`airflow_dagrun_duration_seconds{dag_id,status}` histogram,
`airflow_scheduler_heartbeat`) -> Prometheus scrapes + evaluates three alert
rules -> Grafana renders [dashboards as code](observability/grafana/).
Row counts and task durations are read-only SQL pulls (warehouse-db /
airflow-db datasources) - observability only ever pulls; UDP is
dropped-not-queued; no `depends_on` touches the metrics stack. Drills:
`make metrics-drill` (a rule genuinely fires on a stopped scraped target,
then recovers) and the full-stack non-fatal drill (two closes green with
Prometheus + Grafana + the exporter stopped; `NoStatsLogger` fallback in the
scheduler's own logs).

```bash
make metrics-verify   # API-asserted: targets up, rules loaded, real metric values, Grafana provisioning
make metrics-drill    # alert fire-drill: stop a scraped target -> HeliosScrapeTargetDown fires -> restart -> resolves
```

Honest ceiling: NO Alertmanager and no push channel exists - rules surface as
the ALERTS series, Prometheus `/alerts`, and the dashboard's firing-alerts
table; nothing notifies anyone. Metrics history is derived ephemeral state
(wipe story in ADR-013 D4: not re-derivable, unlike lineage).

### Chaos engineering

Seven scripted destructive scenarios - each one: pre-state measurement ->
chaos act -> assert the platform DEGRADES SAFELY (named mechanism, measured
while degraded) -> recovery -> a measured convergence proof. The full suite
ran 7/7 end-to-end in one sitting (wall ~53 min):

```bash
make chaos-test                      # all 7, in order (~60-90 min wall)
make chaos-test SCENARIO=poison_cdc  # one scenario: number (01..07) or name
```

Scenarios: `kill_worker` (SIGKILL the dbt one-shot mid-build -> the Airflow
task retry converges the SAME run; snapshot invariant bit-identical),
`kill_warehouse_midbuild` (warehouse-db down mid-build -> the build fails
loudly, the published layer is never partial - dbt commits per-model, so
every target is a complete replacement - and the retry rebuilds on the
restored DB),
`kill_oltp_midcdc` (source down -> the replication slot holds; **measured: a
FAILED Debezium task does not self-recover - the scripted recovery restarts
the task via the Connect API**; retained WAL peak/drain measured),
`poison_cdc` / `poison_csv` (gate red -> dead-letter -> **HeliosAirflowTaskFailure
fires, 69-80 s measured across three executions** -> source fix -> green close ->
`dq-replay` resolves; row-level targeting re-proven),
`schema_drift` (ADD COLUMN proven invisible end-to-end - the honest gap -
plus a guarded rename probe: the mutator fails loudly, the pipeline would
not detect it either; both probed with the EXIT-trap guard, and the probe
column still stands in the live source as the exhibit),
`api_outage` (**the dependency contract self-heals stopped/paused/partitioned
sources - measured 3 ways - so the outage is a watchdog-enforced network
partition; task failures are loud, in-run retries converge, raw counts
bit-identical**).

Destructive by design, re-runnable (ADR-014 D7): quarantine only ever grows
by RESOLVED audit rows; `dq.dq_quarantine` ends 0 OPEN. Smoke-test is the
green-state guard and does not grow.

### Bench

`make bench` times the REAL pipeline per stage - every rate is a measured row
count divided by a measured wall, computed by the leg scripts themselves.
Three complete 6/6-leg passes on this laptop (i5-10400F, 15 GiB, rootless
Docker - these are THIS machine's numbers, not production benchmarks); the
honest headline BANDS (never a single run):

| Stage | Band (three passes) |
|---|---|
| dbt full re-materialization (14 models + snapshot - the platform's real full load) | **14,095,849-14,136,532 rows in 65-82 s = 172k-217k rows/s** (the ~5.45M-row items table: ~300-334k staged) |
| SOAP full walk (READ; landed=0 by idempotence) | 382,179 rows @ 2,637-2,773 rows/s |
| CDC applied drain (live mutator, lag=0) | 7.4-7.7 events/s - the mutator's pace, not a ceiling (≈6.3k events/s measured at snapshot drain) |
| GE semantic gate scan (6 suites) | ~8.5M rows @ 608k-766k rows/s |
| Orchestrated `daily_close` end-to-end | 150.7-163.4 s server-side, 5/5 tasks green (dbt_build 69-82 s, dq_gate 16-17 s) |

## Architecture

```mermaid
flowchart LR
    subgraph SRC["Legacy sources"]
        OLTP[("oltp-db :5432<br/>5M+ rows, wal_level=logical")]
        MUT["oltp-mutator<br/>continuous churn"]
        SOAP["soap-service :8000<br/>SOAP 1.1, basic auth<br/>frozen WSDL, 382,179 orders"]
        REST["rest-mock :8001<br/>cursor paging, 429s, flaky 500s"]
        FF["file-drop<br/>nightly dirty CSVs (volume)"]
        MUT --> OLTP
    end

    subgraph MOVE["Ingestion and movement"]
        DEB["cdc-connect<br/>Debezium, logical slot"]
        KAFKA[("kafka :29092<br/>KRaft single broker")]
        SINK["cdc-sink<br/>lsn-guarded upserts"]
        TOOLS["one-shot tool containers<br/>ingest - dbt build - dq gate<br/>watermarks, hash ledger, GE suites"]
        DEB --> KAFKA --> SINK
    end

    subgraph WH["Warehouse"]
        WDB[("warehouse-db :5433")]
        RAW[("raw schema")]
        STG[("staging schema<br/>PII hashed here")]
        MART[("marts schema<br/>star + SCD2 snapshot")]
        RAW --> STG --> MART
    end

    subgraph ORCH["Orchestration"]
        AW["airflow-webserver :8080"]
        AS["airflow-scheduler<br/>daily_close 05:00 UTC<br/>rootless-socket one-shots"]
        ADB[("airflow-db")]
        AS -.-> ADB
        AW -.-> ADB
    end

    subgraph LIN["Lineage - marquez x3"]
        MAPI["marquez-api :5000"]
        MDB[("marquez-db")]
        MWEB["marquez-web :3000"]
        MAPI --> MDB
        MWEB --> MAPI
    end

    subgraph OBS["Observability"]
        SDE["statsd-exporter<br/>static IP 172.31.0.9"]
        PROM["prometheus :9091<br/>3 alert rules"]
        GRAF["grafana :3001<br/>dashboards as code"]
        SDE --> PROM --> GRAF
    end

    OLTP --> DEB
    SOAP --> TOOLS
    FF --> TOOLS
    REST --> TOOLS
    TOOLS --> RAW
    SINK --> RAW
    AS -->|"docker compose run one-shots"| TOOLS
    TOOLS --> WDB
    AS -.->|"OpenLineage events"| MAPI
    AS -.->|"StatsD UDP :9125"| SDE
    GRAF -.->|"read-only SQL pulls"| WDB
```

The 17 long-running containers this diagram maps to: `oltp-db`, `oltp-mutator`,
`soap-service`, `rest-mock`, `kafka`, `cdc-connect`, `cdc-sink`, `warehouse-db`,
`airflow-webserver`, `airflow-scheduler`, `airflow-db`, `marquez-db`,
`marquez-api`, `marquez-web`, `statsd-exporter`, `prometheus`, `grafana` - all
healthy after one `make up` (the batch tools are one-shot containers, run by
`make` or by the scheduler, never long-lived).

### Source 1: soap-service

Legacy "OrderManagement" SOAP 1.1 service ([ADR-001](DECISIONS/adr-001-soap-service.md)):
spyne, HTTP basic auth, operations `CreateOrder / GetOrders / GetOrderStatus /
UpdateOrderStatus`, state machine NEW->PROCESSING->SHIPPED->DELIVERED (+CANCELLED from
NEW/PROCESSING), idempotent `CreateOrder` via `client_reference`, faults
(`OrderNotFound`, `InvalidStateTransition`, `ValidationError`,
`ClientReferenceConflict`). Owns its own SQLite store on the `soap_data` volume
(deliberately NOT the OLTP Postgres - independent source systems), seeded
deterministically with 3 years of order history on first boot.

```bash
make seed-soap          # row counts + gross revenue report (idempotent)
make smoke-soap         # zeep round-trip: create/replay/status/transition-fault/pagination
make test-soap          # pytest suite in a throwaway container (35 tests)
make contract-freeze    # re-capture contract/OrderManagement.wsdl after a WSDL change
```

The golden WSDL artifact at `soap-service/contract/OrderManagement.wsdl` is the frozen
contract; the WSDL drift test fails if the served WSDL ever diverges from it.

### Source 2: file-drop

Nightly CSV feed generator ([ADR-004](DECISIONS/adr-004-file-drop.md)) writing
`customers-<date>.csv` (5,000 rows, PII-carrying) and `products-<date>.csv` (2,000 rows,
OLTP-coherent SKUs/prices) into an SFTP-style drop volume, with seeded, classified dirt:
~2% verbatim duplicate rows, ragged columns (short *and* long), one cp1252-encoded row
inside the UTF-8 file, and `--late-offset` backdating for late-arrival simulation.
Same inputs give byte-identical files (replayable extractor bugs).

```bash
make drop-generate       # emit today's feeds with all dirt modes
make drop-generate-late  # late-arrival simulation (backdated 2 days)
make drop-ls             # inspect the drop volume
make test-drop           # 15 dirt-classification/CLI tests
```

### Source 3: rest-mock

Mock "Pricing & Promotions" API ([ADR-003](DECISIONS/adr-003-rest-mock.md)): FastAPI at
`http://localhost:8001` with opaque-cursor keyset pagination (`/promotions`,
`/products`), per-API-key token-bucket rate limiting (429 + `Retry-After`,
`X-RateLimit-Remaining`), and deterministic seeded flaky 500s (`retryable: true` body).
Prices are integer cents and coherent with the OLTP item-price formula. `/health` is
never limited and never flakes.

```bash
make test-rest      # 34 pytest tests (pagination walk, bucket math, flake determinism)
make smoke-rest     # walks ALL 2,500 promotions, retrying real 429s/500s
```

### Source 4: oltp

The "modern" OLTP source ([ADR-002](DECISIONS/adr-002-oltp-source.md)): normalized
`users / orders / order_items / payments` schema applied by a seeder (never by rebuilding
the volume), **5.4M rows** loaded via a single atomic COPY transaction, and a
continuous mutation loop (status walks, logins, bounded inserts/deletes) generating
measured WAL churn for Debezium CDC.

```bash
make seed-oltp      # idempotent seed + self-healing constraints (skip if already seeded)
make oltp-status    # row counts, on-disk sizes, 15s measured WAL delta
make test-oltp      # 26 pytest tests against a dedicated oltp_test db
make reseed-oltp    # DESTRUCTIVE (OLTP source only): truncate + reload
```

## Repository layout

```
docker-compose.yml     platform definition
Makefile               every target: `make help`
infra/warehouse/init/  warehouse bootstrap SQL (raw/staging/marts schemas)
scripts/               wait-healthy, smoke-test, verify targets, chaos + bench suites
soap-service/          legacy SOAP OrderManagement: spyne, basic auth, deterministic 3y seed,
                       zeep smoke client, frozen WSDL contract
rest-mock/             flaky REST pricing API: cursor paging, 429s, seeded 500s
file-drop/             nightly CSV drop zone + dirty-file generator
oltp/                  OLTP schema + 5.4M-row COPY seeder + mutation loop
ingest/                batch extractors: watermark cursors, Retry-After backoff,
                       content-hash idempotent raw landing, quarantine
cdc-sink/              the CDC consumer: lsn-guarded idempotent upserts + control plane
dags/                  Airflow DAGs
dbt/                   dbt project: 14 table models (staging + marts) + the SCD2
                       snapshot, PII hashing (SHA-256 + env salt), 144 tests
dq/                    Great Expectations suites + dead-letter quarantine + replay (32 tests)
observability/         statsd-exporter mapping, Prometheus scrape+rules,
                       Grafana datasources+dashboard - all as code
DECISIONS/             ADRs - why the platform looks like this
docs/                  RUNBOOK.md (operate + recovery drill), DATA_DICTIONARY.md (schemas +
                       PII), DESIGN_NOTES.md (design Q&A), index.html
screenshots/           real captures of the live UIs (see above)
```

## Operating the platform

See [docs/RUNBOOK.md](docs/RUNBOOK.md) - start/stop, health, logs, destructive ops,
the recovery drill, troubleshooting, and the rootless-Docker bootstrap used on the
machine this was built on. The design reasoning lives in
[docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md) and the [ADRs](DECISIONS/).
