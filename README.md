# HELIOS — Legacy-to-Lakehouse ETL Modernization Platform

> A containerized enterprise data platform that modernizes legacy SOAP services, flat-file
> batch feeds, REST APIs, and OLTP database changes (CDC) into a governed dimensional
> warehouse — with orchestrated ETL, data-quality gates, lineage tracking, and full
> observability.

**This is a personal portfolio project** (not employment work). Every claim in this README
maps to a `make` target or a file you can check. Every number in the docs comes from a
measured run recorded in [`EVIDENCE/`](EVIDENCE/).

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Scaffold: compose baseline, Makefile, state files, ADR-000 | ✅ done — [EVIDENCE/phase-0.md](EVIDENCE/phase-0.md) |
| 1 | Sources: SOAP service, REST mock, dirty file feeds, OLTP seeder | ⬜ |
| 2 | Movement: Debezium CDC → Kafka → raw zone; batch extractors | ⬜ |
| 3 | Warehouse & dbt: star schema, SCD2, Airflow DAGs, `make run-etl` | ⬜ |
| 4 | Trust & observability: Great Expectations gates, Marquez lineage, Prometheus/Grafana | ⬜ |
| 5 | Chaos & performance: `make chaos-test`, `make bench` | ⬜ |
| 6 | Package: RUNBOOK, data dictionary, interview defense pack | ⬜ |

Current state: see [STATE.md](STATE.md) (always current) and [BACKLOG.md](BACKLOG.md).

## Quickstart (Phase 0 baseline)

```bash
make up          # start + wait until every container is healthy
make ps          # all containers healthy
make smoke-test  # Phase 0: infra checks PASS, E2E E2E checks fail LOUDLY by design (exit 1)
make down        # stop (data volumes preserved)
```

Measured on the dev machine (12 vCPU, rootless Docker, see `EVIDENCE/phase-0.md`):
cold `make up` after images are present ≈ 80 s. First run additionally downloads
images once (~3.2 GB: Airflow 2.14 GB + Kafka 628 MB + Postgres 420 MB).

First run creates `.env` from `.env.example` (local-dev defaults) automatically.

### What is running after Phase 0

| Service | URL / conn | Credentials (.env) |
|---|---|---|
| Airflow UI | http://localhost:8080 | `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD` |
| OLTP Postgres (source) | localhost:5432, db `oltp` | `OLTP_POSTGRES_USER` / `OLTP_POSTGRES_PASSWORD` |
| Warehouse Postgres | localhost:5433, db `warehouse` (schemas `raw`, `staging`, `marts`) | `WAREHOUSE_POSTGRES_USER` / `WAREHOUSE_POSTGRES_PASSWORD` |
| Kafka (host listener) | localhost:29092 | n/a (PLAINTEXT, local dev) |

`make smoke-test` exits non-zero on any health mismatch; Stage 2 (ETL assertions) is
intentionally unimplemented until Phase 3 — this loud failure is the Phase 0 definition
of done.

## Architecture (target — components annotated with the phase that delivers them)

```mermaid
flowchart LR
    subgraph SRC["Legacy sources"]
        SOAP["soap-service (Ph.1)<br/>WSDL-first OrderManagement<br/>spyne - SOAP 1.1 - basic auth"]
        FF["file-drop (Ph.1)<br/>nightly CSV feeds<br/>dupes - ragged rows - bad encoding"]
        REST["rest-mock (Ph.1)<br/>Pricing and Promotions API<br/>pagination - rate limits - flaky 500s"]
        OLTP[("oltp Postgres (Ph.1)<br/>users - orders - items - payments<br/>5M+ rows - continuous mutations")]
    end

    subgraph MOVE["Ingestion and movement (Ph.2)"]
        DEB["Debezium CDC"]
        KAFKA[("Kafka KRaft<br/>single broker")]
        ING["ingest lib<br/>watermarks - retry/backoff"]
    end

    subgraph WH["Warehouse Postgres"]
        RAW[("raw schema")]
        STG[("staging schema<br/>PII hash/mask")]
        MART[("marts schema<br/>star schema")]
    end

    ORCH["Airflow<br/>DAGs - SLA - retries (Ph.3)"]
    DBT["dbt<br/>staging to marts - SCD2 (Ph.3)"]
    GE["Great Expectations<br/>quality gates - quarantine (Ph.4)"]
    LIN["OpenLineage to Marquez (Ph.4)"]
    PROM["Prometheus + Grafana (Ph.4)"]

    SOAP --> ING
    FF --> ING
    REST --> ING
    ING --> RAW
    OLTP --> DEB --> KAFKA --> RAW
    ORCH --> ING
    ORCH --> DEB
    RAW --> DBT --> STG --> MART
    GE --> STG
    ORCH --> GE
    ORCH -.-> LIN
    PROM -.-> ORCH
```

Only the platform row (Postgres ×2 + Airflow + Kafka + Airflow metadata DB) exists today;
everything else lands in the phase shown. The diagram is the contract — each phase's
CRITIC review checks the repo against it.

## Repository layout

```
docker-compose.yml     platform definition (Phase 0 baseline)
Makefile               up / down / ps / logs / smoke-test / clean (+ env helper)
infra/warehouse/init/  warehouse bootstrap SQL (raw/staging/marts schemas)
scripts/               wait-healthy.sh, smoke-test.sh (grows per phase)
soap-service/          (Ph.1) WSDL-first legacy SOAP service
rest-mock/             (Ph.1) flaky REST pricing API
file-drop/             (Ph.1) nightly CSV drop zone + dirty-file generator
oltp/                  (Ph.1) OLTP schema + 5M-row seeder + mutation loop
ingest/                (Ph.2) shared extraction library (watermarks, retries)
dags/                  (Ph.3) Airflow DAGs
dbt/                   (Ph.3) dbt project: staging -> marts
dq/                    (Ph.4) Great Expectations suites + quarantine
observability/         (Ph.4) Prometheus/Grafana config, Marquez
DECISIONS/             ADRs — why the platform looks like this
EVIDENCE/              verifier logs + metrics; the source of every number we claim
docs/                  RUNBOOK.md, DATA_DICTIONARY.md, INTERVIEW_DEFENSE.md
STATE.md               one-screen session state; read me first
BACKLOG.md             ordered work items
```

## Operating the platform

See [docs/RUNBOOK.md](docs/RUNBOOK.md) — start/stop, health, logs, destructive ops,
troubleshooting, and the rootless-Docker bootstrap used on the dev machine this was
built on.

## License / honesty

Personal portfolio project by a Solution Architect candidate. Built to be broken into:
try `make chaos-test` (Phase 5) and read [docs/INTERVIEW_DEFENSE.md](docs/INTERVIEW_DEFENSE.md)
for known limitations — including where this design falls over at 100× scale.
