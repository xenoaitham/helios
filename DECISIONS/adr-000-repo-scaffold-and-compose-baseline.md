# ADR-000 — Repository scaffold and compose baseline

- Status: Accepted (Phase 0, Session 1 — 2026-09-08)
- Phase: 0 (Scaffold)

## Context

HELIOS starts from an empty repo on a single dev machine (rootless Docker, no sudo, 12
vCPU / 16 GB RAM, ~11 GB free disk). Phase 0 must deliver a reproducible baseline:
`make up` → healthy containers; `make smoke-test` → fails loudly (ETL does not exist
yet). Later phases extend this file rather than replace it.

## Options

1. **Bare-metal installs** (postgres, kafka, airflow on host) — fastest to start,
   impossible to hand a reviewer a one-command repro; pollutes host. Rejected.
2. **Kubernetes (minikube/k3s)** — realistic for prod, but heavy on a 99%-full disk and
   slow to iterate; the portfolio story is data engineering, not cluster ops. Rejected
   for now; noted as the 100× path in known-limitations.
3. **Docker Compose** — single-file topology, one-command up/down, healthchecks wired to
   `make` targets, easy for an interviewer to run. **Chosen.**

## Decision

Compose baseline with these services (see `docker-compose.yml`):

- **oltp-db** — `postgres:16-alpine`. The legacy OLTP source (schema + 5M-row seed in
  Phase 1). Host port 5432.
- **warehouse-db** — `postgres:16-alpine` with `raw`/`staging`/`marts` schemas created by
  `infra/warehouse/init/01-schemas.sql` on first volume init. Host port 5433.
  Rationale for Postgres-as-warehouse: dbt support is first-class, SQL dialect matches
  the sources, zero extra licensing; the "lakehouse-style zones" concept survives a
  later move to MaxCompute/Snowflake (see INTERVIEW_DEFENSE AliCloud mapping).
- **airflow-db** — separate Postgres for Airflow metadata. This is infrastructure state,
  deliberately NOT one of the two data Postgres instances, so platform rebuilds
  (`make clean`) never depend on analytics data surviving.
- **kafka** — `apache/kafka:3.9.0`, KRaft combined broker+controller (no ZooKeeper),
  dual listeners: PLAINTEXT://kafka:9092 for in-network clients (Debezium, sinks —
  Phase 2), HOST://localhost:29092 for host tooling. Heap capped 512M for this host.
  Chose the official `apache/kafka` image over bitnami (upstream/community direction
  of travel, avoids third-party image deprecation risk).
- **airflow-init / airflow-webserver / airflow-scheduler** — `apache/airflow:2.10.5`
  (python3.11), LocalExecutor. Airflow 2.x not 3.x: the OpenLineage integration, docs,
  and DAG patterns targeted by this project are mature on 2.10; revisit 3.x only if a
  needed feature appears. One-shot init container runs db migrate + admin user creation
  via the image's supported env hooks.

Cross-cutting:

- All config via `.env` (committed `.env.example`, gitignored `.env`); compose variable
  defaults match the example so a missing `.env` still boots a dev stack.
- Every long-running service has a real healthcheck; `make up` blocks on
  `scripts/wait-healthy.sh` until all report healthy (fail fast with container logs).
- Explicit `container_name: helios-*` for stable scripting; no scaling intended.

## Consequences

- **Positive**: one-command repro; Phase 1+ work is additive (services + make targets);
  failure semantics visible early (wait script dumps logs on timeout).
- **Negative / accepted risks**:
  - Single-broker Kafka, single-node everything: no HA. Fine for the portfolio scale;
    documented as a known limit.
  - Rootless Docker on the dev machine means no cgroup resource limits; compose
    `restart: unless-stopped` relies on dockerd-level restart, not systemd.
  - Image tags float on minor versions (`postgres:16-alpine`); pin digests if this ever
    leaves local dev.
  - `initdb` scripts only run on an empty volume — re-initialising requires
    `make clean` (destructive) — documented in RUNBOOK.
