# HELIOS — platform targets. `make help` lists them all.
SHELL := /bin/bash
COMPOSE := docker compose
ENV_FILE := .env

# Absolute repo path, exported for docker compose (ADR-010 D1): the scheduler
# mounts the repo read-only at exactly this path so compose commands run inside
# the scheduler resolve relative bind-mount sources to the same host paths
# compose-on-host computes (bind-source parity). Also what DAG tasks `cd` into.
export HELIOS_PROJECT_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

.DEFAULT_GOAL := help
.PHONY: help env up down ps logs smoke-test test-soap smoke-soap seed-soap reseed-soap contract-freeze seed-oltp reseed-oltp oltp-status test-oltp mutator-logs test-rest smoke-rest test-drop drop-generate drop-generate-late drop-ls cdc-setup cdc-status cdc-verify test-cdc ingest-soap ingest-file ingest-rest ingest-all ingest-status test-ingest dbt-image dbt-build dbt-test dbt-freshness dq-image dq-run dq-replay dq-status test-dq lineage-verify metrics-verify metrics-drill chaos-test bench airflow-image run-etl backfill airflow-logs clean

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: $(ENV_FILE) ## Create .env from .env.example if missing

$(ENV_FILE):
	@cp .env.example $@
	@echo "[env] created $(ENV_FILE) from .env.example (local-dev defaults)"

up: env ## Start the platform and wait until every container is healthy
	$(COMPOSE) up -d
	@bash scripts/wait-healthy.sh

down: ## Stop the platform (named data volumes are preserved)
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow logs from all services
	$(COMPOSE) logs -f --tail 100

smoke-test: env ## Infra health checks (Stage 1) + LOUD expected failure on E2E (until Phase 3)
	@bash scripts/smoke-test.sh; rc=$$?; echo "[smoke-test] exit code: $$rc"; exit $$rc

test-soap: ## Run soap-service unit tests in a throwaway container
	docker compose run --rm -e HELIOS_AUTO_SEED=false soap-service pytest

smoke-soap: ## zeep round-trip smoke client against the running soap-service
	docker compose exec -T soap-service python /app/smoke_client.py

seed-soap: ## Seed SOAP order history if the store is empty (idempotent)
	docker compose exec -T soap-service python -m app.seed --if-empty --report

reseed-soap: ## DESTRUCTIVE (SOAP store only): drop + reseed SOAP order history
	docker compose exec -T soap-service python -m app.seed --reset --report

contract-freeze: ## Capture the served WSDL as the golden contract artifact
	docker compose exec -T soap-service python /app/check_contract.py --emit --out /app/contract/OrderManagement.wsdl

seed-oltp: ## Apply OLTP schema + seed 5M+ rows if empty (idempotent, self-healing; ADR-002)
	docker compose run --rm oltp-seed

reseed-oltp: ## DESTRUCTIVE (OLTP source only): truncate + reseed the OLTP database
	docker compose run --rm oltp-seed python -m oltp.seed --reset --report

oltp-status: ## OLTP row counts, on-disk sizes, measured WAL churn (15s window)
	docker compose run --rm oltp-seed python -m oltp.status --wal-window 15

test-oltp: ## Run oltp seeder/mutator tests in a throwaway container (dedicated oltp_test db)
	docker compose run --rm oltp-seed pytest

mutator-logs: ## Follow the oltp-mutator mutation-loop logs
	$(COMPOSE) logs -f --tail 50 oltp-mutator

test-rest: ## Run rest-mock unit tests in a throwaway container
	docker compose run --rm rest-mock pytest

smoke-rest: ## Walk ALL /promotions pages against the running rest-mock (retries 429/500)
	docker compose exec -T rest-mock python /app/smoke_client.py

drop-generate: ## Emit today's nightly CSV feeds (customers+products, all dirt modes) into the drop volume
	docker compose run --rm filedrop-tools

drop-generate-late: ## Late-arrival simulation: emit feeds backdated by 2 days
	docker compose run --rm filedrop-tools python -m filedrop.generate --late-offset 2 --report

drop-ls: ## List the SFTP-style drop volume (files, sizes, arrival times)
	docker compose run --rm filedrop-tools python -m filedrop.generate --list

test-drop: ## Run file-drop dirt/CLI tests in a throwaway container
	docker compose run --rm filedrop-tools pytest

cdc-setup: ## CDC bootstrap: wal_level=logical on oltp-db + replication role (idempotent; ADR-005)
	bash scripts/cdc-setup.sh

cdc-status: ## CDC control plane: connector state, sink lag, slot retention, raw counts
	docker compose run --rm cdc-sink python -m cdc.status

cdc-verify: ## ADR-005 DoD verification: baseline match, marker latency, replay safety (stops mutator temporarily)
	bash scripts/cdc-verify.sh

test-cdc: ## Run cdc-sink tests in a throwaway container (dedicated cdc_test db on warehouse-db)
	docker compose run --rm cdc-sink pytest

ingest-soap: ## One-shot SOAP extract: windowed GetOrders pull (ADR-006 watermark)
	docker compose run --rm ingest python -m ingest.run --source soap

ingest-file: ## One-shot file-drop extract: CSV feeds with quarantine (ADR-006/007)
	docker compose run --rm ingest python -m ingest.run --source file

ingest-rest: ## One-shot REST extract: full cursor walk of products+promotions
	docker compose run --rm ingest python -m ingest.run --source rest

ingest-all: ## One-shot run of all three extractors
	docker compose run --rm ingest

ingest-status: ## Watermarks, landed counts, file ledger, quarantine, run ledger
	docker compose run --rm ingest python -m ingest.status

test-ingest: ## Run ingest lib tests in a throwaway container (dedicated ingest_test db)
	docker compose run --rm ingest pytest

# --- Phase 3: dbt staging (ADR-008). One-shot tool image, baked-in code: every
# target rebuilds the image first (cached no-op when unchanged) so a project
# edit can never be missed by a run.
dbt-image: ## Build the dbt tool image (pinned dbt-core/dbt-postgres 1.9.x)
	docker compose build dbt

dbt-build: dbt-image ## dbt build: staging models + tests (shared per-run CDC cut)
	docker compose run --rm dbt build

dbt-test: dbt-image ## dbt test: the staging test contract, standalone
	docker compose run --rm dbt test

dbt-freshness: dbt-image ## dbt source freshness (per-cadence thresholds, ADR-008 D4)
	docker compose run --rm dbt source freshness

# --- Phase 3 item 9: Airflow orchestration (ADR-010) -------------------------
airflow-image: ## Build the extended airflow image (base + host compose plugin) and re-own the logs volume
	bash scripts/airflow-image.sh

run-etl: ## Trigger the master daily_close DAG and tail it to completion (nonzero on failure; ADR-010 D4)
	bash scripts/run-etl.sh

backfill: ## Honest replay-based backfill: full daily_close replay + ledger proof (ADR-010 D5)
	bash scripts/backfill.sh

airflow-logs: ## Follow airflow scheduler + webserver logs
	$(COMPOSE) logs -f --tail 100 airflow-scheduler airflow-webserver

# --- Phase 4 item 10: Great Expectations gate (ADR-011). One-shot tool image,
# code baked in: every target rebuilds the image first (cached no-op when
# unchanged) like the dbt targets.
dq-image: ## Build the dq tool image (pinned great-expectations 1.22.0)
	docker compose build dq

dq-run: dq-image ## Run the GE gate over frozen staging/marts; dead-letters failures; nonzero exit on any failure
	docker compose run --rm dq python -m dq gate

dq-replay: dq-image ## Resolve open dead-letter incidents that no longer reproduce (nonzero if any remain open)
	docker compose run --rm dq python -m dq replay

dq-status: dq-image ## Report dq.dq_quarantine status (open/resolved incidents)
	docker compose run --rm dq python -m dq status

test-dq: ## Run dq unit tests in a throwaway container
	docker compose run --rm dq python -m pytest

# --- Phase 4 item 11: OpenLineage + Marquez (ADR-012). The emitters are
# wired in compose (airflow env + the dbt-ol entrypoint); this is the
# verification target: it asserts MEASURED events via the Marquez REST API
# (jobs incl. daily_close's tasks, the raw->staging->marts dataset graph,
# column-level lineage into a mart) and dumps the API JSON to EVIDENCE/.
lineage-verify: ## Prove measured lineage via the Marquez API; dump evidence JSON (nonzero on any missing event)
	bash scripts/lineage-verify.sh

# --- Phase 4 item 12: Prometheus + Grafana (ADR-013). The metrics stack is
# wired in compose + observability/ (dashboards, datasources, scrape config,
# rules and the exporter mapping are CODE). metrics-verify asserts MEASURED
# scrapes/rules/metric-names/values and the Grafana provisioning via the
# APIs (non-destructive, dumps EVIDENCE); metrics-drill is the scripted
# alert fire-drill (stop a scraped target -> rule fires -> restart ->
# recovery). The non-fatal drills are documented in RUNBOOK + EVIDENCE.
metrics-verify: ## Prove measured metrics via the Prometheus/Grafana APIs; dump evidence (nonzero on any missing surface)
	bash scripts/metrics-verify.sh

metrics-drill: ## Alert fire-drill: stop statsd-exporter, assert HeliosScrapeTargetDown fires, restart, assert recovery
	bash scripts/metrics-drill.sh

# --- Phase 5 item 13: chaos-test (ADR-014). Scripted destructive scenarios
# (kill worker mid-DAG, kill DB mid-load, poison CDC/CSV, schema drift, API
# outage) — each asserts the platform DEGRADES SAFELY and ends with a
# measured convergence proof; full transcripts land in EVIDENCE/chaos-*.log.
# Destructive BY DESIGN and re-runnable (ADR-014 D7); smoke-test stays the
# green-state guard and does not grow. One scenario: make chaos-test SCENARIO=kill_worker
chaos-test: ## Chaos drills (all 7, ~60-90 min) or one: SCENARIO=01|kill_worker|poison_cdc|...
	bash scripts/chaos-test.sh $(SCENARIO)

# --- Phase 5 item 14: bench (ADR-015). One measured pass of the REAL pipeline
# per stage — SOAP full-walk read, CDC applied-drain on the live mutator, the
# dbt full re-materialization (~14M rows; the "5M-row full load"), the GE gate
# scan, and one orchestrated daily_close end-to-end — each leg's transcript in
# EVIDENCE/bench-<label>-<leg>.log; EVIDENCE/metrics.md is the roll-up (built
# by hand from the logs, every number traceable). Read-only platform-side:
# no reseed, no mutator pause, no chaos acts; smoke-test does not grow.
# Default label = UTC timestamp (repeated passes never overwrite).
bench: ## Timed full-load + per-stage rows/sec -> EVIDENCE/bench-*.log (ADR-015); label a pass with BENCH_LABEL=
	bash scripts/bench/bench.sh $(BENCH_LABEL)

clean: ## DESTRUCTIVE: stop everything and delete all data volumes
	$(COMPOSE) down -v
