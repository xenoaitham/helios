# HELIOS — platform targets. `make help` lists them all.
SHELL := /bin/bash
COMPOSE := docker compose
ENV_FILE := .env

.DEFAULT_GOAL := help
.PHONY: help env up down ps logs smoke-test test-soap smoke-soap seed-soap reseed-soap contract-freeze seed-oltp reseed-oltp oltp-status test-oltp mutator-logs test-rest smoke-rest test-drop drop-generate drop-generate-late drop-ls cdc-setup cdc-status cdc-verify test-cdc ingest-soap ingest-file ingest-rest ingest-all ingest-status test-ingest dbt-image dbt-build dbt-test dbt-freshness clean

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

clean: ## DESTRUCTIVE: stop everything and delete all data volumes
	$(COMPOSE) down -v
