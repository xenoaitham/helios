# HELIOS — platform targets. `make help` lists them all.
SHELL := /bin/bash
COMPOSE := docker compose
ENV_FILE := .env

.DEFAULT_GOAL := help
.PHONY: help env up down ps logs smoke-test test-soap smoke-soap seed-soap reseed-soap contract-freeze clean

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

clean: ## DESTRUCTIVE: stop everything and delete all data volumes
	$(COMPOSE) down -v
