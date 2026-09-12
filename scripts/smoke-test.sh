#!/usr/bin/env bash
# HELIOS smoke test.
#
# Stage 1 (infra): every container healthy AND answering a real query
#                  (SQL on each Postgres, topic list on Kafka, /health on Airflow,
#                  authenticated WSDL + enforced 401 on soap-service; Phase 2 adds
#                  wal_level=logical + the Debezium connector state — ADR-005).
# Stage 2 (E2E):  seed -> run-etl -> mart/SCD2 SQL assertions.
#                 NOT IMPLEMENTED until Phase 3 — Stage 2 must fail LOUDLY today;
#                 that is the expected behaviour by definition-of-done (STATE.md).
set -uo pipefail
cd "$(dirname "$0")/.."
# Bind-source parity (ADR-010 D1) — the compose exec below parses
# docker-compose.yml, whose scheduler mount needs this var.
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"

set -a; [ -f .env ] && . ./.env; set +a
OLTP_USER="${OLTP_POSTGRES_USER:-oltp}";   OLTP_DB="${OLTP_POSTGRES_DB:-oltp}"
WH_USER="${WAREHOUSE_POSTGRES_USER:-warehouse}"; WH_DB="${WAREHOUSE_POSTGRES_DB:-warehouse}"

STAGE1_FAIL=0

echo "===== Stage 1: infrastructure ====="

for svc in oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler soap-service cdc-connect cdc-sink; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "helios-${svc}" 2>/dev/null || echo missing)"
  if [ "$status" = "healthy" ]; then
    echo "[ok]   container $svc healthy"
  else
    echo "[FAIL] container $svc health=$status"
    STAGE1_FAIL=1
  fi
done

if docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "SELECT 1" >/dev/null 2>&1; then
  echo "[ok]   oltp-db answers SQL"
else
  echo "[FAIL] oltp-db does not answer SQL"; STAGE1_FAIL=1
fi

if docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc "SELECT 1" >/dev/null 2>&1; then
  echo "[ok]   warehouse-db answers SQL"
else
  echo "[FAIL] warehouse-db does not answer SQL"; STAGE1_FAIL=1
fi

schema_count="$(docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc \
  "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('raw','staging','marts')" 2>/dev/null || echo 0)"
if [ "$schema_count" = "3" ]; then
  echo "[ok]   warehouse schemas raw/staging/marts exist"
else
  echo "[FAIL] warehouse schemas: found $schema_count of 3"; STAGE1_FAIL=1
fi

if docker exec helios-airflow-db psql -U airflow -d airflow -tAc "SELECT 1" >/dev/null 2>&1; then
  echo "[ok]   airflow-db answers SQL"
else
  echo "[FAIL] airflow-db does not answer SQL"; STAGE1_FAIL=1
fi

if docker exec helios-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  echo "[ok]   kafka answers topic list"
else
  echo "[FAIL] kafka does not answer topic list"; STAGE1_FAIL=1
fi

if docker exec helios-airflow-webserver curl -sf http://localhost:8080/health 2>/dev/null | grep -q healthy; then
  echo "[ok]   airflow /health reports healthy"
else
  echo "[FAIL] airflow /health endpoint"; STAGE1_FAIL=1
fi

if docker exec helios-soap-service python /app/check_contract.py >/dev/null 2>&1; then
  echo "[ok]   soap-service serves the OrderManagement WSDL (basic auth)"
else
  echo "[FAIL] soap-service WSDL contract check"; STAGE1_FAIL=1
fi

if docker exec helios-soap-service python /app/check_contract.py --expect-unauthorized >/dev/null 2>&1; then
  echo "[ok]   soap-service rejects unauthenticated requests (401)"
else
  echo "[FAIL] soap-service auth not enforced"; STAGE1_FAIL=1
fi

# --- Phase 2 (ADR-005): CDC capture path ---
wal_level="$(docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "SHOW wal_level" 2>/dev/null || echo unknown)"
if [ "$wal_level" = "logical" ]; then
  echo "[ok]   oltp-db wal_level=logical (CDC capture enabled)"
else
  echo "[FAIL] oltp-db wal_level=$wal_level (expected logical — run make cdc-setup)"; STAGE1_FAIL=1
fi

if docker exec helios-cdc-sink python -c "import urllib.request,json,sys;r=json.load(urllib.request.urlopen('http://cdc-connect:8083/connectors/helios-oltp/status',timeout=5));sys.exit(0 if r.get('connector',{}).get('state')=='RUNNING' else 1)" >/dev/null 2>&1; then
  echo "[ok]   Debezium connector helios-oltp RUNNING"
else
  echo "[FAIL] Debezium connector helios-oltp not RUNNING (see make cdc-status)"; STAGE1_FAIL=1
fi

echo
if [ "$STAGE1_FAIL" -ne 0 ]; then
  echo "Stage 1 FAILED — platform not healthy. Try: make down && make up"
  exit 2
fi
echo "Stage 1 PASSED — all infrastructure containers healthy and answering queries."

echo
echo "===== Stage 2: end-to-end ETL assertions ====="
# ADR-010 D6: a REAL orchestrated run (the same daily_close path production
# uses), then the published-layer contract asserted against the live warehouse
# via psql single-line static SQL. Between the dbt build and these assertions
# the only writer is the live CDC sink — staging/marts are frozen tables, so
# the parity counts are stable in this window.
STAGE2_FAIL=0

if bash scripts/run-etl.sh; then
  echo "[ok]   daily_close DAG run succeeded (ingest_file → ingest_soap ∥ ingest_rest → dbt build)"
else
  echo "[FAIL] daily_close DAG run did not succeed (see [run-etl] output above)"; STAGE2_FAIL=1
fi

import_errs="$(docker compose exec -T airflow-scheduler bash -c 'airflow dags list-import-errors --output json | python3 -c "import json,sys; print(len(json.load(sys.stdin)))"' 2>/dev/null || echo parse_error)"
if [ "$import_errs" = "0" ]; then
  echo "[ok]   airflow DAG import errors: 0"
else
  echo "[FAIL] airflow DAG import errors: $import_errs"; STAGE2_FAIL=1
fi

wh() { docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc "$1"; }

# parity_check NAME SQL — SQL returns "left|right"; ok iff equal; prints both.
parity_check() {
  local name="$1" sql="$2" row left right
  row="$(wh "$sql")" || { echo "[FAIL] $name: query error"; STAGE2_FAIL=1; return; }
  left="${row%%|*}"; right="${row#*|}"
  if [ "$left" = "$right" ]; then
    echo "[ok]   $name: $left = $right"
  else
    echo "[FAIL] $name: $left <> $right"; STAGE2_FAIL=1
  fi
}

# zero_check NAME SQL — SQL returns one count; ok iff 0; prints the count.
zero_check() {
  local name="$1" sql="$2" n
  n="$(wh "$sql")" || { echo "[FAIL] $name: query error"; STAGE2_FAIL=1; return; }
  if [ "$n" = "0" ]; then
    echo "[ok]   $name (violations: 0)"
  else
    echo "[FAIL] $name: $n violations"; STAGE2_FAIL=1
  fi
}

# --- mart row-count parity vs staging (mirrors the dbt marts_row_parity contract) ---
parity_check "fct_orders rows = stg_orders + stg_soap_orders" \
  "SELECT (SELECT count(*) FROM marts.fct_orders), (SELECT count(*) FROM staging.stg_orders) + (SELECT count(*) FROM staging.stg_soap_orders)"
parity_check "fct_order_items rows = stg_order_items" \
  "SELECT (SELECT count(*) FROM marts.fct_order_items), (SELECT count(*) FROM staging.stg_order_items)"
parity_check "dim_product rows = distinct(rest ∪ file) SKUs" \
  "SELECT (SELECT count(*) FROM marts.dim_product), (SELECT count(*) FROM (SELECT sku FROM staging.stg_rest_products UNION SELECT sku FROM staging.stg_file_products) catalog)"
parity_check "dim_customer current rows = stg_users" \
  "SELECT (SELECT count(*) FROM marts.dim_customer WHERE is_current), (SELECT count(*) FROM staging.stg_users)"

# --- SCD2 contract (mirrors scd2_current_uniqueness / scd2_window_integrity / fct_customer_scope) ---
zero_check "dim_customer: exactly one current row per customer" \
  "SELECT count(*) FROM (SELECT customer_id FROM marts.dim_customer WHERE is_current GROUP BY customer_id HAVING count(*) <> 1) v"
zero_check "dim_customer: SCD2 windows contiguous (no gaps/overlaps/multi-open)" \
  "SELECT count(*) FROM (SELECT valid_from, valid_to, lead(valid_from) over (partition by customer_id order by valid_from) AS next_from FROM marts.dim_customer) w WHERE (valid_to IS NOT NULL AND valid_to <= valid_from) OR (valid_to IS NOT NULL AND next_from IS NOT NULL AND valid_to <> next_from) OR (valid_to IS NULL AND next_from IS NOT NULL)"
zero_check "fct_orders: SOAP rows keep the unknown member (customer_sk IS NULL)" \
  "SELECT count(*) FROM marts.fct_orders WHERE source_type='soap' AND customer_sk IS NOT NULL"
zero_check "fct_orders: OLTP rows all resolve a customer version" \
  "SELECT count(*) FROM marts.fct_orders WHERE source_type='oltp' AND customer_sk IS NULL"

echo
if [ "$STAGE2_FAIL" -ne 0 ]; then
  echo "Stage 2 FAILED — orchestration and/or published-layer contract broken (see [FAIL] lines above)."
  exit 1
fi
echo "Stage 2 PASSED — orchestrated ETL green; mart parity + SCD2 contract verified on the live warehouse."
exit 0
