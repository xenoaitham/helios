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
echo "[FAIL] NOT IMPLEMENTED — E2E assertions (seed -> ingest -> dbt -> mart row-count + SCD2"
echo "       checks via SQL) arrive with Phase 3's 'make run-etl'."
echo "       This loud failure is the EXPECTED Phase 0 behaviour by definition-of-done;"
echo "       see STATE.md and BACKLOG.md."
exit 1
