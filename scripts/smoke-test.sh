#!/usr/bin/env bash
# HELIOS smoke test — Phase 0 edition.
#
# Stage 1 (infra): every container healthy AND answering a real query
#                  (SQL on each Postgres, topic list on Kafka, /health on Airflow).
# Stage 2 (E2E):  seed -> run-etl -> mart/SCD2 SQL assertions.
#                 NOT IMPLEMENTED until Phase 3 — Stage 2 must fail LOUDLY today;
#                 that is the Phase 0 definition of done (STATE.md, BACKLOG.md).
set -uo pipefail
cd "$(dirname "$0")/.."

set -a; [ -f .env ] && . ./.env; set +a
OLTP_USER="${OLTP_POSTGRES_USER:-oltp}";   OLTP_DB="${OLTP_POSTGRES_DB:-oltp}"
WH_USER="${WAREHOUSE_POSTGRES_USER:-warehouse}"; WH_DB="${WAREHOUSE_POSTGRES_DB:-warehouse}"

STAGE1_FAIL=0

echo "===== Stage 1: infrastructure ====="

for svc in oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler; do
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
