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

for svc in oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler soap-service cdc-connect cdc-sink marquez-db marquez-api marquez-web statsd-exporter prometheus grafana; do
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

# --- Phase 4 (ADR-012): the lineage store answers SQL ---
if docker exec helios-marquez-db psql -U "${MARQUEZ_POSTGRES_USER:-marquez}" -d marquez -tAc "SELECT 1" >/dev/null 2>&1; then
  echo "[ok]   marquez-db answers SQL"
else
  echo "[FAIL] marquez-db does not answer SQL"; STAGE1_FAIL=1
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

# --- Phase 4 (ADR-013): the metrics stack answers (the SQL-probe analog) ---
if curl -sf "http://localhost:${PROMETHEUS_PORT:-9091}/-/ready" 2>/dev/null | grep -q "Ready"; then
  echo "[ok]   prometheus answers /-/ready on :${PROMETHEUS_PORT:-9091}"
else
  echo "[FAIL] prometheus not answering on :${PROMETHEUS_PORT:-9091}"; STAGE1_FAIL=1
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

# --- Phase 4 (ADR-011): the semantic gate's dead-letter must carry no open
# debt after a green orchestrated run — the full poison→replay loop ends with
# this count at zero (a nonzero value here is honest: unresolved DQ debt). ---
zero_check "dq gate: no open quarantined incidents (dead-letter clean)" \
  "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL"

# --- Phase 4 (ADR-012): MEASURED lineage, asserted via the Marquez REST API.
# The orchestrated run above just emitted fresh events (the DAG's dbt_build
# runs dbt-ol; every task emits via the Airflow provider), so these assert
# what THIS run produced. A down Marquez here is a FAIL: the platform's
# declared state includes the lineage backend (the non-fatal contract is
# proven separately by the Marquez-down drill, ADR-012 D6).
# Measured namespaces (2026-09-13): Airflow JOBS land in `helios`; dbt-ol
# namespaces DATASETS by the dbt connection URI. The column-lineage endpoint
# takes a `dataset:`-prefixed nodeId and returns DATASET_FIELD nodes with
# per-node inEdges/outEdges (the format the Marquez UI itself issues). ---
MQ="http://localhost:${MARQUEZ_API_PORT:-5000}"
MQ_DS_NS='postgres%3A%2F%2Fwarehouse-db%3A5432'
# first arg: path; any further args: extra curl options (e.g. -G --data-urlencode ...)
mq() { local p="$1"; shift; curl -sf --max-time 45 "$@" "$MQ$p"; }
# max-time 45 (was 15): the helios /jobs payload grows with run history —
# measured ~16 s server-side 2026-09-15 (see lineage-verify.sh). Same checks,
# same count: latency repair only, not growth.

lineage_ok() { echo "[ok]   $1"; }
lineage_fail() { echo "[FAIL] $1"; STAGE2_FAIL=1; }

if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
  lineage_fail "host needs curl + python3 for the Marquez assertions"
else
  DATASETS_JSON="$(mq "/api/v1/namespaces/$MQ_DS_NS/datasets" || true)"
  if [ -n "$DATASETS_JSON" ] \
     && python3 -c 'import json,sys; d=json.load(sys.stdin); names=" ".join(x["name"] for x in d["datasets"]); sys.exit(0 if ("stg_orders" in names and "fct_orders" in names and "stg_payments" in names) else 1)' <<< "$DATASETS_JSON"; then
    lineage_ok "marquez: dbt dataset graph present (raw sources -> staging -> marts, connection-URI namespace)"
  else
    lineage_fail "marquez: dbt datasets missing (stg_orders/fct_orders/stg_payments) — dbt lineage events absent?"
  fi

  JOBS_JSON="$(mq /api/v1/namespaces/helios/jobs || true)"
  if [ -n "$JOBS_JSON" ] \
     && python3 -c 'import json,sys; d=json.load(sys.stdin); names=" ".join(j["name"] for j in d["jobs"]); sys.exit(0 if ("daily_close.dbt_build" in names and "daily_close.dq_gate" in names) else 1)' <<< "$JOBS_JSON"; then
    lineage_ok "marquez: daily_close task jobs present (dbt_build + dq_gate)"
  else
    lineage_fail "marquez: daily_close task jobs missing (dbt_build/dq_gate) — Airflow events absent?"
  fi

  CL_JSON="$(mq /api/v1/column-lineage -G \
    --data-urlencode 'nodeId=dataset:postgres://warehouse-db:5432:warehouse.marts.fct_orders' \
    --data-urlencode 'depth=2' --data-urlencode 'withDownstream=true' 2>/dev/null || true)"
  if [ -n "$CL_JSON" ] \
     && python3 -c 'import json,sys; g=json.load(sys.stdin); edges=[(e.get("origin") or e.get("source",""),e.get("destination","")) for n in g.get("graph",[]) for e in (n.get("inEdges") or [])+(n.get("outEdges") or []) if "warehouse.marts.fct_orders" in (e.get("destination","")+(e.get("origin") or e.get("source","")))]; sys.exit(0 if edges else 1)' <<< "$CL_JSON"; then
    lineage_ok "marquez: column-level lineage reaches marts.fct_orders (API-proven)"
  else
    lineage_fail "marquez: column-lineage graph for fct_orders empty"
  fi
fi

# --- Phase 4 (ADR-013): MEASURED metrics, asserted via the Prometheus API.
# The orchestrated run above just emitted task/dagrun events through
# StatsD -> statsd-exporter; these assert the scrape is REAL and a known
# metric exists with a real value (exactly the growth item 12 justifies —
# no wholesale additions; the lineage block above is the regression guard).
# A down Prometheus here is a FAIL: the platform's declared state includes
# the metrics stack (the non-fatal contract is proven separately by the
# metrics-stack-down drill, ADR-013 D7). ---
PROM="http://localhost:${PROMETHEUS_PORT:-9091}"
pq() { curl -sf --max-time 15 "$PROM/api/v1/query" --data-urlencode "query=$1" 2>/dev/null || true; }

metrics_ok() { echo "[ok]   $1"; }
metrics_fail() { echo "[FAIL] $1"; STAGE2_FAIL=1; }

# metrics_present NAME PY-CHECK QUERY — retry ≤30s (two scrape intervals):
# the run's terminal datagrams (e.g. the dagrun duration, emitted at run
# completion) can land after the last scrape before run-etl returns; a
# scrape-interval race is not a broken chain (measured 2026-09-13).
metrics_present() {
  local name="$1" check="$2" query="$3" json="" ok=0 n
  for n in 1 2 3 4 5 6; do
    json="$(pq "$query")"
    if [ -n "$json" ] && python3 -c "$check" <<< "$json"; then ok=1; break; fi
    sleep 5
  done
  if [ "$ok" = 1 ]; then metrics_ok "$name"; else metrics_fail "$name"; fi
}

metrics_present "prometheus scrapes statsd-exporter (up == 1)" \
  'import json,sys; r=json.load(sys.stdin)["data"]["result"]; sys.exit(0 if any(x["value"][1]=="1" for x in r) else 1)' \
  'up{job="statsd-exporter"}'

metrics_present "airflow_task_finish_total{dag_id=daily_close} present with real values (failure/success surface measured)" \
  'import json,sys; r=json.load(sys.stdin)["data"]["result"]; sys.exit(0 if r and sum(float(x["value"][1]) for x in r) >= 1 else 1)' \
  'sum(airflow_task_finish_total{dag_id="daily_close"})'

metrics_present "airflow_dagrun_duration_seconds histogram populated (duration surface measured)" \
  'import json,sys; r=json.load(sys.stdin)["data"]["result"]; sys.exit(0 if r and sum(float(x["value"][1]) for x in r) >= 1 else 1)' \
  'airflow_dagrun_duration_seconds_count{dag_id="daily_close",status="success"}'

echo
if [ "$STAGE2_FAIL" -ne 0 ]; then
  echo "Stage 2 FAILED — orchestration and/or published-layer contract broken (see [FAIL] lines above)."
  exit 1
fi
echo "Stage 2 PASSED — orchestrated ETL green; mart parity + SCD2 contract verified on the live warehouse."
exit 0
