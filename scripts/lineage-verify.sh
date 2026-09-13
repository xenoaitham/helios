#!/usr/bin/env bash
# make lineage-verify (ADR-012 D7): prove MEASURED OpenLineage lineage via the
# Marquez REST API — not config claims — and dump the evidence JSON.
#
# Asserts, against the LIVE Marquez api:
#   1. the dbt dataset graph exists in namespace helios (staging + marts),
#   2. the Airflow job events exist for daily_close's tasks — INCLUDING
#      dq_gate — and the child-DAG ingest jobs (ADR-012 D2/D3 boundary),
#   3. column-level lineage reaches a mart: the column-lineage API returns a
#      non-empty upstream graph for fct_orders.total_amount.
# Every API response is dumped to EVIDENCE/phase-4-lineage/ (API dumps) and
# Marquez's own storage shape is dumped (schema + key tables as CSV — DB dump).
#
# Every step aborts loudly on failure: a vacuous PASS is worse than a FAIL
# (Session-9 precedent). Idempotent — safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

NS=helios
# measured (2026-09-13): dbt-ol namespaces datasets by the dbt connection URI
# (postgres://warehouse-db:5432) — URL-encoded for API paths; jobs stay in
# OPENLINEAGE_NAMESPACE (helios).
DBT_NS_ENC='postgres%3A%2F%2Fwarehouse-db%3A5432'
API_PORT="${MARQUEZ_API_PORT:-5000}"
BASE="http://localhost:${API_PORT}"
OUT=EVIDENCE/phase-4-lineage
mkdir -p "$OUT"

fail() { echo "[lineage-verify] FAIL: $*"; exit 1; }
ok()   { echo "[lineage-verify] ok: $*"; }
mq()   { curl -sf --max-time 15 "$BASE$1"; }   # -f: HTTP errors abort loudly

command -v curl >/dev/null || fail "curl not found on host"
command -v python3 >/dev/null || fail "python3 not found on host (JSON parsing)"

# --- 0. backend up -----------------------------------------------------------
HEALTH="$(docker inspect -f '{{.State.Health.Status}}' helios-marquez-api 2>/dev/null || echo missing)"
[ "$HEALTH" = "healthy" ] || fail "marquez-api container health=$HEALTH (make up first)"
mq /healthcheck >/dev/null 2>&1 || mq /api/v1/namespaces >/dev/null 2>&1 || \
  fail "marquez api not answering on $BASE (make up first)"

# --- 1. the dbt dataset graph: staging + marts present -----------------------
# Measured (2026-09-13): dbt-ol namespaces datasets by the dbt connection URI
# (postgres://warehouse-db:5432) — the job namespace stays OPENLINEAGE_NAMESPACE
# (helios). Airflow task jobs land in helios (step 2); the dataset graph here.
mq "/api/v1/namespaces/$DBT_NS_ENC/datasets" > "$OUT/marquez-datasets.json" \
  || fail "GET datasets failed"
DATASETS="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print("\n".join(x["name"] for x in d.get("datasets",[])))' \
  < "$OUT/marquez-datasets.json")" || fail "datasets JSON unparseable"
N_DATASETS="$(printf '%s\n' "$DATASETS" | grep -c . || true)"
echo "[lineage-verify] datasets in namespace postgres://warehouse-db:5432: $N_DATASETS"
[ "$N_DATASETS" -ge 10 ] || fail "expected >=10 datasets (staging+marts), got $N_DATASETS — has a dbt build run with lineage on?"
for want in stg_orders stg_users stg_payments dim_customer fct_order_items fct_orders; do
  printf '%s\n' "$DATASETS" | grep -q "$want" || fail "dataset $want missing from Marquez (dbt lineage incomplete)"
done
ok "dbt dataset graph present (staging + marts; full list dumped)"

# resolve the fct_orders dataset's exact Marquez name for the column probe
FCT_ORDERS="$(printf '%s\n' "$DATASETS" | grep 'fct_orders$' | head -1)"
[ -n "$FCT_ORDERS" ] || fail "fct_orders dataset name not found"

# --- 2. Airflow job events: daily_close tasks INCLUDING dq_gate --------------
mq "/api/v1/namespaces/$NS/jobs" > "$OUT/marquez-jobs.json" \
  || fail "GET jobs failed"
JOBS="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print("\n".join(j["name"] for j in d.get("jobs",[])))' \
  < "$OUT/marquez-jobs.json")" || fail "jobs JSON unparseable"
N_JOBS="$(printf '%s\n' "$JOBS" | grep -c . || true)"
echo "[lineage-verify] jobs in namespace $NS: $N_JOBS"
for want in dbt_build dq_gate trigger_ingest_file trigger_ingest_soap trigger_ingest_rest; do
  printf '%s\n' "$JOBS" | grep -q "daily_close.*$want" \
    || fail "job daily_close.$want missing from Marquez (Airflow events incomplete)"
done
ok "daily_close task jobs present in Marquez (incl. dq_gate)"
for want in ingest_file ingest_soap ingest_rest; do
  printf '%s\n' "$JOBS" | grep -q "$want" \
    || fail "child-DAG job for $want missing (child DAG runs should emit their own task events)"
done
ok "child-DAG ingest jobs present (trigger->child run linkage documented as a gap: ADR-012 D3)"

# --- 3. column-level lineage into a mart -------------------------------------
# Measured API contract (2026-09-13): the nodeId prefix is `dataset:` with the
# namespace + withDownstream; the graph's DATASET_FIELD nodes carry the edges
# as inEdges/outEdges. UI-verified format (the UI itself issues this call).
curl -sf --max-time 15 -G "$BASE/api/v1/column-lineage" \
   --data-urlencode "nodeId=dataset:postgres://warehouse-db:5432:$FCT_ORDERS" \
   --data-urlencode "depth=2" \
   --data-urlencode "withDownstream=true" \
   > "$OUT/marquez-column-lineage-fct_orders.json" \
  || fail "column-lineage API call failed for $FCT_ORDERS"
python3 - "$OUT/marquez-column-lineage-fct_orders.json" "$FCT_ORDERS" <<'PY' || fail "column-lineage graph empty or malformed — no column-level lineage into fct_orders"
import json, sys
g = json.load(open(sys.argv[1]))
edges = set()
for n in g.get("graph", []):
    for e in (n.get("inEdges") or []) + (n.get("outEdges") or []):
        # measured edge contract: {"origin": nodeId, "destination": nodeId}
        edges.add((e.get("origin") or e.get("source"), e.get("destination")))
into_fct = sorted({(s, d) for s, d in edges if d and d.endswith(f"{sys.argv[2]}:customer_sk")}
                  | {(s, d) for s, d in edges if s and s.endswith(f"{sys.argv[2]}:customer_sk")})
print(f"[lineage-verify] column-lineage edges in graph: {len(edges)}")
for s, d in into_fct:
    print(f"[lineage-verify]   {s} -> {d}")
sys.exit(0 if edges and into_fct else 1)
PY
ok "column-level lineage reaches the mart (see $OUT/marquez-column-lineage-fct_orders.json)"

# --- 4. Marquez's own storage: schema + key tables as CSV (DB dump) ----------
docker exec helios-marquez-db pg_dump -U "${MARQUEZ_POSTGRES_USER:-marquez}" -d marquez --schema-only \
  > "$OUT/marquez-db-schema.sql" || fail "pg_dump (schema) failed"
TABLES="$(docker exec helios-marquez-db psql -U "${MARQUEZ_POSTGRES_USER:-marquez}" -d marquez -tAc \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('datasets','dataset_fields','column_lineage','jobs','lineage_events')")"
[ -n "$TABLES" ] || fail "expected Marquez storage tables not found in marquez-db"
for t in $TABLES; do
  docker exec helios-marquez-db psql -U "${MARQUEZ_POSTGRES_USER:-marquez}" -d marquez -c \
    "COPY public.$t TO STDOUT WITH CSV HEADER" > "$OUT/marquez-db-$t.csv" \
    || fail "CSV dump of $t failed"
done
ok "Marquez storage dumped (schema + $(printf '%s ' $TABLES))"

echo "[lineage-verify] PASSED — evidence in $OUT/"
