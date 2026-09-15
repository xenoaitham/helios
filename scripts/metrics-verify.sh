#!/usr/bin/env bash
# make metrics-verify (ADR-013 D8): prove MEASURED metrics — not config
# claims — and dump the evidence.
#
# Asserts, against the LIVE stack:
#   1. the metrics stack is healthy (statsd-exporter, prometheus, grafana),
#   2. Prometheus answers and every scrape target reports up,
#   3. the three alert rules are LOADED by Prometheus (not just in a file),
#   4. the mapped metric names exist in the EXPORTER'S OWN /metrics
#      (the mapping file is applied; airflow is emitting),
#   5. a known metric exists with a REAL value via the query API
#      (task-finish counters + the dag-run duration histogram),
#   6. Grafana has the three provisioned datasources and the dashboard.
#
# Non-destructive. Every step aborts loudly on failure: a vacuous PASS is
# worse than a FAIL (Session-9 precedent). Idempotent — safe to re-run.
# Requires at least one pipeline task to have run since the metrics stack
# came up (run `make run-etl` first on a fresh stack).
set -euo pipefail
cd "$(dirname "$0")/.."
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

PROM_PORT="${PROMETHEUS_PORT:-9091}"
GRAF_PORT="${GRAFANA_PORT:-3001}"
PROM="http://localhost:${PROM_PORT}"
GRAF="http://localhost:${GRAF_PORT}"
OUT=EVIDENCE/phase-4-metrics
mkdir -p "$OUT"

fail() { echo "[metrics-verify] FAIL: $*"; exit 1; }
ok()   { echo "[metrics-verify] ok: $*"; }

command -v curl >/dev/null || fail "curl not found on host"
command -v python3 >/dev/null || fail "python3 not found on host (JSON parsing)"

# --- 0. the metrics stack is healthy -----------------------------------------
for svc in statsd-exporter prometheus grafana; do
  H="$(docker inspect -f '{{.State.Health.Status}}' "helios-${svc}" 2>/dev/null || echo missing)"
  [ "$H" = "healthy" ] || fail "helios-${svc} health=$H (make up first)"
done
ok "metrics stack healthy (statsd-exporter, prometheus, grafana)"

# --- 1. Prometheus ready + every scrape target up ----------------------------
curl -sf --max-time 15 "$PROM/-/ready" >/dev/null || fail "prometheus not answering /-/ready on $PROM"
curl -sf --max-time 15 "$PROM/api/v1/targets" > "$OUT/prom-targets.json" || fail "GET /api/v1/targets"
python3 - "$OUT/prom-targets.json" <<'PY' || fail "not every scrape target reports up — see $OUT/prom-targets.json"
import json, sys
targets = json.load(open(sys.argv[1]))["data"]["activeTargets"]
rows = [(t["labels"].get("job", "?"), t["health"]) for t in targets]
for job, health in rows:
    print(f"[metrics-verify]   target {job}: {health}")
sys.exit(0 if rows and all(h == "up" for _, h in rows) else 1)
PY
ok "all scrape targets up (prometheus, statsd-exporter, grafana; dump: prom-targets.json)"

# --- 2. the alert rules are LOADED -------------------------------------------
curl -sf --max-time 15 "$PROM/api/v1/rules" > "$OUT/prom-rules.json" || fail "GET /api/v1/rules"
python3 - "$OUT/prom-rules.json" <<'PY' || fail "alert rules missing from the loaded rule set — see prom-rules.json"
import json, sys
groups = json.load(open(sys.argv[1]))["data"]["groups"]
rules = [r["name"] for g in groups for r in g["rules"]]
print(f"[metrics-verify]   rules loaded: {', '.join(rules)}")
sys.exit(0 if {"HeliosScrapeTargetDown", "HeliosAirflowTaskFailure", "HeliosDailyCloseStale"} <= set(rules) else 1)
PY
ok "all three alert rules loaded by Prometheus"

# --- 3. mapped names in the exporter's OWN /metrics (mapping-file proof) -----
# Pulled from INSIDE the scheduler container (in-net, the same network the
# scraper uses) — proving the exporter + mapping + airflow emission chain.
# (The prometheus container's busybox wget cannot resolve in-net DNS — probed
# at boot; the scheduler's python urllib is the honest in-net probe.)
EXPORTER_METRICS="$(docker exec helios-airflow-scheduler python -c "
import urllib.request
print(urllib.request.urlopen('http://statsd-exporter:9102/metrics', timeout=10).read().decode())
" 2>/dev/null || true)"
for want in airflow_task_finish_total airflow_task_start_total airflow_scheduler_heartbeat; do
  grep -q "^# HELP ${want} " <<< "$EXPORTER_METRICS" \
    || fail "mapped metric $want absent from statsd-exporter /metrics (mapping not applied, or airflow not emitting — check AIRFLOW__METRICS__STATSD_*)"
done
printf '%s\n' "$EXPORTER_METRICS" > "$OUT/statsd-exporter-metrics.txt"
ok "mapped metric names present in the exporter's /metrics (mapping file applied; dump: statsd-exporter-metrics.txt)"

# --- 4. real values via the Prometheus query API -----------------------------
pq() { curl -sf --max-time 15 "$PROM/api/v1/query" --data-urlencode "query=$1"; }

TASK_JSON="$(pq 'sum(airflow_task_finish_total{dag_id="daily_close"})')" || fail "query airflow_task_finish_total"
echo "$TASK_JSON" > "$OUT/query-task-finish-total.json"
python3 - "$OUT/query-task-finish-total.json" <<'PY' || fail "airflow_task_finish_total{dag_id=daily_close} has no samples — no task has run since the metrics stack came up (make run-etl first), or the StatsD path is broken"
import json, sys
result = json.load(open(sys.argv[1]))["data"]["result"]
total = sum(float(x["value"][1]) for x in result)
print(f"[metrics-verify]   sum(airflow_task_finish_total{{dag_id=daily_close}}) = {total}")
sys.exit(0 if total >= 1 else 1)
PY
ok "task-finish counters present with real values (the failure/success surface, measured)"

DUR_JSON="$(pq 'airflow_dagrun_duration_seconds_count{dag_id="daily_close",status="success"}')" || fail "query dagrun duration count"
echo "$DUR_JSON" > "$OUT/query-dagrun-duration.json"
python3 - "$OUT/query-dagrun-duration.json" <<'PY' || fail "airflow_dagrun_duration_seconds_count{status=success} empty — no successful close recorded since wiring (make run-etl first)"
import json, sys
result = json.load(open(sys.argv[1]))["data"]["result"]
count = sum(float(x["value"][1]) for x in result)
print(f"[metrics-verify]   airflow_dagrun_duration_seconds_count{{status=success}} = {count}")
sys.exit(0 if count >= 1 else 1)
PY
ok "dag-run duration histogram populated (the duration surface, measured; dumps: query-*.json)"

# informational: current alert states (the DRILL, not this script, asserts firing)
curl -sf --max-time 15 "$PROM/api/v1/alerts" > "$OUT/prom-alerts.json" || fail "GET /api/v1/alerts"
python3 - "$OUT/prom-alerts.json" <<'PY'
import json, sys
alerts = json.load(open(sys.argv[1]))["data"]["alerts"]
if alerts:
    for a in alerts:
        print(f"[metrics-verify]   alert {a['labels'].get('alertname')}: {a['state']}")
else:
    print("[metrics-verify]   alerts: none currently (active)")
PY
ok "alert states dumped (prom-alerts.json; firing is proven by make metrics-drill)"

# --- 5. Grafana: provisioned datasources + dashboard (env-authenticated) -----
GA="${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD:-grafana_local_dev}"
curl -sf --max-time 15 -u "$GA" "$GRAF/api/datasources" > "$OUT/grafana-datasources.json" \
  || fail "grafana datasources API failed (auth uses GRAFANA_ADMIN_* from .env)"
python3 - "$OUT/grafana-datasources.json" <<'PY' || fail "grafana datasources not provisioned as expected"
import json, sys
names = {d["name"] for d in json.load(open(sys.argv[1]))}
print(f"[metrics-verify]   grafana datasources: {', '.join(sorted(names))}")
sys.exit(0 if {"Helios-Prometheus", "Helios-Warehouse", "Helios-Airflow"} <= names else 1)
PY
ok "grafana datasources provisioned (Prometheus + warehouse-db + airflow-db; dump: grafana-datasources.json)"

curl -sf --max-time 15 -u "$GA" "$GRAF/api/search" > "$OUT/grafana-dashboards.json" || fail "grafana search API"
python3 - "$OUT/grafana-dashboards.json" <<'PY' || fail "the helios-pipeline dashboard is not provisioned"
import json, sys
boards = json.load(open(sys.argv[1]))
uids = {b.get("uid") for b in boards}
print(f"[metrics-verify]   grafana dashboards: {', '.join(sorted(b.get('title','?') for b in boards))}")
sys.exit(0 if "helios-pipeline" in uids else 1)
PY
ok "grafana dashboard provisioned from code (uid helios-pipeline; dump: grafana-dashboards.json)"

echo "[metrics-verify] PASSED — evidence in $OUT/"
