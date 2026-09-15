# scripts/chaos/lib.sh — shared helpers for the chaos scenarios (ADR-014 D5).
# Sourced by scripts/chaos/NN-*.sh, never executed directly. Every assert
# exits nonzero LOUDLY: a vacuous PASS is worse than a FAIL (the
# smoke/verify contract, inherited by chaos per ADR-014).
#
# All SQL passes through docker exec psql with single-line static literals
# supplied by the scenario scripts (the smoke-test precedent) — the lib adds
# no SQL of its own beyond these one-liners it owns:
#   - preflight dag-run collision check (ADR-014 D4)
#   - retained-WAL / slot probes (scenario 03)
# Nothing here ever writes warehouse state; poison writes are named UPDATEs
# and drop-volume files in the scenario scripts, each logged in its transcript.

CHAOS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$CHAOS_LIB_DIR/../.." || exit 1
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

OLTP_USER="${OLTP_POSTGRES_USER:-oltp}";         OLTP_DB="${OLTP_POSTGRES_DB:-oltp}"
WH_USER="${WAREHOUSE_POSTGRES_USER:-warehouse}"; WH_DB="${WAREHOUSE_POSTGRES_DB:-warehouse}"
PROM_URL="http://localhost:${PROMETHEUS_PORT:-9091}"
CHAOS_START_S="$(date +%s)"

chaos_fail() { echo "[chaos] FAIL: $*"; exit 1; }
chaos_ok()   { echo "[chaos] ok: $*"; }
chaos_note() { echo "[chaos] -- $*"; }
chaos_wall() { echo "$(( $(date +%s) - CHAOS_START_S ))s"; }

# --- container health -------------------------------------------------------
chaos_health() { docker inspect -f '{{.State.Health.Status}}' "helios-$1" 2>/dev/null || echo missing; }

# One-shot tool containers (compose profiles ["tools"]) are NOT addressable by
# container_name: `docker compose run` (v2) appends -run-<hash> and ignores
# container_name for run (found-by-verification, first chaos-01 attempt
# 2026-09-13 — the fixed name never existed and the poll missed every build).
# The stable handle is the compose service label under the helios project.
chaos_oneshot_ids() { # <service> -> space-separated running container IDs (empty when none)
  docker ps -q --filter "label=com.docker.compose.project=helios" --filter "label=com.docker.compose.service=$1"
}

chaos_wait_healthy() { # <container> [timeout_s=120]
  local c="$1" t="${2:-120}" h=""
  local deadline=$(( $(date +%s) + t ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    h="$(chaos_health "$c")"
    [ "$h" = "healthy" ] && { chaos_ok "container $c healthy"; return 0; }
    sleep 3
  done
  chaos_fail "container $c not healthy within ${t}s (last: $h)"
}

# --- SQL probes (single-line static literals from the scenario scripts) -----
oltpq() { docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "$1" || chaos_fail "oltp-db query failed"; }
whq()   { docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc "$1" || chaos_fail "warehouse-db query failed"; }
# Deliberate during-outage probe: reports DOWN instead of failing, so the
# outage itself can be asserted as the observed degradation (scenario 02).
whq_down_ok() { docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc "$1" 2>/dev/null || echo "DOWN"; }
afdbq() { docker exec helios-airflow-db psql -U airflow -d airflow -tAc "$1" || chaos_fail "airflow-db query failed"; }

# cdc-status (docker compose run of the tool image, same as make cdc-status)
chaos_cdc_status() { docker compose run --rm cdc-sink python -m cdc.status; }

# --- preflight (ADR-014 D4): stack healthy + no dag-run collision ----------
CHAOS_PREFLIGHT_CONTAINERS="oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler soap-service rest-mock cdc-connect cdc-sink marquez-db marquez-api marquez-web statsd-exporter prometheus grafana oltp-mutator"

chaos_preflight() {
  local c h active
  for c in $CHAOS_PREFLIGHT_CONTAINERS; do
    h="$(chaos_health "$c")"
    [ "$h" = "healthy" ] || chaos_fail "preflight: helios-$c health=$h (make up first)"
  done
  # Nightly 05:00 UTC trap (ADR-014 D4): never act while a close is live or
  # queued (max_active_runs=1 would silently starve a queued nightly).
  active="$(afdbq "SELECT count(*) FROM dag_run WHERE dag_id IN ('daily_close','ingest_file','ingest_soap','ingest_rest') AND state IN ('running','queued')")"
  [ "$active" = "0" ] || chaos_fail "preflight: $active dag run(s) running/queued across daily_close/ingest_* — nightly-collision guard; check Airflow UI :8080 and wait"
  chaos_ok "preflight: 17 containers healthy, no daily_close/ingest dag run active"
}

# --- DAG state probes -------------------------------------------------------
chaos_dag_state()  { afdbq "SELECT state FROM dag_run WHERE dag_id='$1' AND run_id='$2'" | tail -1; }
chaos_task_state() { afdbq "SELECT state, try_number FROM task_instance WHERE dag_id='$1' AND run_id='$2' AND task_id='$3'"; }
# Latest dag run of a child DAG created after a given timestamp (trigger discovery).
chaos_latest_run_since() { afdbq "SELECT run_id FROM dag_run WHERE dag_id='$1' AND execution_date >= '$2' ORDER BY execution_date DESC LIMIT 1" | tail -1; }

chaos_wait_task_state() { # <dag> <run_id> <task> <state> [timeout_s=120]
  local dag="$1" run="$2" task="$3" want="$4" t="${5:-120}" cur=""
  local deadline=$(( $(date +%s) + t ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    cur="$(chaos_task_state "$dag" "$run" "$task")"
    [ "${cur%%|*}" = "$want" ] && { chaos_ok "task $dag.$task reached $want (ti: $cur)"; return 0; }
    sleep 5
  done
  chaos_fail "task $dag.$task did not reach '$want' within ${t}s (last ti state: $cur)"
}

# --- close drivers ----------------------------------------------------------
# Read the fct_items_order_integrity violation count out of a failed
# dbt_build attempt's task log (the known live-CDC straddle test, ADR-008
# D3 / ADR-009 D6 — the mart refuses to publish unreconciled rows).
chaos_straddle_violations() { # <run_id> <attempt>
  docker exec helios-airflow-scheduler sh -c \
    "grep -o 'FAIL [0-9]* fct_items_order_integrity' '/opt/airflow/logs/dag_id=daily_close/run_id=$1/task_id=dbt_build/attempt=$2.log' 2>/dev/null" \
    | head -1 | awk '{print $2}'
}

# Trigger closes until one fails AT the dq_gate (the poisoned-data red leg).
# A close that fails EARLIER (at dbt_build) is the documented µs-window
# live-CDC straddle (cascade deletes land non-atomically across raw topics —
# measured 2026-09-13: item tombstones up to 9.5 min behind their order's in
# the same source transaction); the DAG's own run-level retry absorbs it —
# the scenario retries the close and attributes every such attempt.
# Sets CHAOS_RED_RUN / CHAOS_RED_LOG on success; rc 9 = a close SUCCEEDED on
# poisoned data (the gate did not block — the worst outcome, loud fail);
# rc 8 = attempts exhausted without reaching the gate.
chaos_close_until_gate_red() { # <run_id_prefix> [max_attempts=3]
  local run="$1" max="${2:-3}" i=1 rc dbt_state dq_state v
  while [ "$i" -le "$max" ]; do
    local myrun="${run}-a${i}"
    local log="/tmp/chaos-etl-${myrun}.log"
    bash scripts/run-etl.sh "$myrun" >"$log" 2>&1 &
    local pid=$!
    wait "$pid"; rc=$?
    if [ "$rc" = "0" ]; then
      echo "[chaos] FAIL: close SUCCEEDED on poisoned data (attempt $i) — THE GATE DID NOT BLOCK"
      cat "$log"
      return 9
    fi
    dbt_state="$(chaos_task_state daily_close "$myrun" dbt_build)"
    dq_state="$(chaos_task_state daily_close "$myrun" dq_gate)"
    if [ "${dq_state%%|*}" = "failed" ]; then
      CHAOS_RED_RUN="$myrun"; CHAOS_RED_LOG="$log"
      chaos_ok "red AT THE GATE (attempt $i): dbt_build=${dbt_state%%|*} dq_gate=failed"
      return 0
    fi
    v="$(chaos_straddle_violations "$myrun" 3)"
    chaos_note "attempt $i failed BEFORE the gate (dbt_build=${dbt_state%%|*}, dq_gate=${dq_state%%|*}) — live-CDC straddle absorbed by run-level retry (measured fct_items_order_integrity violations this attempt: ${v:-n/a}); retrying ($((i+1))/$max)"
    i=$((i+1))
  done
  chaos_fail "exhausted $max attempts without a gate-red close (last dbt_build: ${dbt_state%%|*})"
}

# Trigger closes until one SUCCEEDS (the green leg); a pre-gate failure is
# attributed to the same straddle and retried. rc nonzero if never green.
chaos_close_until_green() { # <run_id_prefix> [max_attempts=3]
  local run="$1" max="${2:-3}" i=1 rc dbt_state v
  while [ "$i" -le "$max" ]; do
    local myrun="${run}-a${i}"
    local log="/tmp/chaos-etl-${myrun}.log"
    bash scripts/run-etl.sh "$myrun" >"$log" 2>&1 &
    local pid=$!
    wait "$pid"; rc=$?
    if [ "$rc" = "0" ]; then
      CHAOS_GREEN_RUN="$myrun"; CHAOS_GREEN_LOG="$log"
      chaos_ok "green close SUCCESS (attempt $i)"
      return 0
    fi
    dbt_state="$(chaos_task_state daily_close "$myrun" dbt_build)"
    v="$(chaos_straddle_violations "$myrun" 3)"
    chaos_note "green attempt $i failed (dbt_build=${dbt_state%%|*}; straddle violations: ${v:-n/a}) — retrying ($((i+1))/$max)"
    i=$((i+1))
  done
  chaos_fail "exhausted $max green attempts (last dbt_build: ${dbt_state%%|*})"
}

# --- Prometheus alerts (read-only host curl probes; ADR-014 D6) -------------
chaos_alert_state() { # <alertname> -> firing|pending|inactive|none
  curl -sf --max-time 10 "$PROM_URL/api/v1/alerts" | python3 -c '
import json, sys
name = sys.argv[1]
d = json.load(sys.stdin)
states = [a["state"] for a in d["data"]["alerts"] if a["labels"].get("alertname") == name]
print("firing" if "firing" in states else ("pending" if "pending" in states else ("inactive" if states else "none")))
' "$1" || echo "prom-unreachable"
}

chaos_wait_alert_firing() { # <alertname> [timeout_s=180] [start_epoch=now] — prints measured latency
  local name="$1" t="${2:-180}" start="${3:-$(date +%s)}" st=""
  local deadline=$(( $(date +%s) + t ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st="$(chaos_alert_state "$name")"
    if [ "$st" = "firing" ]; then
      local lat=$(( $(date +%s) - start ))
      chaos_ok "$name FIRING (measured latency ${lat}s from the act)"
      echo "[chaos] measured-alert-latency ${name} ${lat}s"
      return 0
    fi
    sleep 5
  done
  chaos_fail "$name did not FIRE within ${t}s (last state: $st)"
}

chaos_wait_alert_resolved() { # <alertname> [timeout_s=900] — rule semantics: increase([10m]) rolls off
  local name="$1" t="${2:-900}" st="" start="$(date +%s)"
  local deadline=$(( $(date +%s) + t ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st="$(chaos_alert_state "$name")"
    if [ "$st" != "firing" ] && [ "$st" != "pending" ]; then
      chaos_ok "$name resolved after $(( $(date +%s) - start ))s of waiting (increase([10m]) window rolled off)"
      return 0
    fi
    sleep 20
  done
  chaos_fail "$name still $st after ${t}s"
}

chaos_dump_alerts() { curl -sf --max-time 10 "$PROM_URL/api/v1/alerts" | python3 -m json.tool | head -60 || echo "(alerts dump unavailable)"; }

# Prometheus instant-query value (empty on error — callers decide).
chaos_metric() { # <promql>
  curl -sf --max-time 10 "$PROM_URL/api/v1/query" --get --data-urlencode "query=$1" \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "0")' 2>/dev/null
}

# Wait until a PromQL instant value >= min (measured observability surface).
chaos_wait_metric() { # <promql> <min> [timeout_s=180]
  local q="$1" min="$2" t="${3:-180}" v=""
  local deadline=$(( $(date +%s) + t ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    v="$(chaos_metric "$q")"
    [ -n "$v" ] && [ "$(python3 -c "print(1 if float('$v') >= float('$min') else 0)")" = "1" ] && { chaos_ok "metric assertion met: $q = $v (>= $min)"; return 0; }
    sleep 5
  done
  chaos_fail "metric '$q' never reached >= $min within ${t}s (last: ${v:-unreachable})"
}
