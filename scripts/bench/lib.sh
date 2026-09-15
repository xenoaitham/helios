# scripts/bench/lib.sh — shared helpers for the bench legs (ADR-015 D5).
# Sourced by scripts/bench/NN-*.sh, never executed directly. Same contract as
# scripts/chaos/lib.sh: every assert exits nonzero LOUDLY — a vacuous PASS is
# worse than a FAIL. The bench measures the shipped platform; it creates no
# state it does not own (no reseed, no --full-refresh, no connector changes,
# no network verbs, no mutator pause).
#
# All SQL passes through docker exec psql with single-line static literals
# supplied by the leg scripts (the smoke/chaos precedent); this lib owns none.

BENCH_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BENCH_LIB_DIR/../.." || exit 1
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

OLTP_USER="${OLTP_POSTGRES_USER:-oltp}";         OLTP_DB="${OLTP_POSTGRES_DB:-oltp}"
WH_USER="${WAREHOUSE_POSTGRES_USER:-warehouse}"; WH_DB="${WAREHOUSE_POSTGRES_DB:-warehouse}"
PROM_URL="http://localhost:${PROMETHEUS_PORT:-9091}"

# ADR-015 D4: the snapshot invariant — NEVER moves (canon 2026-09-11).
BENCH_SNAPSHOT_EXPECTED="50001|2026-09-11 05:40:37.528841"

bench_fail() { echo "[bench] FAIL: $*"; exit 1; }
bench_ok()   { echo "[bench] ok: $*"; }
bench_note() { echo "[bench] -- $*"; }

# --- clocks & rates (all float math via awk; no bc dependency) --------------
BENCH_T0=""; BENCH_T1=""
bench_t0() { BENCH_T0="$(date +%s.%N)"; }
bench_t1() { BENCH_T1="$(date +%s.%N)"; }
bench_elapsed() { awk -v a="$BENCH_T0" -v b="$BENCH_T1" 'BEGIN{printf "%.3f", b-a}'; }
bench_rate() { # <rows> <wall_s> <label> — prints "RATE <label>: <rows> / <wall>s = <n>/s"
  local out
  out="$(awk -v r="$1" -v w="$2" -v l="$3" 'BEGIN{
    if (w+0 <= 0) exit 1
    printf "RATE %s: %s rows / %.3fs = %.1f rows/s\n", l, r, w, r/w }')" \
    || bench_fail "rate computation failed for $3 (rows=$1 wall=$2)"
  echo "[bench] $out"
}

# --- SQL probes (single-line static literals from the leg scripts) ----------
whq()   { docker exec helios-warehouse-db psql -U "$WH_USER" -d "$WH_DB" -tAc "$1" || bench_fail "warehouse-db query failed"; }
oltpq() { docker exec helios-oltp-db psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "$1" || bench_fail "oltp-db query failed"; }
afdbq() { docker exec helios-airflow-db psql -U airflow -d airflow -tAc "$1" || bench_fail "airflow-db query failed"; }

# --- container health -------------------------------------------------------
bench_health() { docker inspect -f '{{.State.Health.Status}}' "helios-$1" 2>/dev/null || echo missing; }

# One-shot tool containers are addressed by compose service label (docker
# compose run ignores container_name — ADR-014 D2 canon).
bench_oneshot_ids() {
  docker ps -q --filter "label=com.docker.compose.project=helios" --filter "label=com.docker.compose.service=$1"
}

# --- preflight (ADR-015 D3): health + no dag-run collision + 05:00 UTC guard -
BENCH_PREFLIGHT_CONTAINERS="oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler soap-service rest-mock cdc-connect cdc-sink marquez-db marquez-api marquez-web statsd-exporter prometheus grafana oltp-mutator"
# The longest leg (the orchestrated close) is < 10 min warm; 30 min is the guard.
BENCH_NIGHTLY_GUARD_S=1800

bench_preflight() {
  local c h active now today05 fire dist
  echo "[bench] preflight at $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  for c in $BENCH_PREFLIGHT_CONTAINERS; do
    h="$(bench_health "$c")"
    [ "$h" = "healthy" ] || bench_fail "preflight: helios-$c health=$h (make up first)"
  done
  # Nightly 05:00 UTC trap (ADR-015 D3): never start a leg while a close is
  # live or queued, nor inside the guard window before the fire.
  active="$(afdbq "SELECT count(*) FROM dag_run WHERE dag_id IN ('daily_close','ingest_file','ingest_soap','ingest_rest') AND state IN ('running','queued')")"
  [ "$active" = "0" ] || bench_fail "preflight: $active dag run(s) running/queued — nightly-collision guard; check Airflow UI :8080 and wait"
  now="$(date -u +%s)"
  today05="$(date -u -d "$(date -u +%F) 05:00" +%s)"
  if [ "$now" -ge "$today05" ]; then fire=$((today05 + 86400)); else fire="$today05"; fi
  dist=$((fire - now))
  [ "$dist" -gt "$BENCH_NIGHTLY_GUARD_S" ] || bench_fail "preflight: next 05:00 UTC fire is ${dist}s away (<= ${BENCH_NIGHTLY_GUARD_S}s guard) — starting a leg now risks measuring the nightly queue; wait it out"
  bench_ok "preflight: 17 containers healthy, no dag run active, next 05:00 UTC fire in ${dist}s"
}

# --- invariants (re-asserted by every leg that could move them) -------------
bench_snapshot_invariant() {
  local got
  got="$(whq "SELECT count(*) || '|' || min(dbt_valid_from)::text FROM snapshots.customers_snapshot")"
  [ "$got" = "$BENCH_SNAPSHOT_EXPECTED" ] || bench_fail "snapshot invariant moved: got '$got' expected '$BENCH_SNAPSHOT_EXPECTED'"
  bench_ok "snapshot invariant intact: $got"
}

bench_quarantine_zero_open() {
  local n
  n="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
  [ "$n" = "0" ] || bench_fail "dq.dq_quarantine has $n OPEN incidents (expected 0)"
  bench_ok "dq.dq_quarantine: 0 OPEN"
}

# CDC sink lag: probe cdc-status until lag TOTAL=0 (transient drain at probe
# time is not a bench failure; a persistent nonzero lag is).
bench_wait_cdc_lag_zero() { # [tries=4]
  local tries="${1:-4}" i out lag
  for i in $(seq 1 "$tries"); do
    out="$(docker compose run --rm cdc-sink python -m cdc.status 2>/dev/null)" || bench_fail "cdc-status failed"
    echo "$out"
    lag="$(grep -o 'lag TOTAL=[0-9]*' <<< "$out" | tail -1 | cut -d= -f2)"
    [ "$lag" = "0" ] && { bench_ok "cdc lag TOTAL=0"; return 0; }
    bench_note "cdc lag TOTAL=$lag (drain in progress? try $i/$tries) — waiting 15s"
    sleep 15
  done
  bench_fail "cdc lag never reached 0 after $tries probes (last TOTAL=$lag)"
}

# --- dbt output parsing (ANSI-stripped; per-model walls) --------------------
bench_strip_ansi() { sed -e 's/\x1b\[[0-9;]*m//g'; }

# Reads dbt build output on stdin; prints "schema.table <wall_s>" per created
# resource; the arg filters by schema prefix (staging|marts|snapshots). Note
# dbt's per-resource verbs (probed 2026-09-14): models print "OK created",
# snapshots print "OK snapshotted" — both carry the [SELECT/INSERT … in N.NNs]
# wall this parser extracts.
bench_parse_model_walls() { # <schema_prefix>
  bench_strip_ansi | awk -v want="$1" '
    / OK (created|snapshotted) / {
      name=""; wall="";
      for (i=1; i<=NF; i++) if ($i ~ /^(staging|marts|snapshots)\./ && name=="") name=$i;
      if (name == "" || index(name, want".") != 1) next;
      if (match($0, /in [0-9]+\.[0-9]+s/))      wall=substr($0, RSTART+3, RLENGTH-4);
      else if (match($0, /in [0-9]+s/))          wall=substr($0, RSTART+3, RLENGTH-4);
      if (wall != "") print name, wall;
    }'
}

# Sum the wall column of "<name> <wall>" lines; prints the sum, line count too.
bench_sum_walls() { awk '{s+=$2; n++} END{printf "%.3f %d\n", s, n}'; }
