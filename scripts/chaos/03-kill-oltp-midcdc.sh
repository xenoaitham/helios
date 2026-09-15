#!/usr/bin/env bash
# chaos-03 kill_oltp_midcdc (ADR-014 D3): stop oltp-db mid-CDC churn, then
# prove the replication-slot contract end to end.
#
# Degrade-safely mechanism (named BEFORE the act): the replication slot pins
# retained WAL while the source is unavailable — nothing is lost, the
# connector resumes from the slot, and the sink consumes nothing while there
# is nothing to consume. The blast radius is measured at three layers while
# degraded: the connector loses its source (Connect REST status), the
# mutator's ticks fail loudly (its own log line), and the source DB is
# unreachable.
# Convergence proof: oltp-db healthy again → connector RUNNING → churn
# resumes → sink drains (cdc-status lag TOTAL=0) → retained WAL measured at
# peak (recovery start) and drained after.
# This scenario owns NO dag run: it is pure movement-layer chaos.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 03 kill_oltp_midcdc: stop oltp-db mid-churn; recovery = slot resume + sink drain"
chaos_preflight

SLOT_SQL="SELECT COALESCE(round(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))/1024/1024, 1) FROM pg_replication_slots WHERE slot_name='helios_cdc_slot'"

# 1. pre-state
WAL_BASE_MB="$(oltpq "$SLOT_SQL")"
USERS_RAW_BASE="$(whq "SELECT count(*) FROM raw.cdc_users")"
CDC_BASE="$(chaos_cdc_status | tail -20)"
chaos_ok "pre-state: retained WAL=${WAL_BASE_MB}MB raw.cdc_users=${USERS_RAW_BASE}"
chaos_note "pre-state cdc-status: $(echo "$CDC_BASE" | grep -i 'total\|lag' | head -2 | tr '\n' ' ')"

# 2. chaos act
chaos_note "CHAOS ACT: docker stop oltp-db (the CDC source)"
MUT_FAILS_BEFORE="$(docker logs --tail 500 helios-oltp-mutator 2>&1 | grep -c 'tick failed' || true)"
STOP_TS="$(date +%s)"
docker compose stop oltp-db >/dev/null || chaos_fail "docker compose stop oltp-db"
sleep 5
[ "$(docker exec helios-oltp-db pg_isready -U "$OLTP_USER" -d "$OLTP_DB" 2>/dev/null)" ] \
  && chaos_fail "oltp-db still answers after stop" || chaos_ok "oltp-db down (measured: no pg_isready answer)"

# 3. degrade-safely assertions (while degraded)
sleep 55
CONN_STATE="$(docker exec helios-cdc-connect curl -sf --max-time 10 localhost:8083/connectors/helios-oltp/status 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["connector"]["state"], d["tasks"][0]["state"] if d["tasks"] else "no-tasks")' 2>/dev/null || echo unreachable)"
chaos_ok "connector surface while degraded: '$CONN_STATE' (source gone — recorded, whatever it honestly reports)"
# docker logs --since is unreliable on this daemon (measured: empty output where
# lines exist) — the tick-failure count is a BEFORE/AFTER delta over --tail.
MUT_FAILS_AFTER="$(docker logs --tail 500 helios-oltp-mutator 2>&1 | grep -c 'tick failed' || true)"
MUT_FAILS=$((MUT_FAILS_AFTER - MUT_FAILS_BEFORE))
MUT_HEALTH="$(docker exec helios-oltp-mutator python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8081/health').status)" 2>/dev/null || echo unreachable)"
chaos_ok "mutator surface while degraded: $MUT_FAILS failed tick(s) during the outage (delta), health endpoint=$MUT_HEALTH (503 = stale)"
[ "$MUT_FAILS" -ge 1 ] || [ "$MUT_HEALTH" = "503" ] || chaos_note "NOTE: mutator did not visibly fail in this window (tick timing); source-layer degradation is carried by the connector state above"

# 4. recovery
chaos_note "restarting oltp-db (the slot holds: no source rows lost, no sink action needed)"
docker compose start oltp-db >/dev/null || chaos_fail "docker compose start oltp-db"
chaos_wait_healthy oltp-db 120
WAL_PEAK_MB="$(oltpq "$SLOT_SQL")"
chaos_ok "retained WAL at recovery start: ${WAL_PEAK_MB}MB (peak retention while the slot was behind)"

# FOUND-BY-VERIFICATION (first run, 2026-09-13): a Debezium task that FAILED
# on a total source outage does NOT self-recover when the source returns —
# it stays FAILED until a task restart via the Connect API (204). The
# restart is scripted recovery, not re-setup: config/offsets/slot untouched.
chaos_note "waiting briefly for connector self-recovery ..."
CS="FAILED"
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  CS="$(docker exec helios-cdc-connect curl -sf --max-time 10 localhost:8083/connectors/helios-oltp/status 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["tasks"][0]["state"])' 2>/dev/null || echo unreachable)"
  [ "$CS" = "RUNNING" ] && break
  sleep 10
done
if [ "$CS" != "RUNNING" ]; then
  chaos_note "measured finding confirmed: task did not self-recover (state: $CS) — POSTing the scripted task restart (no cdc-setup, offsets/slot untouched)"
  docker exec helios-cdc-connect curl -sf -X POST localhost:8083/connectors/helios-oltp/tasks/0/restart -o /dev/null \
    || chaos_fail "connector task restart POST failed"
  chaos_ok "connector task restart issued (Connect API 204)"
fi
chaos_note "waiting for the connector task to be RUNNING ..."
deadline=$(( $(date +%s) + 240 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  CS="$(docker exec helios-cdc-connect curl -sf --max-time 10 localhost:8083/connectors/helios-oltp/status 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["tasks"][0]["state"])' 2>/dev/null || echo unreachable)"
  [ "$CS" = "RUNNING" ] && break
  sleep 10
done
[ "$CS" = "RUNNING" ] || chaos_fail "connector task not RUNNING within 240s (state: $CS)"
chaos_ok "connector task RUNNING again"

# 5. convergence proof: sink drains, lag 0, WAL retention drains
chaos_note "waiting for cdc-status lag TOTAL=0 (timeout 300s)"
deadline=$(( $(date +%s) + 300 ))
LAG_TOTAL=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  LAG_LINE="$(chaos_cdc_status | grep -i 'total' | tail -1)"
  LAG_TOTAL="$(echo "$LAG_LINE" | grep -oE 'TOTAL=[0-9]+' | head -1 | cut -d= -f2)"
  [ -n "$LAG_TOTAL" ] && [ "$LAG_TOTAL" = "0" ] && break
  sleep 15
done
[ "$LAG_TOTAL" = "0" ] || chaos_fail "cdc lag TOTAL=${LAG_TOTAL:-unknown} did not drain to 0 within 300s ($LAG_LINE)"
chaos_ok "sink drained: cdc-status $LAG_LINE"
USERS_RAW_NOW="$(whq "SELECT count(*) FROM raw.cdc_users")"
[ "$USERS_RAW_NOW" -ge "$USERS_RAW_BASE" ] || chaos_fail "raw.cdc_users shrank: $USERS_RAW_BASE -> $USERS_RAW_NOW"
chaos_ok "raw.cdc_users baseline=$USERS_RAW_BASE now=$USERS_RAW_NOW (churn resumed and landed)"
WAL_DRAINED_MB="$(oltpq "$SLOT_SQL")"
sleep 30
WAL_DRAINED2_MB="$(oltpq "$SLOT_SQL")"
chaos_ok "retained WAL three-point story: baseline=${WAL_BASE_MB}MB peak=${WAL_PEAK_MB}MB post-drain=${WAL_DRAINED_MB}MB -> ${WAL_DRAINED2_MB}MB (mid-churn sits above confirmed_flush; the slot drains back toward baseline, no unbounded growth)"
[ "$(python3 -c "print(1 if float('${WAL_DRAINED2_MB:-999999}') < 100 else 0)")" = "1" ] \
  || chaos_fail "retained WAL did not drain (${WAL_DRAINED2_MB}MB >= 100MB ceiling)"

chaos_ok "scenario 03 kill_oltp_midcdc PASSED (wall $(chaos_wall))"
