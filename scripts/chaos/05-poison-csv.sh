#!/usr/bin/env bash
# chaos-05 poison_csv (ADR-014 D3): the Session-9 DRILL-2 injection as
# scripted chaos — a file feed row with an impossible future signup_date,
# red at the semantic gate, ROW-LEVEL dead-letter, fix-and-reland, replay.
#
# Poison: hand-written customers-YYYYMMDD.csv (filename matches the
# extractor's ^customers-\d{8}\.csv$ contract) with row 99990001
# signup_date=2031-01-01 (impossible future) + control row 99990002
# (2026-09-01, clean). Injected through the filedrop volume via the
# filedrop-tools container (its rw mount — the ingest side is read-only).
#
# Degrade-safely mechanism (named BEFORE the act): the semantic gate blocks
# the close and dead-letters ONLY the offending row (row-level targeting
# re-proven: the control row lands clean and is never quarantined);
# HeliosAirflowTaskFailure fires on the gate failure.
# Convergence proof: fix-and-reland (new content hash → the ledger relands;
# hash-guarded upsert no-ops the unchanged control row) → next close green →
# staging signup corrected (measured) → dq-replay resolves → 0 OPEN.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 05 poison_csv: file signup 2031 -> row-level quarantine -> fix-and-reland -> replay"
chaos_preflight

DROP_FILE="customers-$(date -u +%Y%m%d).csv"
POISON_RUN="chaos05-red-$(date -u +%Y%m%dT%H%M%SZ)"
GREEN_RUN="chaos05-green-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-05-etl.log"
WORK="/tmp/chaos-05"
mkdir -p "$WORK"

# 1. pre-state
OPEN_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
RESOLVED_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NOT NULL")"
[ "$OPEN_BASE" = "0" ] || chaos_fail "quarantine open=$OPEN_BASE (expected 0 before chaos)"
CTRL_BASE="$(whq "SELECT signup_date FROM staging.stg_file_customers WHERE customer_id='99990002'")"
chaos_ok "pre-state: quarantine open=0 resolved=$RESOLVED_BASE; control row 99990002 signup_date=${CTRL_BASE:-absent}"

# 2. chaos act: poison file into the drop volume
chaos_note "writing poison CSV $DROP_FILE (99990001 signup 2031-01-01; control 99990002 clean)"
printf '%s\n' 'customer_id,email,full_name,country_code,signup_date,tier' \
  '99990001,chaos.99990001@example.com,Chaos Ninehundredone,US,2031-01-01,bronze' \
  '99990002,chaos.99990002@example.com,Chaos Ninehundredtwo,US,2026-09-01,gold' > "$WORK/$DROP_FILE"
chaos_note "CHAOS ACT: injecting $DROP_FILE into the filedrop volume"
docker compose run --rm -v "$WORK:/src:ro" filedrop-tools cp "/src/$DROP_FILE" /data/drop/ >/dev/null \
  || chaos_fail "filedrop-tools cp into /data/drop"

# 3. poison close: run-etl must FAIL at dq_gate
chaos_note "triggering the POISON close run_id=$POISON_RUN (failure at the gate expected and asserted; straddle retries attributed)"
chaos_close_until_gate_red "$POISON_RUN" 3 || exit 1
POISON_RUN="$CHAOS_RED_RUN"
DQ_TI="$(chaos_task_state daily_close "$POISON_RUN" dq_gate)"
chaos_ok "red AT THE GATE: dq_gate=$DQ_TI"
DQ_FAIL_TS="$(afdbq "SELECT extract(epoch FROM end_date)::bigint FROM task_instance WHERE dag_id='daily_close' AND run_id='$POISON_RUN' AND task_id='dq_gate' AND state='failed' ORDER BY try_number DESC LIMIT 1")"

# 4. degrade-safely: ROW-LEVEL dead-letter (control row untouched) + alert
N1="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL AND source_pk->>'customer_id'='99990001'")"
N2="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL AND source_pk->>'customer_id'='99990002'")"
[ "$N1" = "1" ] || chaos_fail "expected exactly 1 OPEN incident for 99990001, found $N1"
[ "$N2" = "0" ] || chaos_fail "control row 99990002 was quarantined ($N2 incidents) — row-level targeting violated"
CTRL_IN_STG="$(whq "SELECT count(*) FROM staging.stg_file_customers WHERE customer_id='99990002'")"
[ "$CTRL_IN_STG" = "1" ] || chaos_fail "control row missing from staging (landed=$CTRL_IN_STG)"
chaos_ok "row-level proven: 1 OPEN incident for 99990001, 0 for control 99990002 (control landed clean)"
chaos_wait_alert_firing HeliosAirflowTaskFailure 180 "${DQ_FAIL_TS:-$(date +%s)}"

# 5. recovery: fix-and-reland (same filename, new content hash -> ledger relands)
chaos_note "RECOVERY: fix-and-reland $DROP_FILE (99990001 signup corrected to 2026-09-01)"
printf '%s\n' 'customer_id,email,full_name,country_code,signup_date,tier' \
  '99990001,chaos.99990001@example.com,Chaos Ninehundredone,US,2026-09-01,bronze' \
  '99990002,chaos.99990002@example.com,Chaos Ninehundredtwo,US,2026-09-01,gold' > "$WORK/$DROP_FILE"
docker compose run --rm -v "$WORK:/src:ro" filedrop-tools cp "/src/$DROP_FILE" /data/drop/ >/dev/null \
  || chaos_fail "fix-and-reland cp"
chaos_note "green close run_id=$GREEN_RUN"
chaos_close_until_green "$GREEN_RUN" 3 || exit 1
GREEN_RUN="$CHAOS_GREEN_RUN"
FIXED_SIGNUP="$(whq "SELECT signup_date::text FROM staging.stg_file_customers WHERE customer_id='99990001'")"
[ "$FIXED_SIGNUP" = "2026-09-01" ] || chaos_fail "staging 99990001 signup_date=$FIXED_SIGNUP, expected the relanded correction 2026-09-01"
chaos_ok "relanded correction measured: staging 99990001 signup_date=$FIXED_SIGNUP (no longer impossible)"

chaos_note "replay: resolving the dead-lettered incident"
docker compose run --rm dq python -m dq replay 2>&1 | tee /tmp/chaos-05-replay.log | tail -5
REPLAY_RC="${PIPESTATUS[0]}"
[ "$REPLAY_RC" = "0" ] || chaos_fail "dq-replay exited $REPLAY_RC"
OPEN_NOW="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
RESOLVED_NOW="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NOT NULL")"
[ "$OPEN_NOW" = "0" ] || chaos_fail "quarantine open=$OPEN_NOW after replay"
[ "$RESOLVED_NOW" = "$((RESOLVED_BASE + 1))" ] || chaos_fail "resolved count moved $RESOLVED_BASE -> $RESOLVED_NOW (expected exactly +1)"
chaos_ok "converged: quarantine open=0 resolved=$RESOLVED_NOW"

echo "[chaos] ---- run-etl driver transcripts (/tmp/chaos-etl-*.log) ----"
cat "$CHAOS_RED_LOG" "$CHAOS_GREEN_LOG" 2>/dev/null
cat /tmp/chaos-etl-${POISON_RUN}-a*.log /tmp/chaos-etl-${GREEN_RUN}-a*.log 2>/dev/null
echo "[chaos] ---- dq-replay transcript (/tmp/chaos-05-replay.log) ----"
cat /tmp/chaos-05-replay.log
chaos_ok "scenario 05 poison_csv PASSED (wall $(chaos_wall))"
