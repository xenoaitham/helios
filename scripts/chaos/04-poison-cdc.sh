#!/usr/bin/env bash
# chaos-04 poison_cdc (ADR-014 D3): re-run the Session-9 DRILL-1 injection as
# scripted chaos WITH the observability cross-link (ADR-014 D6).
#
# Poison: payment 764768 (order 664768 — DELIVERED, terminal, mutator-inert;
# the Session-9 target) amount 377.12 → −777.77 through the mutator's own
# write path (a plain OLTP UPDATE; CDC carries it).
#
# Degrade-safely mechanism (named BEFORE the act): the SEMANTIC gate —
# dq_gate exits nonzero, the DAG is red AT THE GATE (ADR-011 D8), the row is
# dead-lettered to dq.dq_quarantine with provenance, and HeliosAirflowTaskFailure
# FIRES on the same event (ADR-013 D6 rule) — measured here end-to-end with
# its latency, for the first time on a pipeline failure.
# Convergence proof: source fixed (amount back to 377.12, CDC carries the
# correction) → next close green 5/5 → make dq-replay resolves the incident
# (0 OPEN measured, resolved count +1, the two Session-9 RESOLVED rows
# untouched) → the alert resolves when increase([10m]) rolls off (measured).
set -uo pipefail
. "$(dirname "$0")/lib.sh"
chaos_note "scenario 04 poison_cdc: payment 764768 -> -777.77; recovery = source fix + green close + dq-replay"
chaos_preflight

PAY_ID=764768
POISON_RUN="chaos04-red-$(date -u +%Y%m%dT%H%M%SZ)"
GREEN_RUN="chaos04-green-$(date -u +%Y%m%dT%H%M%SZ)"
ETL_LOG="/tmp/chaos-04-etl.log"

# 1. pre-state
AMOUNT_BASE="$(oltpq "SELECT amount FROM public.payments WHERE payment_id=$PAY_ID")"
[ "$AMOUNT_BASE" = "377.12" ] || chaos_fail "payment $PAY_ID amount=$AMOUNT_BASE, expected the Session-9 restored value 377.12 — refusing to poison unknown state"
OPEN_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
RESOLVED_BASE="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NOT NULL")"
[ "$OPEN_BASE" = "0" ] || chaos_fail "dq_quarantine open=$OPEN_BASE (expected 0 before chaos)"
chaos_ok "pre-state: payment $PAY_ID amount=$AMOUNT_BASE; quarantine open=0 resolved=$RESOLVED_BASE"

# 2. chaos act: poison at the source; CDC carries it
chaos_note "CHAOS ACT: OLTP UPDATE payment $PAY_ID amount -> -777.77"
POISON_TS="$(date +%s)"
oltpq "UPDATE public.payments SET amount=-777.77 WHERE payment_id=$PAY_ID" >/dev/null
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  N="$(whq "SELECT count(*) FROM raw.cdc_payments WHERE pk->>'payment_id'='$PAY_ID' AND op='u' AND after->>'amount'='-777.77'")"
  [ "$N" -ge 1 ] && break
  sleep 5
done
[ "$N" -ge 1 ] || chaos_fail "CDC never carried the poison after-image within 90s"
chaos_ok "CDC carried the poison: raw.cdc_payments after-image amount=-777.77 ($N event(s))"

# 3. poison close: run-etl must FAIL at dq_gate (loud)
chaos_note "triggering the POISON close run_id=$POISON_RUN (failure at the gate is the expected, asserted outcome; straddle retries attributed)"
chaos_close_until_gate_red "$POISON_RUN" 3 || exit 1
POISON_RUN="$CHAOS_RED_RUN"
DBT_TI="$(chaos_task_state daily_close "$POISON_RUN" dbt_build)"
DQ_TI="$(chaos_task_state daily_close "$POISON_RUN" dq_gate)"
[ "${DBT_TI%%|*}" = "success" ] || chaos_note "NOTE: dbt_build state=${DBT_TI%%|*} (dbt is blind to the sign policy by design — ADR-011 D2)"
chaos_ok "red AT THE GATE: dbt_build=$DBT_TI dq_gate=$DQ_TI (gate re-ran once and re-detected, ADR-011 residual)"

DQ_FAIL_TS="$(afdbq "SELECT extract(epoch FROM end_date)::bigint FROM task_instance WHERE dag_id='daily_close' AND run_id='$POISON_RUN' AND task_id='dq_gate' AND state='failed' ORDER BY try_number DESC LIMIT 1")"

# 4. degrade-safely: dead-letter + alert
N="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL AND source_pk->>'payment_id'='$PAY_ID'")"
[ "$N" = "1" ] || chaos_fail "expected exactly 1 OPEN incident for payment $PAY_ID, found $N"
chaos_ok "dead-lettered with provenance: 1 OPEN incident source_pk->>'payment_id'=$PAY_ID"
chaos_note "asserting the ALERTING path end-to-end on a pipeline failure (first scripted proof)"
chaos_wait_alert_firing HeliosAirflowTaskFailure 180 "${DQ_FAIL_TS:-$POISON_TS}"
chaos_dump_alerts

# 5. recovery: fix at the source, green close, replay
chaos_note "RECOVERY: OLTP UPDATE payment $PAY_ID amount -> 377.12"
oltpq "UPDATE public.payments SET amount=377.12 WHERE payment_id=$PAY_ID" >/dev/null
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  N="$(whq "SELECT count(*) FROM raw.cdc_payments WHERE pk->>'payment_id'='$PAY_ID' AND op='u' AND after->>'amount'='377.12'")"
  [ "$N" -ge 1 ] && break
  sleep 5
done
[ "$N" -ge 1 ] || chaos_fail "CDC never carried the fix after-image within 90s"
chaos_ok "CDC carried the fix"

chaos_note "green close run_id=$GREEN_RUN"
chaos_close_until_green "$GREEN_RUN" 3 || exit 1
GREEN_RUN="$CHAOS_GREEN_RUN"

chaos_note "replay: resolving the dead-lettered incident (dq-replay)"
docker compose run --rm dq python -m dq replay 2>&1 | tee /tmp/chaos-04-replay.log | tail -5
REPLAY_RC="${PIPESTATUS[0]}"
[ "$REPLAY_RC" = "0" ] || chaos_fail "dq-replay exited $REPLAY_RC (incident still open?)"
OPEN_NOW="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL")"
RESOLVED_NOW="$(whq "SELECT count(*) FROM dq.dq_quarantine WHERE resolved_at IS NOT NULL")"
[ "$OPEN_NOW" = "0" ] || chaos_fail "quarantine open=$OPEN_NOW after replay"
[ "$RESOLVED_NOW" = "$((RESOLVED_BASE + 1))" ] || chaos_fail "resolved count moved $RESOLVED_BASE -> $RESOLVED_NOW (expected exactly +1)"
chaos_ok "converged: quarantine open=0 resolved=$RESOLVED_NOW (the drill's own incident resolved; history preserved)"

# 6. alert resolve semantics (measured once, documented ~10m: increase([10m]) rolls off)
chaos_note "waiting for HeliosAirflowTaskFailure to resolve (increase([10m]) window rolls off; measured once here)"
chaos_wait_alert_resolved HeliosAirflowTaskFailure 900

echo "[chaos] ---- run-etl driver transcripts (/tmp/chaos-etl-*.log) ----"
cat "$CHAOS_RED_LOG" "$CHAOS_GREEN_LOG" 2>/dev/null
cat /tmp/chaos-etl-${POISON_RUN}-a*.log /tmp/chaos-etl-${GREEN_RUN}-a*.log 2>/dev/null
echo "[chaos] ---- dq-replay transcript (/tmp/chaos-04-replay.log) ----"
cat /tmp/chaos-04-replay.log
chaos_ok "scenario 04 poison_cdc PASSED (wall $(chaos_wall))"
