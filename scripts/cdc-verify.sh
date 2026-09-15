#!/usr/bin/env bash
# ADR-005 DoD live verification — every claim below is a measured real run and
# its JSON lines feed EVIDENCE/phase-2-cdc.md:
#   [1] controlled window: mutator stopped, snapshot drained (consumer lag = 0)
#   [2] baseline compare: oltp vs raw per-table counts + money checksums
#   [3] marker INSERT/UPDATE/DELETE latency into raw (DoD: "within seconds")
#   [4] replay safety: reset the sink group to the pre-marker offsets, force a
#       full re-consumption of that window, raw state must be unchanged
set -uo pipefail
cd "$(dirname "$0")/.."

set -a; [ -f .env ] && . ./.env; set +a

RUN_SINK="docker compose run --rm cdc-sink"
KAFKA_CG="docker exec helios-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092"
GROUP="helios-cdc-sink"
TOPICS="helios.public.users helios.public.orders helios.public.order_items helios.public.payments"

restore() {
  docker compose start cdc-sink >/dev/null 2>&1 || true
  docker compose start oltp-mutator >/dev/null 2>&1 || true
}
trap restore EXIT

echo "===== [1/4] controlled window: stop mutator, drain snapshot ====="
docker compose stop oltp-mutator >/dev/null
if ! $RUN_SINK python -m cdc.verify wait-catchup --timeout 2400; then
  echo "[cdc-verify] FAIL: sink did not catch up" >&2
  exit 1
fi

echo "===== [2/4] baseline compare (oltp vs raw) ====="
if ! $RUN_SINK python -m cdc.verify baseline-compare; then
  echo "[cdc-verify] FAIL: baseline mismatch" >&2
  exit 1
fi

echo "===== [2b/4] record pre-marker offsets (the replay target) ====="
declare -A RESET_OFF=()
while IFS= read -r line; do
  topic="$(echo "$line" | awk '{print $2}')"
  offset="$(echo "$line" | awk '{print $4}')"
  case "$topic" in
    helios.public.*) [ "$offset" != "-" ] && RESET_OFF["$topic"]="$offset" ;;
  esac
done < <($KAFKA_CG --describe --group "$GROUP" 2>/dev/null)
for t in "${!RESET_OFF[@]}"; do echo "[cdc-verify] replay target $t @ offset ${RESET_OFF[$t]}"; done
for t in $TOPICS; do
  # a topic absent from describe has no commits: its replay start is simply
  # the earliest offset, so there is nothing to reset
  [ -z "${RESET_OFF[$t]:-}" ] && echo "[cdc-verify] replay target $t @ earliest (no commits)"
done
if [ "${#RESET_OFF[@]}" -eq 0 ]; then
  echo "[cdc-verify] FAIL: no committed offsets found for group $GROUP" >&2
  exit 1
fi

echo "===== [3/4] marker INSERT/UPDATE/DELETE latency (budget 90s each) ====="
if ! $RUN_SINK python -m cdc.verify marker --budget 90; then
  echo "[cdc-verify] FAIL: marker not visible in raw within budget" >&2
  exit 1
fi

echo "===== [4/4] replay safety: reset offsets to [2b], re-consume, compare ====="
EXPECT="$($RUN_SINK python -m cdc.verify replay-record)"
echo "[cdc-verify] pre-replay marker state: $EXPECT"
if [ -z "$EXPECT" ] || [ "$EXPECT" = "null" ]; then
  echo "[cdc-verify] FAIL: no marker state recorded" >&2
  exit 1
fi

docker compose stop cdc-sink >/dev/null
for t in "${!RESET_OFF[@]}"; do
  # Kafka 3.9: --reset-offsets is the action, --to-offset the target; the
  # partition is part of the --topic spec (topic:partition), there is no --partition flag
  if ! $KAFKA_CG --reset-offsets --to-offset "${RESET_OFF[$t]}" --group "$GROUP" --topic "$t:0" --execute >/dev/null; then
    echo "[cdc-verify] FAIL: offset reset for $t failed" >&2
    exit 1
  fi
  echo "[cdc-verify] reset $t -> offset ${RESET_OFF[$t]}"
done
docker compose start cdc-sink >/dev/null
if ! $RUN_SINK python -m cdc.verify wait-catchup --timeout 900; then
  echo "[cdc-verify] FAIL: sink did not drain the replay window" >&2
  exit 1
fi
if ! $RUN_SINK python -m cdc.verify replay-assert --expect "$EXPECT"; then
  echo "[cdc-verify] FAIL: raw state drifted across replay" >&2
  exit 1
fi

echo "===== [4b/4] baseline compare after replay ====="
if ! $RUN_SINK python -m cdc.verify baseline-compare; then
  echo "[cdc-verify] FAIL: baseline mismatch after replay" >&2
  exit 1
fi

docker compose start oltp-mutator >/dev/null 2>&1 || true
echo "===== [cdc-verify] PASS: insert/update/delete landed, replay converged ====="
