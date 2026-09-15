#!/usr/bin/env bash
# Wait until every long-running HELIOS container reports healthy, or fail loudly.
set -uo pipefail
cd "$(dirname "$0")/.."

# oltp-mutator is gated last: it only starts after oltp-seed completed, so a
# healthy mutator transitively proves the 5M-row seed finished (fresh clones
# need headroom for that seed -> WAIT_TIMEOUT=900 by default).
SERVICES=(oltp-db warehouse-db airflow-db kafka airflow-webserver airflow-scheduler soap-service oltp-mutator rest-mock cdc-connect cdc-sink marquez-db marquez-api marquez-web statsd-exporter prometheus grafana)
TIMEOUT="${WAIT_TIMEOUT:-900}"
START=$(date +%s)

for svc in "${SERVICES[@]}"; do
  container="helios-${svc}"
  while :; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
      echo "[wait] healthy: $container"
      break
    fi
    state="$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || true)"
    if [ "$state" = "exited" ]; then
      echo "[wait] ERROR: $container exited before becoming healthy. Last logs:" >&2
      docker logs --tail 30 "$container" >&2 || true
      exit 1
    fi
    if [ $(( $(date +%s) - START )) -gt "$TIMEOUT" ]; then
      echo "[wait] ERROR: timed out after ${TIMEOUT}s waiting for $container (health=$status state=$state)." >&2
      docker logs --tail 30 "$container" >&2 || true
      exit 1
    fi
    sleep 5
  done
done
echo "[wait] all HELIOS containers healthy."
