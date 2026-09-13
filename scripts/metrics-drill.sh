#!/usr/bin/env bash
# make metrics-drill (ADR-013 D6/D7): the scripted alert FIRE-DRILL.
#
# A real rule firing on a real outage, not a canned screenshot:
#   1. stop helios-statsd-exporter (a genuinely scraped target),
#   2. poll /api/v1/alerts until HeliosScrapeTargetDown is FIRING,
#   3. restart it, wait healthy, assert the alert recovers.
# Any step failing exits nonzero loudly (and the exporter is restarted
# best-effort so the drill cannot leave the stack degraded).
#
# While the exporter is down, airflow's StatsD datagrams are DROPPED, not
# queued (ADR-013 D7) — metrics for that window are lost forever; that is
# the documented contract, and the drill doubles as its proof shape.
set -uo pipefail
cd "$(dirname "$0")/.."
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

PROM="http://localhost:${PROMETHEUS_PORT:-9091}"
OUT=EVIDENCE/phase-4-metrics/drill
mkdir -p "$OUT"

fail() { echo "[metrics-drill] FAIL: $*"; exit 1; }
ok()   { echo "[metrics-drill] ok: $*"; }

command -v curl >/dev/null || fail "curl not found on host"
command -v python3 >/dev/null || fail "python3 not found on host (JSON parsing)"

H="$(docker inspect -f '{{.State.Health.Status}}' helios-statsd-exporter 2>/dev/null || echo missing)"
[ "$H" = "healthy" ] || fail "helios-statsd-exporter health=$H (make up first)"
curl -sf --max-time 15 "$PROM/api/v1/alerts" >/dev/null || fail "prometheus not answering on $PROM"

# the drill's own honesty: this only works if the rule is a REAL one — the
# baseline dump shows its state before we act on the world.
curl -sf --max-time 15 "$PROM/api/v1/alerts" > "$OUT/alerts-baseline.json" || fail "baseline alerts dump"
ok "baseline alert states dumped (alerts-baseline.json)"

echo "[metrics-drill] stopping helios-statsd-exporter (the drill act)"
docker compose stop statsd-exporter >/dev/null || fail "docker compose stop statsd-exporter"

FIRING=0
deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl -sf --max-time 10 "$PROM/api/v1/alerts" > "$OUT/alerts-firing.json" 2>/dev/null; then
    state="$(python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
states = [a["state"] for a in d["data"]["alerts"] if a["labels"].get("alertname") == "HeliosScrapeTargetDown"]
print("firing" if "firing" in states else "waiting")
' "$OUT/alerts-firing.json" 2>/dev/null || echo waiting)"
    if [ "$state" = "firing" ]; then FIRING=1; break; fi
  fi
  echo "[metrics-drill]   waiting for HeliosScrapeTargetDown to fire ..."
  sleep 10
done
if [ "$FIRING" -ne 1 ]; then
  docker compose start statsd-exporter >/dev/null 2>&1 || true
  fail "HeliosScrapeTargetDown did not FIRE within 120s of stopping statsd-exporter (rule real? scrape interval? 'for: 30s'?)"
fi
ok "HeliosScrapeTargetDown FIRING on the real outage (evidence: alerts-firing.json)"
sleep 2
curl -sf --max-time 15 "$PROM/api/v1/alerts" > "$OUT/alerts-firing.json" || fail "final firing-state dump"
docker inspect helios-statsd-exporter -f 'target health during drill: {{.State.Health.Status}}' > "$OUT/stopped-target-state.txt" 2>/dev/null || true

echo "[metrics-drill] restarting helios-statsd-exporter"
docker compose start statsd-exporter >/dev/null || fail "docker compose start statsd-exporter"
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  H="$(docker inspect -f '{{.State.Health.Status}}' helios-statsd-exporter 2>/dev/null || echo missing)"
  [ "$H" = "healthy" ] && break
  sleep 5
done
[ "$H" = "healthy" ] || fail "statsd-exporter did not return to healthy within 90s of restart"

RESOLVED=0
deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl -sf --max-time 10 "$PROM/api/v1/alerts" > "$OUT/alerts-recovered.json" 2>/dev/null; then
    state="$(python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
states = [a["state"] for a in d["data"]["alerts"] if a["labels"].get("alertname") == "HeliosScrapeTargetDown"]
print("still-firing" if "firing" in states else "recovered")
' "$OUT/alerts-recovered.json" 2>/dev/null || echo waiting)"
    if [ "$state" = "recovered" ]; then RESOLVED=1; break; fi
  fi
  echo "[metrics-drill]   waiting for HeliosScrapeTargetDown to resolve ..."
  sleep 10
done
[ "$RESOLVED" -eq 1 ] || fail "HeliosScrapeTargetDown still firing 120s after the target recovered"
ok "alert resolved after recovery (evidence: alerts-recovered.json)"

echo "[metrics-drill] PASSED — the rule genuinely fired on a real outage and recovered. Evidence in $OUT/"
