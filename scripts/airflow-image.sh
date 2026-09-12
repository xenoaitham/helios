#!/usr/bin/env bash
# Build helios/airflow:2.10.5 — the extended scheduler image (ADR-010 D1).
#
# Stages the HOST's docker compose plugin (a static ELF) into airflow/ so the
# in-image compose parser is byte-identical to the one `make` uses, then builds
# the image, then (idempotently) re-owns the airflow_logs volume to AIRFLOW_UID
# — needed once after switching the airflow containers from the image-default
# uid 50000 to the socket-owner uid (AIRFLOW_UID in .env). Every step aborts
# loudly on failure; a vacuous PASS is worse than a FAIL.
set -euo pipefail
cd "$(dirname "$0")/.."
# Bind-source parity (ADR-010 D1) — see scripts/run-etl.sh.
export HELIOS_PROJECT_DIR="${HELIOS_PROJECT_DIR:-$(pwd)}"
set -a; [ -f .env ] && . ./.env; set +a

PLUGIN=""
for p in \
  /usr/libexec/docker/cli-plugins/docker-compose \
  /usr/lib/docker/cli-plugins/docker-compose \
  /usr/local/lib/docker/cli-plugins/docker-compose \
  /usr/local/libexec/docker/cli-plugins/docker-compose \
  "$HOME/.docker/cli-plugins/docker-compose"; do
  if [ -x "$p" ]; then PLUGIN="$p"; break; fi
done
if [ -z "$PLUGIN" ]; then
  echo "[airflow-image] FAIL: no docker compose plugin found on the host" >&2
  echo "[airflow-image] (looked in the standard cli-plugins paths; install" >&2
  echo "[airflow-image]  docker-compose-plugin or adjust the search list)" >&2
  exit 1
fi
echo "[airflow-image] staging host compose plugin: $PLUGIN"
cp "$PLUGIN" airflow/docker-compose
chmod 0755 airflow/docker-compose

echo "[airflow-image] building helios/airflow:2.10.5 (base: apache/airflow:2.10.5-python3.11 + compose plugin)"
docker compose build airflow-webserver

# One-time (idempotent) fix: the logs volume may hold dirs/files owned by other
# container uids (the image-default 50000 from earlier sessions, or an older
# AIRFLOW_UID choice). The airflow services run as the daemon's userns root
# (ADR-010 D1) — re-own the volume to 0:0 to match. No data touched.
echo "[airflow-image] re-owning airflow_logs volume to 0:0 (idempotent)"
docker compose run --rm --user 0:0 --no-deps --entrypoint "" \
  airflow-scheduler chown -R 0:0 /opt/airflow/logs

echo "[airflow-image] OK"
