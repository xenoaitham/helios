# HELIOS — extended Airflow scheduler image (ADR-010 D1).
#
# Not a new service: FROM the same apache/airflow:2.10.5-python3.11 the compose
# baseline has run since Phase 0. The only delta is the docker compose plugin so
# DAG tasks can invoke the SAME one-shot tool containers `make` runs:
#   cd /helios && docker compose run --rm ingest python -m ingest.run --source soap
#   cd /helios && docker compose build dbt && docker compose run --rm dbt build
#
# The base image already ships the docker CLI (27.5.1, API-negotiates with the
# host rootless daemon). The compose plugin binary is staged into this directory
# from the HOST at build time by `make airflow-image` (scripts/airflow-image.sh)
# so the in-image parser is byte-identical to the one make uses; it is a
# gitignored build artifact (~65 MB) and must never be committed. A build without
# the staged binary fails loudly at COPY — that is the intended guard.
FROM apache/airflow:2.10.5-python3.11

COPY --chmod=0755 docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
