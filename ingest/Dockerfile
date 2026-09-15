# HELIOS ingest image (ADR-006/007): one-shot batch extractors (SOAP / file /
# REST) into the warehouse raw zone. The same image powers `make test-ingest`,
# `make ingest-status` and the `make ingest-*` runs (docker compose run
# one-shots — Airflow takes over scheduling in Phase 3). Slim base, binary wheels.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini ./
COPY ingest/ ./ingest/
COPY tests/ ./tests/

# Default: run every extractor once (make ingest-all / compose run).
CMD ["python", "-m", "ingest.run", "--source", "all"]
