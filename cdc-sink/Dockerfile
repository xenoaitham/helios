# HELIOS cdc-sink image (ADR-005): long-running raw-zone sink consumer; the same
# image powers `make test-cdc` (pytest), `make cdc-status`, and `make cdc-verify`
# (one-shot `docker compose run` invocations). Slim base, binary psycopg wheel.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini ./
COPY cdc/ ./cdc/
COPY tests/ ./tests/

# Default: the sink loop (compose runs this for the always-on service).
CMD ["python", "-m", "cdc.sink"]
