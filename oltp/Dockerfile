# HELIOS oltp-tools image (ADR-002): powers both the one-shot seeder (oltp-seed)
# and the always-on mutation loop (oltp-mutator). Slim base, binary psycopg wheel.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini ./
COPY oltp/ ./oltp/
COPY tests/ ./tests/

# Default: one-shot idempotent seed (compose overrides for the mutator / tests).
CMD ["python", "-m", "oltp.seed", "--if-empty", "--report"]
