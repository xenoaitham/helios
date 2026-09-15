FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0 \
    SOAP_DB_PATH=/data/orders.db

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/docker-entrypoint.sh && mkdir -p /data /contract

EXPOSE 8000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
# 2 workers x 4 threads: SQLite WAL + busy_timeout handles concurrent writes for a
# single legacy service (see ADR-001 scale note).
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", \
     "--timeout", "120", "--access-logfile", "-", "app.wsgi:application"]
