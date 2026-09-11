# HELIOS dbt image (ADR-008 D5): dbt staging over the raw zone. One-shot tool
# image (compose service `dbt`, profiles ["tools"]) driven by make dbt-build /
# dbt-test / dbt-freshness. Code is BAKED IN — rebuild the image after any
# project change; the make dbt-* targets depend on `dbt-image` so this cannot
# be forgotten. No dbt packages on purpose (ADR-008 D5): hermetic, the runner
# never fetches anything at run time.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY project/ ./

ENTRYPOINT ["dbt"]
CMD ["build"]
