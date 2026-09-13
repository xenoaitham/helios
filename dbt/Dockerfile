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

# ADR-012 D4: the OpenLineage wrapper IS the invocation path — dbt-ol passes
# arguments through to dbt, then parses the run artifacts and emits
# run/dataset/column-lineage events. ONE command wraps dbt for `make
# dbt-build` AND the DAG's dbt_build task (build-then-run convention,
# ADR-010 D1); no second invocation path exists.
ENTRYPOINT ["dbt-ol"]
CMD ["build"]
