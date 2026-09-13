# HELIOS dq image (ADR-011 D3): Great Expectations gate over the frozen
# staging/marts relations, dead-lettering to dq.dq_quarantine. One-shot TOOL
# (compose service `dq`, profiles ["tools"]) driven by make dq-run /
# dq-replay / dq-status; the DAG `dq_gate` task runs the same container at the
# seam ADR-010 D3 reserved (downstream of dbt_build). Code is BAKED IN —
# rebuild after any change; the make dq-* targets depend on dq-image exactly
# like the dbt targets do.
#
# ADR-011 D9: the tool is a ZERO-filesystem-write design — ephemeral GX data
# context built in Python from env (no great_expectations.yml, no
# uncommitted/, no data docs); results live in the warehouse and stdout.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GX_ANALYTICS_ENABLED=false

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY dq/ ./dq/
COPY tests/ ./tests/

# No ENTRYPOINT: the compose command spells the subcommand explicitly
# (gate | replay | status | pytest | tools.probe_gx) like the ingest tool.
CMD ["python", "-m", "dq", "gate"]
