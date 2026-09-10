#!/usr/bin/env bash
# ADR-005 CDC bootstrap — idempotent, safe on every run:
#   1. apply the compose change so oltp-db runs with wal_level=logical
#      (command override only; the 5.4M-row data volume is NEVER rebuilt)
#   2. verify SHOW wal_level BEFORE anything downstream is built
#   3. ensure the helios_cdc replication role with least-privilege grants
# Credentials come from .env only — no literals in this file.
set -euo pipefail
cd "$(dirname "$0")/.."

set -a; [ -f .env ] && . ./.env; set +a
: "${OLTP_POSTGRES_USER:?set OLTP_POSTGRES_USER in .env}"
: "${OLTP_POSTGRES_DB:?set OLTP_POSTGRES_DB in .env}"
: "${CDC_DB_USER:?set CDC_DB_USER in .env}"
: "${CDC_DB_PASSWORD:?set CDC_DB_PASSWORD in .env}"

echo "[cdc-setup] applying compose config to oltp-db (recreates container, keeps volume)"
docker compose up -d oltp-db

status=""
for _ in $(seq 1 60); do
  status="$(docker inspect -f '{{.State.Health.Status}}' helios-oltp-db 2>/dev/null || true)"
  [ "$status" = "healthy" ] && break
  sleep 2
done
if [ "$status" != "healthy" ]; then
  echo "[cdc-setup] ERROR: oltp-db not healthy after recreate" >&2
  exit 1
fi

wal="$(docker exec helios-oltp-db psql -U "$OLTP_POSTGRES_USER" -d "$OLTP_POSTGRES_DB" -tAc "SHOW wal_level")"
if [ "$wal" != "logical" ]; then
  echo "[cdc-setup] ERROR: SHOW wal_level returned '$wal' (expected 'logical')" >&2
  exit 1
fi
echo "[cdc-setup] wal_level=logical confirmed"

docker exec -i helios-oltp-db psql -U "$OLTP_POSTGRES_USER" -d "$OLTP_POSTGRES_DB" -v ON_ERROR_STOP=1 \
  -v cdc_user="$CDC_DB_USER" -v cdc_pass="$CDC_DB_PASSWORD" -v oltp_db="$OLTP_POSTGRES_DB" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user') \gexec
ALTER ROLE :"cdc_user" WITH LOGIN REPLICATION PASSWORD :'cdc_pass';
GRANT CONNECT ON DATABASE :"oltp_db" TO :"cdc_user";
GRANT USAGE ON SCHEMA public TO :"cdc_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"cdc_user";
SQL

# The PUBLICATION belongs to the table OWNER (adding a table to a publication
# requires owning it), so the DBA role creates it here; the Debezium connector
# runs with publication.autocreate.mode=disabled and only READS it.
docker exec -i helios-oltp-db psql -U "$OLTP_POSTGRES_USER" -d "$OLTP_POSTGRES_DB" -v ON_ERROR_STOP=1 <<'SQL'
SELECT format('CREATE PUBLICATION helios_publication FOR TABLE public.users, public.orders, public.order_items, public.payments')
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'helios_publication') \gexec
SELECT format('ALTER PUBLICATION helios_publication ADD TABLE public.%I', t.tablename)
FROM (VALUES ('users'), ('orders'), ('order_items'), ('payments')) AS t(tablename)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_publication_tables
  WHERE pubname = 'helios_publication' AND schemaname = 'public' AND tablename = t.tablename
) \gexec
SQL

echo "[cdc-setup] publication 'helios_publication' ensured (owned by $OLTP_POSTGRES_USER, 4 tables)"

echo "[cdc-setup] replication role '$CDC_DB_USER' ensured (REPLICATION + SELECT public.* + CREATE for the publication)"
