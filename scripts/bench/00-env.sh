#!/usr/bin/env bash
# bench leg 00 — environment + hardware + pre-state inventory (ADR-015 D4).
# Every hardware fact is printed as command output (never prose), measured
# fresh on every pass: this is the header EVIDENCE/metrics.md cites.
set -uo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

bench_preflight

echo "=== date ==="
date -u
echo
echo "=== CPU (lscpu) ==="
lscpu | grep -E 'Model name|^CPU\(s\)|Thread\(s\) per core|Core\(s\) per socket|Socket\(s\)|CPU max MHz'
echo
echo "=== RAM (free -h) ==="
free -h
echo
echo "=== disk (df -h / + rotational flag) ==="
df -h /
ROOT_SRC="$(findmnt -no SOURCE /)"
bench_note "root filesystem device: $ROOT_SRC"
lsblk -no ROTA,SIZE,TYPE "${ROOT_SRC}"
# lsblk pads with leading spaces — strip before comparing (found-by-verification
# 2026-09-14: the unstripped comparison mislabeled this SSD as ROTA=1).
[ "$(lsblk -no ROTA "$ROOT_SRC" | tr -d '[:space:]')" = "0" ] && bench_ok "root device is non-rotating (SSD-class)" \
  || bench_note "root device reports ROTA=1 (spinning) — recorded, not judged"
echo
echo "=== OS / kernel ==="
head -2 /etc/os-release
uname -r
echo
echo "=== docker daemon identity (rootless context) ==="
docker context show
docker info --format 'SecurityOptions: {{.SecurityOptions}}' || bench_note "docker info probe unavailable"
docker info --format 'Rootless detected: {{.SecurityOptions}}' | grep -q rootless && bench_ok "daemon runs rootless (SecurityOptions contains rootless)" || bench_note "daemon rootless status not self-reported by docker info"
bench_note "RUNBOOK §5: rootless dockerd (slirp4netns/fuse-overlayfs), no cgroup resource limits, published ports bind host loopback only — numbers on this host carry those caveats"
echo
echo "=== containers (17 expected healthy) ==="
for c in $BENCH_PREFLIGHT_CONTAINERS; do
  echo "helios-$c $(bench_health "$c")"
done
echo
echo "=== pre-state inventory (measured) ==="
for t in stg_users stg_orders stg_order_items stg_payments stg_file_customers stg_file_products stg_rest_products stg_rest_promotions stg_soap_orders; do
  echo "staging.$t $(whq "SELECT count(*) FROM staging.$t")"
done
for t in fct_orders fct_order_items dim_customer dim_product dim_date; do
  echo "marts.$t $(whq "SELECT count(*) FROM marts.$t")"
done
echo "snapshots.customers_snapshot $(whq "SELECT count(*) FROM snapshots.customers_snapshot")"
STG_SUM="$(whq "SELECT count(*) FROM (SELECT 1 FROM staging.stg_users UNION ALL SELECT 1 FROM staging.stg_orders UNION ALL SELECT 1 FROM staging.stg_order_items UNION ALL SELECT 1 FROM staging.stg_payments UNION ALL SELECT 1 FROM staging.stg_file_customers UNION ALL SELECT 1 FROM staging.stg_file_products UNION ALL SELECT 1 FROM staging.stg_rest_products UNION ALL SELECT 1 FROM staging.stg_rest_promotions UNION ALL SELECT 1 FROM staging.stg_soap_orders) s")"
MART_SUM="$(whq "SELECT count(*) FROM (SELECT 1 FROM marts.fct_orders UNION ALL SELECT 1 FROM marts.fct_order_items UNION ALL SELECT 1 FROM marts.dim_customer UNION ALL SELECT 1 FROM marts.dim_product UNION ALL SELECT 1 FROM marts.dim_date) m")"
CDC_SUM="$(whq "SELECT count(*) FROM (SELECT 1 FROM raw.cdc_users UNION ALL SELECT 1 FROM raw.cdc_orders UNION ALL SELECT 1 FROM raw.cdc_order_items UNION ALL SELECT 1 FROM raw.cdc_payments) r")"
echo "staging TOTAL: $STG_SUM"
echo "marts TOTAL:   $MART_SUM"
echo "raw.cdc_* TOTAL (envelope): $CDC_SUM"
echo
bench_snapshot_invariant
bench_quarantine_zero_open
echo
echo "=== dq gate scope (the 6 suite tables, ADR-011) ==="
SCOPE_SUM=0
for t in staging.stg_payments staging.stg_orders staging.stg_order_items staging.stg_file_customers staging.stg_soap_orders marts.fct_orders; do
  n="$(whq "SELECT count(*) FROM $t")"
  echo "$t $n"
done
echo
echo "[bench] leg 00-env DONE"
