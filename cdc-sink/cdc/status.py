"""`python -m cdc.status` — one-screen CDC control-plane report (ADR-005).

Shows: connector + task state, per-topic consumer-group lag, replication-slot
retention (WAL held while the sink is behind), and raw-zone row counts. All
SQL is a single static literal at its call site. Lag uses the Admin API (never
joins the group — see cdc/lag.py).
"""

from __future__ import annotations

import sys

import psycopg

from cdc import config, lag, register


def _connector_line() -> str:
    try:
        status = register.connector_status()
        conn_state = status.get("connector", {}).get("state", "?")
        task_states = ",".join(t.get("state", "?") for t in status.get("tasks", [])) or "no-tasks"
        return f"[cdc] connector {config.CONNECTOR_NAME}: connector={conn_state} tasks={task_states}"
    except Exception as exc:
        return f"[cdc] connector status UNAVAILABLE ({exc!r})"


def _lag_lines() -> list[str]:
    try:
        total, per_topic = lag.group_lag()
    except Exception as exc:
        return [f"[cdc] lag UNAVAILABLE ({exc!r})"]
    if not per_topic:
        return ["[cdc] lag: no capture topics exist yet"]
    lines = [f"[cdc] lag {topic}: lag={value}" for topic, value in per_topic.items()]
    lines.append(f"[cdc] lag TOTAL={total}")
    return lines


def _slot_line() -> str:
    cfg = config.oltp_settings()
    try:
        with psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True) as conn:
            row = conn.execute("SELECT slot_name, active, restart_lsn::text, confirmed_flush_lsn::text, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained FROM pg_replication_slots WHERE slot_name = 'helios_cdc_slot'").fetchone()
        if row is None:
            return "[cdc] slot helios_cdc_slot: NOT CREATED (connector never started?)"
        return f"[cdc] slot {row[0]}: active={row[1]} restart_lsn={row[2]} confirmed_flush={row[3]} retained_wal={row[4]}"
    except Exception as exc:
        return f"[cdc] slot UNAVAILABLE ({exc!r})"


def _raw_counts_line() -> str:
    cfg = config.warehouse_settings()
    try:
        with psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True) as conn:
            row = conn.execute("SELECT (SELECT count(*) FROM raw.cdc_users) AS users, (SELECT count(*) FROM raw.cdc_orders) AS orders, (SELECT count(*) FROM raw.cdc_order_items) AS order_items, (SELECT count(*) FROM raw.cdc_payments) AS payments").fetchone()
        return f"[cdc] raw rows: users={row[0]} orders={row[1]} order_items={row[2]} payments={row[3]}"
    except Exception as exc:
        return f"[cdc] raw counts UNAVAILABLE ({exc!r})"


def main() -> int:
    print(_connector_line())
    for line in _lag_lines():
        print(line)
    print(_slot_line())
    print(_raw_counts_line())
    return 0


if __name__ == "__main__":
    sys.exit(main())
