"""Fixtures for cdc-sink tests.

Tests never touch the live warehouse data: each pytest session creates a
dedicated `cdc_test` database on the same Postgres instance (warehouse-db) and
drops it at the end — the oltp conftest pattern. Connection settings come from
the environment (compose service env); there are no credentials in this file.
"""

from __future__ import annotations

import psycopg
import pytest

from cdc import config
from cdc.ensure import ensure_raw_tables

TEST_DB_NAME = "cdc_test"


@pytest.fixture(scope="session")
def admin():
    """Autocommit connection to the main warehouse DB (for CREATE/DROP DATABASE)."""
    cfg = config.warehouse_settings()
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def testdb(admin):
    admin.execute("DROP DATABASE IF EXISTS cdc_test WITH (FORCE)")
    admin.execute("CREATE DATABASE cdc_test")
    yield TEST_DB_NAME
    admin.execute("DROP DATABASE IF EXISTS cdc_test WITH (FORCE)")


@pytest.fixture()
def tconn(testdb):
    cfg = config.warehouse_settings()
    # autocommit: apply_events is called inside explicit transaction() blocks,
    # which must be real commits (Session-3 CRITIC lesson) — same as production.
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=testdb, autocommit=True)
    ensure_raw_tables(conn)
    # the scratch DB persists for the whole session; isolate each test
    with conn.transaction():
        conn.execute("TRUNCATE raw.cdc_users, raw.cdc_orders, raw.cdc_order_items, raw.cdc_payments")
    yield conn
    conn.close()
