"""Fixtures for ingest lib tests.

Tests never touch the live warehouse data: each pytest session creates a
dedicated `ingest_test` database on the same Postgres instance (warehouse-db)
and drops it at the end — the cdc-sink conftest pattern. Connection settings
come from the environment (compose service env); there are no credentials in
this file. File-extractor tests write fixtures into pytest's tmp_path INSIDE
the test container — never the real filedrop volume.
"""

from __future__ import annotations

import psycopg
import pytest

from ingest import config
from ingest.ensure import ensure_ingest_tables

TEST_DB_NAME = "ingest_test"


@pytest.fixture(scope="session")
def admin():
    """Autocommit connection to the main warehouse DB (for CREATE/DROP DATABASE)."""
    cfg = config.warehouse_settings()
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def testdb(admin):
    admin.execute("DROP DATABASE IF EXISTS ingest_test WITH (FORCE)")
    admin.execute("CREATE DATABASE ingest_test")
    yield TEST_DB_NAME
    admin.execute("DROP DATABASE IF EXISTS ingest_test WITH (FORCE)")


@pytest.fixture()
def tconn(testdb):
    cfg = config.warehouse_settings()
    # autocommit: landing calls run inside explicit transaction() blocks, which
    # must be real commits (Session-3 CRITIC lesson) — same as production.
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=testdb, autocommit=True)
    ensure_ingest_tables(conn)
    # the scratch DB persists for the whole session; isolate each test
    with conn.transaction():
        conn.execute("TRUNCATE raw.ingest_watermarks, raw.ingest_loads, raw.ingest_files, raw.ingest_quarantine, raw.soap_orders, raw.file_customers, raw.file_products, raw.rest_products, raw.rest_promotions RESTART IDENTITY")
    yield conn
    conn.close()
