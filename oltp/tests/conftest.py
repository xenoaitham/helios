"""Test fixtures for the OLTP source package.

Tests never touch the real `oltp` database: each pytest session creates a
dedicated `oltp_test` database on the same Postgres instance and drops it at
the end. Connection settings come from the environment (compose service env);
there are no credentials in this file.
"""

from __future__ import annotations

import psycopg
import pytest

from oltp import seed as oltp_seed
from oltp.db import connect_settings

TEST_DB_NAME = "oltp_test"
SMALL_USERS = 40
SMALL_ORDERS = 120


@pytest.fixture(scope="session")
def admin():
    """Autocommit connection to the main OLTP database (for CREATE/DROP DATABASE)."""
    cfg = connect_settings()
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def testdb(admin):
    admin.execute("DROP DATABASE IF EXISTS oltp_test WITH (FORCE)")
    admin.execute("CREATE DATABASE oltp_test")
    yield TEST_DB_NAME
    admin.execute("DROP DATABASE IF EXISTS oltp_test WITH (FORCE)")


@pytest.fixture()
def tconn(testdb):
    cfg = connect_settings()
    # autocommit: statements and seed/tick transaction blocks commit for real,
    # which WAL assertions depend on (no outer implicit transaction wrapping).
    conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=testdb, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture()
def seeded(tconn):
    # reset=True: with autocommit the previous test's rows persist, and the
    # seeder rightly refuses to double-load a non-empty database.
    oltp_seed.seed_database(tconn, users=SMALL_USERS, orders=SMALL_ORDERS, reset=True)
    yield tconn
