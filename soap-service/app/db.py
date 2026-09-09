"""SQLite storage for the legacy OrderManagement store.

Money is stored as integer cents (``total_cents`` / ``unit_price_cents``) — no
float money anywhere. Timestamps are naive UTC ``YYYY-MM-DD HH:MM:SS`` strings,
which makes date-range filters plain lexicographic SQL comparisons.
"""
import os
import sqlite3
from contextlib import contextmanager

DEFAULT_DB_PATH = "/data/orders.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id          INTEGER PRIMARY KEY,
    customer_id       INTEGER NOT NULL,
    status            TEXT    NOT NULL,
    total_cents       INTEGER NOT NULL,
    currency          TEXT    NOT NULL DEFAULT 'USD',
    client_reference  TEXT,
    client_ref_hash   TEXT,
    created_at        TEXT    NOT NULL,
    updated_at        TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_client_reference
    ON orders(client_reference) WHERE client_reference IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS ix_orders_created_at ON orders(created_at);

CREATE TABLE IF NOT EXISTS order_items (
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    line_no          INTEGER NOT NULL,
    product_id       INTEGER NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS order_status_history (
    history_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL REFERENCES orders(order_id),
    from_status TEXT,
    to_status   TEXT NOT NULL,
    note        TEXT,
    changed_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_history_order ON order_status_history(order_id);
"""


def db_path():
    return os.environ.get("SOAP_DB_PATH", DEFAULT_DB_PATH)


def connect(path=None):
    """Open a connection with the pragmas the service relies on.

    ``isolation_level=None`` = autocommit; transactions are managed explicitly
    via :func:`tx` so fault paths roll back deterministically.
    """
    conn = sqlite3.connect(path or db_path(), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_schema(conn):
    conn.executescript(SCHEMA)


@contextmanager
def tx(conn):
    """BEGIN IMMEDIATE transaction: serializes writers, rolls back on faults."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()
