"""Settings for the cdc-sink, sourced from the environment only (ADR-005).

Credentials never have defaults here — a missing variable fails fast with an
actionable message (oltp/db.py precedent). The Kafka Connect REST base URL is
validated against an explicit allowlist (scheme/host/port/path) before any
request is built anywhere in this package.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

CONNECTOR_NAME = "helios-oltp"

# Tables captured by the Debezium connector (ADR-005) and the raw-zone tables
# they land in. Topics are <prefix>.public.<table>, one partition each.
SOURCE_TABLES = ("users", "orders", "order_items", "payments")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"environment variable {name} is required (see .env.example / compose service env)"
        )
    return value


def _connect_settings(prefix: str, host_env: str, host_default: str) -> dict:
    return {
        "host": os.environ.get(host_env, host_default),
        "port": int(os.environ.get(f"{prefix}PGPORT", "5432")),
        "user": _require(f"{prefix}USER"),
        "password": _require(f"{prefix}PASSWORD"),
        "dbname": _require(f"{prefix}DB"),
    }


def warehouse_settings() -> dict:
    """psycopg kwargs for the warehouse (raw zone) connection."""
    return _connect_settings("WAREHOUSE_POSTGRES_", "WAREHOUSE_PGHOST", "warehouse-db")


def oltp_settings() -> dict:
    """psycopg kwargs for the OLTP source (used by verify/status only)."""
    return _connect_settings("OLTP_POSTGRES_", "OLTP_PGHOST", "oltp-db")


def kafka_bootstrap() -> str:
    return os.environ.get("CDC_KAFKA_BOOTSTRAP", "kafka:9092")


def group_id() -> str:
    return os.environ.get("CDC_GROUP_ID", "helios-cdc-sink")


def topic_prefix() -> str:
    return os.environ.get("CDC_TOPIC_PREFIX", "helios")


def topics() -> list[str]:
    return [f"{topic_prefix()}.public.{t}" for t in SOURCE_TABLES]


def cdc_db_user() -> str:
    return _require("CDC_DB_USER")


def cdc_db_password() -> str:
    return _require("CDC_DB_PASSWORD")


def oltp_db_host() -> str:
    """Hostname the CONNECTOR uses to reach the OLTP source (in-network)."""
    return os.environ.get("CDC_OLTP_HOST", "oltp-db")


def health_port() -> int:
    return int(os.environ.get("CDC_HEALTH_PORT", "8081"))


# The Connect REST endpoint lives inside the compose network. Only these hosts
# are acceptable targets; anything else is refused before a request is built.
_ALLOWED_CONNECT_HOSTS = frozenset({"cdc-connect", "localhost", "127.0.0.1"})


def validate_connect_url(url: str) -> str:
    """Return the canonical base URL if (and only if) it passes the allowlist."""
    parsed = urlparse(url)
    if parsed.scheme != "http":
        raise RuntimeError(f"CDC_CONNECT_URL scheme must be http, got: {parsed.scheme!r}")
    if parsed.hostname not in _ALLOWED_CONNECT_HOSTS:
        raise RuntimeError(
            f"CDC_CONNECT_URL host {parsed.hostname!r} is not allowlisted "
            f"(allowed: {sorted(_ALLOWED_CONNECT_HOSTS)})"
        )
    port = parsed.port if parsed.port is not None else 80
    if not 1 <= port <= 65535:
        raise RuntimeError(f"CDC_CONNECT_URL port out of range: {port}")
    if parsed.path not in ("", "/"):
        raise RuntimeError(f"CDC_CONNECT_URL path must be empty, got: {parsed.path!r}")
    return f"http://{parsed.hostname}:{port}"


def connect_url() -> str:
    return validate_connect_url(os.environ.get("CDC_CONNECT_URL", "http://cdc-connect:8083"))
