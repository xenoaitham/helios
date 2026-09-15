"""Connection settings for the OLTP source database.

Credentials come from the environment only (compose injects OLTP_POSTGRES_*
from .env); there are no fallback credentials here by design — a missing var
fails fast with an actionable message.
"""

from __future__ import annotations

import os

import psycopg


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"environment variable {name} is required (see .env.example / compose service env)"
        )
    return value


def connect_settings() -> dict:
    """Kwargs for psycopg.connect(); host/port have dev defaults, credentials do not."""
    return {
        "host": os.environ.get("OLTP_PGHOST", "oltp-db"),
        "port": int(os.environ.get("OLTP_PGPORT", "5432")),
        "user": _require("OLTP_POSTGRES_USER"),
        "password": _require("OLTP_POSTGRES_PASSWORD"),
        "dbname": _require("OLTP_POSTGRES_DB"),
    }


def connect(**overrides):
    settings = connect_settings()
    settings.update(overrides)
    return psycopg.connect(**settings)
