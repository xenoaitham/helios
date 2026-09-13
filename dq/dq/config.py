"""Environment-derived configuration (ADR-011 D9): no credential-looking
literals anywhere — everything arrives via env, exactly like the dbt profile
and the ingest tool. Host/port have in-compose topology defaults (not
secrets); user/password/dbname have NO defaults and fail loudly when absent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """A required environment variable is missing — the tool must not guess."""


@dataclass(frozen=True)
class Config:
    warehouse_user: str
    warehouse_password: str
    warehouse_db: str
    warehouse_host: str
    warehouse_port: int
    quarantine_max_rows: int

    def connection_string(self) -> str:
        """SQLAlchemy URL for GX's Postgres datasource (env-only assembly)."""
        return (
            f"postgresql+psycopg2://{self.warehouse_user}:{self.warehouse_password}"
            f"@{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"
        )

    def dsn(self) -> dict:
        """psycopg2 connect kwargs for the tool's own dead-letter writes."""
        return {
            "host": self.warehouse_host,
            "port": self.warehouse_port,
            "user": self.warehouse_user,
            "password": self.warehouse_password,
            "dbname": self.warehouse_db,
        }


def load_config(env: dict | None = None) -> Config:
    env = os.environ if env is None else env
    missing = [
        name
        for name in ("WAREHOUSE_POSTGRES_USER", "WAREHOUSE_POSTGRES_PASSWORD", "WAREHOUSE_POSTGRES_DB")
        if not env.get(name)
    ]
    if missing:
        raise ConfigError(f"missing required env vars: {', '.join(sorted(missing))} (compose injects them; ADR-011 D9)")
    max_rows_raw = env.get("DQ_QUARANTINE_MAX_ROWS", "10000")
    try:
        max_rows = int(max_rows_raw)
    except ValueError as exc:
        raise ConfigError(f"DQ_QUARANTINE_MAX_ROWS must be an integer, got {max_rows_raw!r}") from exc
    if max_rows < 1:
        raise ConfigError(f"DQ_QUARANTINE_MAX_ROWS must be >= 1, got {max_rows}")
    return Config(
        warehouse_user=env["WAREHOUSE_POSTGRES_USER"],
        warehouse_password=env["WAREHOUSE_POSTGRES_PASSWORD"],
        warehouse_db=env["WAREHOUSE_POSTGRES_DB"],
        warehouse_host=env.get("WAREHOUSE_POSTGRES_HOST", "warehouse-db"),
        warehouse_port=int(env.get("WAREHOUSE_POSTGRES_PORT", "5432")),
        quarantine_max_rows=max_rows,
    )
