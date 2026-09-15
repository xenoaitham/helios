"""Config loading (ADR-011 D9): env-only credentials, loud failures."""

import pytest

from dq.config import Config, ConfigError, load_config

BASE_ENV = {
    "WAREHOUSE_POSTGRES_USER": "u1",
    "WAREHOUSE_POSTGRES_PASSWORD": "p1",
    "WAREHOUSE_POSTGRES_DB": "db1",
}


def test_missing_required_env_is_loud():
    with pytest.raises(ConfigError) as exc:
        load_config({})
    assert "WAREHOUSE_POSTGRES_USER" in str(exc.value)
    assert "WAREHOUSE_POSTGRES_PASSWORD" in str(exc.value)
    assert "WAREHOUSE_POSTGRES_DB" in str(exc.value)


def test_empty_required_env_counts_as_missing():
    env = dict(BASE_ENV, WAREHOUSE_POSTGRES_USER="")
    with pytest.raises(ConfigError):
        load_config(env)


def test_topology_defaults_and_overrides():
    cfg = load_config(dict(BASE_ENV))
    assert (cfg.warehouse_host, cfg.warehouse_port) == ("warehouse-db", 5432)
    assert cfg.quarantine_max_rows == 10000
    cfg2 = load_config(dict(BASE_ENV, WAREHOUSE_POSTGRES_HOST="wh", WAREHOUSE_POSTGRES_PORT="5433", DQ_QUARANTINE_MAX_ROWS="7"))
    assert (cfg2.warehouse_host, cfg2.warehouse_port, cfg2.quarantine_max_rows) == ("wh", 5433, 7)


def test_bad_max_rows_is_loud():
    with pytest.raises(ConfigError):
        load_config(dict(BASE_ENV, DQ_QUARANTINE_MAX_ROWS="zero"))
    with pytest.raises(ConfigError):
        load_config(dict(BASE_ENV, DQ_QUARANTINE_MAX_ROWS="0"))


def test_connection_string_and_dsn_come_from_env_only():
    cfg = load_config(dict(BASE_ENV, WAREHOUSE_POSTGRES_HOST="h1", WAREHOUSE_POSTGRES_PORT="5432"))
    cs = cfg.connection_string()
    assert cs.startswith("postgresql+psycopg2://")
    assert "u1" in cs and "h1:5432" in cs and "db1" in cs
    dsn = cfg.dsn()
    assert dsn == {"host": "h1", "port": 5432, "user": "u1", "password": "p1", "dbname": "db1"}


def test_config_is_frozen():
    cfg = load_config(dict(BASE_ENV))
    with pytest.raises(Exception):
        cfg.warehouse_user = "other"  # type: ignore[misc]
    assert isinstance(cfg, Config)
