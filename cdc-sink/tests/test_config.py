"""Allowlist validation for the Kafka Connect REST endpoint (Mimosa HTTP rule)."""

from __future__ import annotations

import pytest

from cdc import config


def test_default_connect_url_is_allowed():
    assert config.validate_connect_url("http://cdc-connect:8083") == "http://cdc-connect:8083"


def test_localhost_allowed():
    assert config.validate_connect_url("http://localhost:8083") == "http://localhost:8083"


def test_https_rejected():
    with pytest.raises(RuntimeError, match="scheme"):
        config.validate_connect_url("https://cdc-connect:8083")


def test_unknown_host_rejected():
    with pytest.raises(RuntimeError, match="allowlist"):
        config.validate_connect_url("http://169.254.169.254:8083")


def test_path_rejected():
    with pytest.raises(RuntimeError, match="path"):
        config.validate_connect_url("http://cdc-connect:8083/admin")


def test_topics_derive_from_prefix():
    assert config.topics() == [
        "helios.public.users",
        "helios.public.orders",
        "helios.public.order_items",
        "helios.public.payments",
    ]


def test_rest_url_allows_only_fixed_connect_endpoints():
    from cdc import register

    assert register.rest_url("/connectors") == "http://cdc-connect:8083/connectors"
    assert register.rest_url("/connectors/helios-oltp/config") == "http://cdc-connect:8083/connectors/helios-oltp/config"
    assert register.rest_url("/connectors/helios-oltp/status") == "http://cdc-connect:8083/connectors/helios-oltp/status"
    with pytest.raises(RuntimeError, match="path"):
        register.rest_url("/connectors/other-connector/config")
    with pytest.raises(RuntimeError, match="path"):
        register.rest_url("/../../admin")


def test_missing_credentials_fail_fast(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_POSTGRES_USER", raising=False)
    with pytest.raises(RuntimeError, match="WAREHOUSE_POSTGRES_USER"):
        config.warehouse_settings()
