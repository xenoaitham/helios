"""Config validation tests: base-URL allowlists fail closed."""

from __future__ import annotations

import pytest

from ingest import config

REST_HOSTS = frozenset({"rest-mock", "localhost", "127.0.0.1"})
SOAP_HOSTS = frozenset({"soap-service", "localhost", "127.0.0.1"})


def test_allows_the_compose_service_host():
    assert config.validate_source_base_url("http://rest-mock:8000", REST_HOSTS) == "http://rest-mock:8000"


def test_allows_loopback_for_in_container_fakes():
    assert config.validate_source_base_url("http://127.0.0.1:8123", REST_HOSTS) == "http://127.0.0.1:8123"


def test_rejects_https_scheme():
    with pytest.raises(RuntimeError, match="scheme"):
        config.validate_source_base_url("https://rest-mock:8000", REST_HOSTS)


def test_rejects_off_allowlist_host():
    with pytest.raises(RuntimeError, match="not allowlisted"):
        config.validate_source_base_url("http://metadata.cloud.internal:80", REST_HOSTS)


def test_rejects_cross_service_host():
    # the SOAP host must not be reachable through the REST allowlist
    with pytest.raises(RuntimeError, match="not allowlisted"):
        config.validate_source_base_url("http://soap-service:8000", REST_HOSTS)
    with pytest.raises(RuntimeError, match="not allowlisted"):
        config.validate_source_base_url("http://rest-mock:8000", SOAP_HOSTS)


def test_rejects_embedded_path_and_credentials():
    with pytest.raises(RuntimeError):
        config.validate_source_base_url("http://rest-mock:8000/evil", REST_HOSTS)
