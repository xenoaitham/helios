"""Shared fixtures: app factory with injectable flake/rate params.

Tests build their own app instances (no network, no env dependence) so 429/500
behaviour is fully deterministic.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from restmock.app import create_app
from restmock.data import build_catalog


@pytest.fixture(scope="session")
def catalog():
    return build_catalog()


@pytest.fixture()
def client(catalog):
    """Well-behaved app: no flakes, a generous bucket."""
    app = create_app(flake_percent=0, rate_capacity=1000, rate_refill_per_sec=1000, catalog=catalog)
    return TestClient(app)
