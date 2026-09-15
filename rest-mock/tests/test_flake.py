"""Deterministic-flake tests: the DoD requirement that 500s are reproducible."""

from __future__ import annotations

from fastapi.testclient import TestClient

from restmock.app import create_app
from restmock.flake import Flaker


def test_flaker_percent_100_always_fails_in_sequence():
    flaker = Flaker(percent=100, seed="s")
    assert [flaker.should_fail("/promotions") for _ in range(10)] == [True] * 10


def test_flaker_percent_0_never_fails():
    flaker = Flaker(percent=0, seed="s")
    assert not any(flaker.should_fail("/promotions") for _ in range(50))


def test_flaker_is_reproducible_for_same_seed():
    first = Flaker(percent=50, seed="42")
    second = Flaker(percent=50, seed="42")
    assert [first.should_fail("/promotions") for _ in range(100)] == [second.should_fail("/promotions") for _ in range(100)]


def test_flaker_percent_is_clamped():
    assert Flaker(percent=500, seed="s").percent == 100
    assert Flaker(percent=-7, seed="s").percent == 0


def test_data_route_returns_deterministic_500_with_retryable_body():
    client = TestClient(create_app(flake_percent=100, rate_capacity=1000, rate_refill_per_sec=1000))
    resp = client.get("/promotions")
    assert resp.status_code == 500
    body = resp.json()["detail"]
    assert body["retryable"] is True
    assert body["message"] == "transient upstream failure"
    # the decision is a pure function of (seed, endpoint, counter): a fresh app
    # replays the same failure on the same request
    resp2 = TestClient(create_app(flake_percent=100, rate_capacity=1000, rate_refill_per_sec=1000)).get("/promotions")
    assert resp2.status_code == 500


def test_health_never_flakes_and_detail_404s_when_calm():
    client = TestClient(create_app(flake_percent=100, rate_capacity=1000, rate_refill_per_sec=1000))
    assert client.get("/health").status_code == 200
    calm = TestClient(create_app(flake_percent=0, rate_capacity=1000, rate_refill_per_sec=1000))
    # guards run on every matched route (incl. unknown ids); with flakes off the
    # lookup gets its turn and answers 404
    assert calm.get("/promotions/99999").status_code == 404


def test_default_flake_percent_comes_from_env(monkeypatch):
    monkeypatch.setenv("REST_MOCK_FLAKE_PERCENT", "100")
    client = TestClient(create_app())
    assert client.get("/promotions").status_code == 500


def test_promotion_detail_route_flakes_too():
    client = TestClient(create_app(flake_percent=100, rate_capacity=1000, rate_refill_per_sec=1000))
    assert client.get("/promotions/1").status_code == 500
