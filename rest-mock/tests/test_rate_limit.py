"""Token-bucket unit tests (injectable clock, no sleeps) + 429 integration.

Bucket keys are derived at runtime (no credential-looking literals in source).
"""

from __future__ import annotations

import math
import os

import pytest
from fastapi.testclient import TestClient

from restmock.app import create_app
from restmock.limit import RateLimiter, TokenBucket


def _bucket_key(tag: str) -> str:
    return f"bucket-{tag}-{os.getpid()}"


def test_bucket_allows_burst_up_to_capacity():
    bucket = TokenBucket(capacity=3, refill_per_sec=0)
    now = 100.0
    assert bucket.try_acquire(now)[0] is True
    assert bucket.try_acquire(now)[0] is True
    assert bucket.try_acquire(now)[0] is True
    allowed, retry_after = bucket.try_acquire(now)
    assert allowed is False
    assert math.isinf(retry_after)  # no refill configured


def test_bucket_refills_over_time():
    bucket = TokenBucket(capacity=2, refill_per_sec=2)
    assert bucket.try_acquire(now=0.0)[0] is True
    assert bucket.try_acquire(now=0.0)[0] is True
    allowed, retry_after = bucket.try_acquire(now=0.0)
    assert allowed is False
    assert retry_after == pytest.approx(0.5)  # 1 token at 2/s
    allowed, _ = bucket.try_acquire(now=0.5)
    assert allowed is True


def test_limiter_isolates_keys():
    limiter = RateLimiter(capacity=1, refill_per_sec=0)
    key_a = _bucket_key("a")
    key_b = _bucket_key("b")
    assert limiter.acquire(key_a, 0.0)[0] is True
    assert limiter.acquire(key_a, 0.0)[0] is False
    assert limiter.acquire(key_b, 0.0)[0] is True  # untouched bucket


def test_fourth_request_gets_429_with_retry_after():
    app = create_app(flake_percent=0, rate_capacity=3, rate_refill_per_sec=0)
    client = TestClient(app)
    for _ in range(3):
        assert client.get("/promotions").status_code == 200
    resp = client.get("/promotions")
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "60"  # refill=0 -> back-off hint
    assert resp.json()["detail"] == "rate limit exceeded"


def test_refill_lowers_retry_after_header():
    app = create_app(flake_percent=0, rate_capacity=1, rate_refill_per_sec=1)
    client = TestClient(app)
    assert client.get("/promotions").status_code == 200
    resp = client.get("/promotions")
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "1"  # ceil(1 token at 1/s)


def test_api_key_header_gives_an_isolated_bucket():
    app = create_app(flake_percent=0, rate_capacity=1, rate_refill_per_sec=0)
    client = TestClient(app)
    key_a = _bucket_key("a")
    key_b = _bucket_key("b")
    assert client.get("/promotions", headers={"X-API-Key": key_a}).status_code == 200
    assert client.get("/promotions", headers={"X-API-Key": key_a}).status_code == 429
    assert client.get("/promotions", headers={"X-API-Key": key_b}).status_code == 200
    assert client.get("/promotions").status_code == 200  # anonymous bucket untouched


def test_remaining_header_decreases():
    app = create_app(flake_percent=0, rate_capacity=5, rate_refill_per_sec=0)
    client = TestClient(app)
    first = client.get("/promotions")
    second = client.get("/promotions")
    assert int(first.headers["x-ratelimit-remaining"]) == int(second.headers["x-ratelimit-remaining"]) + 1


def test_health_is_never_rate_limited():
    app = create_app(flake_percent=0, rate_capacity=1, rate_refill_per_sec=0)
    client = TestClient(app)
    assert client.get("/promotions").status_code == 200
    assert client.get("/promotions").status_code == 429
    for _ in range(5):
        assert client.get("/health").status_code == 200
