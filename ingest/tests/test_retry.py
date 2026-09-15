"""Retry policy unit tests (ADR-006): scripted transport + recording sleep."""

from __future__ import annotations

import pytest

from ingest.retry import HttpError, RetryExhausted, RetryPolicy, http_request


class FakeTransport:
    """script: list of (status, headers, body) tuples or Exception instances."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeSleep:
    def __init__(self):
        self.slept = []

    def __call__(self, seconds):
        self.slept.append(seconds)


def tiny_policy(**overrides) -> RetryPolicy:
    defaults = dict(max_attempts=5, backoff_base=0.5, backoff_factor=2.0, backoff_max=8.0, retry_after_default=3.0)
    defaults.update(overrides)
    return RetryPolicy(**defaults)


def test_429_honors_retry_after_verbatim():
    transport = FakeTransport([(429, {"Retry-After": "2"}, b"{}"), (200, {}, b'{"ok": 1}')])
    sleep = FakeSleep()
    doc = http_request("http://rest-mock:8000/products?limit=1", transport=transport, policy=tiny_policy(), sleep=sleep)
    assert doc == {"ok": 1}
    assert sleep.slept == [2.0]  # EXACTLY the header value — the ADR-006 contract


def test_429_without_usable_header_uses_default():
    transport = FakeTransport([(429, {"Retry-After": "soon"}, b"{}"), (200, {}, b'{"ok": 1}')])
    sleep = FakeSleep()
    http_request("http://rest-mock:8000/products", transport=transport, policy=tiny_policy(retry_after_default=3.0), sleep=sleep)
    assert sleep.slept == [3.0]


def test_429_header_lookup_is_case_insensitive():
    # uvicorn/FastAPI sends 'retry-after' lowercase — the header must still be honored
    transport = FakeTransport([(429, {"retry-after": "2"}, b"{}"), (200, {}, b'{"ok": 1}')])
    sleep = FakeSleep()
    http_request("http://rest-mock:8000/products", transport=transport, policy=tiny_policy(), sleep=sleep)
    assert sleep.slept == [2.0]


def test_500_backoff_is_exponential_with_full_jitter():
    transport = FakeTransport([(500, {}, b"boom"), (500, {}, b"boom"), (200, {}, b'{"ok": 1}')])
    sleep = FakeSleep()
    http_request("http://rest-mock:8000/products", transport=transport, policy=tiny_policy(), sleep=sleep)
    assert len(sleep.slept) == 2
    assert 0 <= sleep.slept[0] <= 0.5   # attempt 1: uniform(0, 0.5 * 2^0)
    assert 0 <= sleep.slept[1] <= 1.0   # attempt 2: uniform(0, 0.5 * 2^1)


def test_backoff_respects_cap():
    transport = FakeTransport([(500, {}, b"x")] * 4 + [(200, {}, b"{}")])
    sleep = FakeSleep()
    http_request("http://rest-mock:8000/products", transport=transport, policy=tiny_policy(backoff_max=1.5), sleep=sleep)
    assert len(sleep.slept) == 4
    assert all(0 <= s <= 1.5 for s in sleep.slept)  # attempts 3+ would be 2.0/4.0 uncapped


def test_exhaustion_is_loud_with_history():
    transport = FakeTransport([(500, {}, b"down")] * 5)
    sleep = FakeSleep()
    policy = tiny_policy(max_attempts=5)
    with pytest.raises(RetryExhausted) as excinfo:
        http_request("http://rest-mock:8000/products", transport=transport, policy=policy, sleep=sleep)
    assert len(policy.history) == 5
    assert len(sleep.slept) == 4  # no sleep after the final failed attempt
    assert "5 attempts" in str(excinfo.value)


def test_404_fails_fast_without_retry():
    transport = FakeTransport([(404, {}, b"nope")])
    sleep = FakeSleep()
    with pytest.raises(HttpError):
        http_request("http://rest-mock:8000/promotions/999", transport=transport, policy=tiny_policy(), sleep=sleep)
    assert sleep.slept == []


def test_network_error_backs_off_then_succeeds():
    transport = FakeTransport([OSError("connection reset"), (200, {}, b'{"ok": 1}')])
    sleep = FakeSleep()
    doc = http_request("http://rest-mock:8000/products", transport=transport, policy=tiny_policy(), sleep=sleep)
    assert doc == {"ok": 1}
    assert len(sleep.slept) == 1
