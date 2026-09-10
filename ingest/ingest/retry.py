"""Retry/backoff for source HTTP calls (ADR-006 retry policy).

Transport-generic on purpose: extractors own the allowlisted URL build and pass a
`transport(url) -> (status, headers, body)` callable; this module owns the policy
and is fully unit-testable with a scripted transport and a recording sleep.

Policy (ADR-006):
- 429  -> sleep EXACTLY the Retry-After header (verbatim; default if unusable).
- 5xx / network error -> exponential backoff with full jitter:
       uniform(0, min(cap, base * factor^(attempt-1))).
- other 4xx -> no retry, loud HttpError (a 404 is a contract break, not noise).
- bounded attempts, then RetryExhausted carrying the full attempt history.

Every retry is a structured JSON log line (attempt, status, sleep_s), so honoring
Retry-After is provable from run logs, not asserted.
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field

from ingest.log import log

# Jitter source: SystemRandom — recommended by the security gate; cost is
# irrelevant at retry frequency (a handful of draws per run).
_rng = random.SystemRandom()


class HttpError(RuntimeError):
    """Non-retryable HTTP status — the source rejected the request for real."""

    def __init__(self, url: str, status: int, body: bytes):
        self.url = url
        self.status = status
        self.body = body.decode("utf-8", "replace")[:500]
        super().__init__(f"HTTP {status} for {url} (no retry): {self.body}")


class RetryExhausted(RuntimeError):
    """Attempts ran out — loud failure with the full history attached."""

    def __init__(self, url: str, history: list[dict]):
        self.url = url
        self.history = history
        super().__init__(f"retry budget exhausted for {url} after {len(history)} attempts: {history}")


@dataclass
class RetryPolicy:
    max_attempts: int = 8
    backoff_base: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 8.0
    retry_after_default: float = 5.0
    history: list[dict] = field(default_factory=list, repr=False)


def _parse_retry_after(headers) -> float | None:
    """Case-insensitive Retry-After lookup (HTTP headers are case-insensitive;
    uvicorn sends 'retry-after'). Returns non-negative seconds or None."""
    if headers is None:
        return None
    value = None
    for key, header_value in headers.items():
        if str(key).lower() == "retry-after":
            value = header_value
            break
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def _backoff_delay(policy: RetryPolicy, attempt: int) -> float:
    ceiling = min(policy.backoff_max, policy.backoff_base * (policy.backoff_factor ** (attempt - 1)))
    return _rng.uniform(0, ceiling)


def http_request(url: str, *, transport, policy: RetryPolicy, sleep=time.sleep) -> dict:
    """GET `url` through `transport` under `policy` until a 200 JSON body.

    Returns the parsed JSON document. Raises HttpError (non-retryable) or
    RetryExhausted (budget spent). `sleep` is injectable for tests.
    """
    policy.history = []
    for attempt in range(1, policy.max_attempts + 1):
        try:
            status, headers, body = transport(url)
        except OSError as exc:
            delay = _backoff_delay(policy, attempt)
            policy.history.append({"attempt": attempt, "error": repr(exc), "sleep_s": delay})
            log("retry_backoff", source_url=url, attempt=attempt, error=repr(exc), sleep_s=delay)
            if attempt == policy.max_attempts:
                raise RetryExhausted(url, policy.history) from exc
            sleep(delay)
            continue

        if status == 200:
            return json.loads(body) if body else {}

        if status == 429:
            retry_after = _parse_retry_after(headers)
            delay = policy.retry_after_default if retry_after is None else retry_after
            policy.history.append({"attempt": attempt, "status": 429, "retry_after": retry_after, "sleep_s": delay})
            log("retry_rate_limited", source_url=url, attempt=attempt, retry_after=retry_after, sleep_s=delay)
            if attempt == policy.max_attempts:
                raise RetryExhausted(url, policy.history)
            sleep(delay)
            continue

        if 500 <= status < 600:
            delay = _backoff_delay(policy, attempt)
            policy.history.append({"attempt": attempt, "status": status, "sleep_s": delay})
            log("retry_backoff", source_url=url, attempt=attempt, status=status, sleep_s=delay)
            if attempt == policy.max_attempts:
                raise RetryExhausted(url, policy.history)
            sleep(delay)
            continue

        raise HttpError(url, status, body)

    raise RetryExhausted(url, policy.history)  # unreachable; keeps the loop honest
