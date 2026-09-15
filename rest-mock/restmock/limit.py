"""Token-bucket rate limiting with an injectable clock (ADR-003).

`try_acquire(now=...)` makes 429 behaviour unit-testable without sleeping:
capacity bounds bursts, refill_per_sec bounds the sustained rate, and the
returned retry_after seconds drive the `Retry-After` header.
"""

from __future__ import annotations

import math


class TokenBucket:
    def __init__(self, capacity: float, refill_per_sec: float):
        if capacity <= 0 or refill_per_sec < 0:
            raise ValueError("capacity must be > 0 and refill_per_sec >= 0")
        self.capacity = float(capacity)
        self.refill_per_sec = float(refill_per_sec)
        self.tokens = float(capacity)
        self._updated: float | None = None

    def try_acquire(self, now: float, cost: float = 1.0) -> tuple[bool, float]:
        """Returns (allowed, retry_after_seconds); retry_after is inf when the
        bucket has no refill configured."""
        if self._updated is None:
            self._updated = now
        else:
            self.tokens = min(self.capacity, self.tokens + (now - self._updated) * self.refill_per_sec)
            self._updated = now
        if self.tokens >= cost:
            self.tokens -= cost
            return True, 0.0
        if self.refill_per_sec <= 0:
            return False, math.inf
        return False, (cost - self.tokens) / self.refill_per_sec


class RateLimiter:
    """One bucket per client key (X-API-Key; anonymous clients share one bucket)."""

    def __init__(self, capacity: float, refill_per_sec: float):
        self._capacity = capacity
        self._refill = refill_per_sec
        self._buckets: dict[str, TokenBucket] = {}

    def acquire(self, key: str, now: float) -> tuple[bool, float, int]:
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = TokenBucket(self._capacity, self._refill)
            self._buckets[key] = bucket
        allowed, retry_after = bucket.try_acquire(now)
        return allowed, retry_after, int(bucket.tokens)
