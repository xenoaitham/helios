"""Deterministic flaky-500 simulation (ADR-003).

Failure decision is a pure function of (seed, endpoint, request counter):
sha256 digests make it stable across processes (unlike Python's salted hash),
so a fresh app with percent=100 fails the first data request, percent=0 never
fails, and the default is a stationary 5% — reproducible under test.
"""

from __future__ import annotations

import hashlib
import itertools


class Flaker:
    def __init__(self, percent: int, seed: str):
        self.percent = max(0, min(100, int(percent)))
        self.seed = str(seed)
        self._counter = itertools.count()

    def should_fail(self, endpoint: str) -> bool:
        n = next(self._counter)
        if self.percent <= 0:
            return False
        digest = hashlib.sha256(f"{self.seed}:{endpoint}:{n}".encode()).digest()
        return int.from_bytes(digest[:8], "big") % 100 < self.percent
