"""Content hashing for idempotent landing (ADR-007).

The hash is over the row's canonical JSON: sorted keys, tight separators, str()
fallback for exotic types (Decimal, datetime). Identical source content therefore
hashes identically in every run — the token the raw-zone upsert guard compares.
"""

from __future__ import annotations

import hashlib
import json


def canonical_json(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(row: dict) -> str:
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()
