"""Content-hash tests: the idempotency token must be canonical (ADR-007)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from ingest.hashing import canonical_json, content_hash


def test_hash_is_key_order_independent():
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})


def test_hash_changes_with_content():
    assert content_hash({"a": 1}) != content_hash({"a": 2})


def test_exotic_types_are_stable_through_default_str():
    row = {"amount": Decimal("19.99"), "when": dt.datetime(2026, 9, 11, 10, 0, 0)}
    again = {"when": dt.datetime(2026, 9, 11, 10, 0, 0), "amount": Decimal("19.99")}
    assert content_hash(row) == content_hash(again)


def test_canonical_json_is_tight_and_sorted():
    assert canonical_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
