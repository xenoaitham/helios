"""Debezium envelope parsing (cdc.events) against realistic payloads."""

from __future__ import annotations

import json

import pytest

from cdc.events import parse

TOPIC = "helios.public.users"


def _envelope(op: str, lsn: int, ts_ms: int, after=None, before=None) -> bytes:
    value = {
        "before": before,
        "after": after,
        "source": {"db": "oltp", "lsn": lsn, "ts_ms": ts_ms, "table": "users"},
        "op": op,
        "ts_ms": ts_ms + 1,
    }
    return json.dumps(value).encode("utf-8")


def test_parse_snapshot_read():
    after = {"user_id": 1, "email": "a@x.io", "total_amount": "10.00"}
    event = parse(TOPIC, json.dumps({"user_id": 1}).encode(), _envelope("r", 500, 1000, after=after))
    assert event is not None
    assert event.table == "users"
    assert event.pk == {"user_id": 1}
    assert event.op == "r"
    assert event.lsn == 500
    assert event.ts_ms == 1000
    assert event.after == after


def test_parse_update_keeps_before_and_after():
    event = parse(TOPIC, json.dumps({"user_id": 2}).encode(), _envelope("u", 900, 2000, after={"user_id": 2, "email": "b@x.io"}, before={"user_id": 2, "email": "a@x.io"}))
    assert event.op == "u"
    assert event.before == {"user_id": 2, "email": "a@x.io"}
    assert event.after == {"user_id": 2, "email": "b@x.io"}


def test_parse_delete_has_null_after():
    event = parse(TOPIC, json.dumps({"user_id": 3}).encode(), _envelope("d", 950, 2500, before={"user_id": 3, "email": "c@x.io"}))
    assert event.op == "d"
    assert event.after is None
    assert event.before is not None


def test_parse_tombstone_returns_none():
    # Debezium emits a key-only record (null value) after every delete event.
    assert parse(TOPIC, json.dumps({"user_id": 3}).encode(), None) is None


def test_parse_unknown_table_returns_none():
    assert parse("helios.public.audit_log", b'{"id": 1}', _envelope("c", 1, 1, after={"id": 1})) is None
    assert parse("schemahistory.helios-oltp", None, b'{"source": {}}') is None


def test_parse_malformed_value_raises():
    with pytest.raises(ValueError):
        parse(TOPIC, b'{"user_id": 1}', b"not-json{{{")


def test_parse_unknown_op_raises():
    with pytest.raises(ValueError):
        parse(TOPIC, b'{"user_id": 1}', _envelope("x", 1, 1))


def test_parse_missing_key_raises():
    with pytest.raises(ValueError):
        parse(TOPIC, None, _envelope("c", 1, 1, after={"user_id": 1}))


def test_parse_missing_lsn_defaults_to_zero():
    value = {"before": None, "after": {"user_id": 9}, "source": {"ts_ms": 5}, "op": "c", "ts_ms": 6}
    event = parse(TOPIC, b'{"user_id": 9}', json.dumps(value).encode())
    assert event.lsn == 0


def test_parse_composite_key_passthrough():
    event = parse("helios.public.order_items", json.dumps({"order_id": 7, "line": 1}).encode(), _envelope("c", 10, 10, after={"order_id": 7, "line": 1}))
    assert event.pk == {"order_id": 7, "line": 1}
