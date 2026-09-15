"""Debezium envelope parsing (ADR-005).

The connector runs with JsonConverter and schemas.enable=false, so each record
carries:

    key:   {"user_id": 1}                     (the PK, possibly composite)
    value: {"before": {...}|null, "after": {...}|null,
            "source": {..., "lsn": 123456, "ts_ms": 17890..., ...},
            "op": "r|c|u|d", "ts_ms": ...}

A delete produces one op="d" event followed by a key-only tombstone record
(null value) — tombstones are skipped here: the op="d" event is what lands the
tombstone row in the raw zone. Unknown topics and malformed records never crash
the sink; the caller logs and skips them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from cdc import config

VALID_OPS = frozenset({"r", "c", "u", "d"})


@dataclass(frozen=True)
class Event:
    topic: str
    table: str
    pk: dict
    lsn: int
    op: str
    ts_ms: int
    before: dict | None
    after: dict | None


def parse(topic: str, key_raw: bytes | None, value_raw: bytes | None) -> Event | None:
    """Parse one Kafka record. Returns None for skip-worthy records (tombstones,
    topics outside the capture set); raises ValueError for malformed payloads."""
    if value_raw is None:
        return None  # tombstone after a delete event; nothing to land

    # Topic membership first: anything outside the capture set is skipped no
    # matter what its payload looks like (e.g. internal Connect topics).
    table = topic.rsplit(".", 1)[-1]
    if table not in config.SOURCE_TABLES:
        return None

    try:
        value = json.loads(value_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"undecodable value on {topic}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"value on {topic} is not a JSON object")

    op = value.get("op")
    if op not in VALID_OPS:
        raise ValueError(f"unknown op {op!r} on {topic}")

    source = value.get("source") or {}
    if not isinstance(source, dict):
        raise ValueError(f"source block on {topic} is not an object")
    try:
        lsn = int(source.get("lsn") or 0)  # 0 keeps snapshot rows lowest-priority
        ts_ms = int(source.get("ts_ms") or value.get("ts_ms") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"non-numeric lsn/ts_ms on {topic}") from exc

    if key_raw is None:
        raise ValueError(f"record on {topic} has no key")
    try:
        pk = json.loads(key_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"undecodable key on {topic}: {exc}") from exc
    if not isinstance(pk, dict) or not pk:
        raise ValueError(f"key on {topic} is not a non-empty object")

    before = value.get("before")
    after = value.get("after")
    if before is not None and not isinstance(before, dict):
        raise ValueError(f"before on {topic} is not an object")
    if after is not None and not isinstance(after, dict):
        raise ValueError(f"after on {topic} is not an object")

    return Event(
        topic=topic, table=table, pk=pk, lsn=lsn, op=op, ts_ms=ts_ms,
        before=before, after=after,
    )
