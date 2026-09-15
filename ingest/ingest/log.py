"""Structured JSON logging: one JSON object per line on stdout (ADR-006).

Every attempt, batch, quarantine decision and watermark move is a machine-readable
event, so `make ingest-*` output IS the evidence trail (grep-able, timestamped).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone


def log(event: str, level: str = "info", **fields) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "event": event,
    }
    record.update(fields)
    print(json.dumps(record, default=str), flush=True)
