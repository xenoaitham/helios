"""Structured JSON logging: one JSON object per line on stdout — the same
convention as ingest/log.py (ADR-006) and the cdc sink, so `make dq-run`
output IS the evidence trail and `dq.gate_failed` events are grep-able in
`make airflow-logs` (ADR-011 D8: the alert IS the gate, and its paper trail).
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
