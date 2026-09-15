"""The raw-zone sink loop (ADR-005): Kafka consumer → lsn-guarded upserts.

Startup sequence: ensure raw DDL → register the Debezium connector (idempotent
PUT) → wait for the four capture topics to exist → consume. Offsets commit only
AFTER a batch is applied in one warehouse transaction (autocommit connection +
explicit transaction block = a real commit per batch, not a savepoint), so a
crash replays at-least-once and the lsn guard makes the effect idempotent.

Logs are structured JSON lines, one per batch plus lifecycle events.
"""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
import traceback

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError

from cdc import config, events, register, upsert
from cdc.ensure import ensure_raw_tables
from cdc.health import SinkHealth, serve
from cdc.upsert import apply_events

import psycopg

_MAX_POLL_RECORDS = 10000
_WAIT_TOPICS_TIMEOUT_S = 900


def _log(event: str, **fields) -> None:
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": event, **fields}), flush=True)


def _ensure_topics(bootstrap: str, wanted: list[str], timeout_s: float) -> None:
    """Create the capture topics if the connector hasn't yet (auto-create is on,
    but being explicit keeps the sink usable before the connector's first read)."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap)
            existing = set(admin.list_topics())
            missing = [t for t in wanted if t not in existing]
            if missing:
                admin.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=1) for t in missing])
            admin.close()
            return
        except KafkaError as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"capture topics not available after {timeout_s}s: {exc}") from exc
            time.sleep(3)


def run() -> int:
    health = SinkHealth()
    server = serve(health, config.health_port())

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    wh_cfg = config.warehouse_settings()
    # autocommit: each batch is wrapped in its own transaction() block, which is
    # a REAL commit on an autocommit connection (Session-3 CRITIC lesson: on a
    # non-autocommit connection it silently degrades to a savepoint).
    conn = psycopg.connect(**wh_cfg, autocommit=True)
    ensure_raw_tables(conn)
    _log("raw_ddl_ensured")

    # Register the connector every start (idempotent PUT) so a bare `make up`
    # brings up the whole capture path. Non-fatal: the sink can serve an
    # already-registered connector even if this instance's REST call hiccups.
    try:
        register.register()
        _log("connector_registered", connector=config.CONNECTOR_NAME)
    except Exception as exc:
        health.record_error(f"register: {exc!r}")
        _log("connector_register_failed", error=repr(exc))

    bootstrap = config.kafka_bootstrap()
    wanted = config.topics()
    _ensure_topics(bootstrap, wanted, _WAIT_TOPICS_TIMEOUT_S)
    _log("topics_ready", topics=wanted)

    consumer = KafkaConsumer(
        *wanted,
        bootstrap_servers=bootstrap,
        group_id=config.group_id(),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=_MAX_POLL_RECORDS,
        fetch_max_bytes=50 * 1024 * 1024,
    )
    _log("sink_started", group=config.group_id(), topics=wanted)

    exit_code = 0
    try:
        while not stop.is_set():
            try:
                batches = consumer.poll(timeout_ms=1000)
            except KafkaError as exc:
                health.record_error(f"poll: {exc!r}")
                _log("poll_error", error=repr(exc))
                time.sleep(2)
                continue

            if not batches:
                health.record_poll_ok()
                continue

            parsed: list[events.Event] = []
            skipped = 0
            last_offsets: dict[str, int] = {}
            for topic_partitions in batches.values():
                for record in topic_partitions:
                    last_offsets[f"{record.topic}@{record.partition}"] = record.offset
                    try:
                        event = events.parse(record.topic, record.key, record.value)
                    except ValueError as exc:
                        skipped += 1
                        _log("malformed_skipped", topic=record.topic, offset=record.offset, error=str(exc))
                        continue
                    if event is None:
                        skipped += 1  # tombstone / non-capture topic
                        continue
                    parsed.append(event)

            if not parsed:
                health.record_poll_ok()
                consumer.commit()
                continue

            started = time.monotonic()
            with conn.transaction():
                applied, coalesced, rejected = apply_events(conn, parsed)
            consumer.commit()
            latency_ms = int((time.monotonic() - started) * 1000)
            health.record_batch(len(parsed), applied, rejected)
            _log(
                "batch_applied",
                events=len(parsed),
                applied=applied,
                coalesced=coalesced,
                stale_rejected=rejected,
                skipped=skipped,
                latency_ms=latency_ms,
                offsets=last_offsets,
            )
    except Exception as exc:
        health.record_error(repr(exc))
        _log("sink_crashed", error=repr(exc), trace=traceback.format_exc(limit=4))
        exit_code = 1
    finally:
        consumer.close()
        conn.close()
        server.shutdown()

    if exit_code == 0:
        _log("sink_stopped_cleanly")
    return exit_code


if __name__ == "__main__":
    sys.exit(run())
