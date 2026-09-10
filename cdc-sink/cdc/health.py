"""Thread-safe liveness state + tiny HTTP endpoint for the sink container.

The endpoint reports process liveness and consumer progress, NOT data
freshness: with the mutator stopped the captured tables can legitimately be
quiet for a long time, so freshness would false-alarm. Healthy means: the
consumer loop is polling Kafka successfully (recent successful poll cycle).
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class SinkHealth:
    def __init__(self, stale_after_s: float = 300) -> None:
        self._lock = threading.Lock()
        self._stale_after_s = stale_after_s
        self._started_epoch = time.time()
        self._last_poll_ok_epoch = 0.0
        self._events_landed = 0
        self._stale_rejected = 0
        self._batches = 0
        self._last_error: str | None = None

    def record_poll_ok(self) -> None:
        with self._lock:
            self._last_poll_ok_epoch = time.monotonic()

    def record_batch(self, events: int, applied: int, rejected: int) -> None:
        with self._lock:
            self._batches += 1
            self._events_landed += events
            self._stale_rejected += rejected
            self._last_poll_ok_epoch = time.monotonic()
            self._last_error = None

    def record_error(self, message: str) -> None:
        with self._lock:
            self._last_error = message

    def snapshot(self) -> dict:
        with self._lock:
            fresh = (time.monotonic() - self._last_poll_ok_epoch) < self._stale_after_s
            return {
                "status": "ok" if fresh else "stale",
                "started_epoch": self._started_epoch,
                "events_landed": self._events_landed,
                "stale_rejected": self._stale_rejected,
                "batches": self._batches,
                "last_error": self._last_error,
            }


def serve(health: SinkHealth, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/health":
                self.send_error(404)
                return
            body = json.dumps(health.snapshot()).encode("utf-8")
            code = 200 if health.snapshot()["status"] == "ok" else 503
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:  # keep logs about data, not probes
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True, name="health").start()
    return server
