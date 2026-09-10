"""Idempotent Debezium connector registration via the Kafka Connect REST API.

Uses PUT /connectors/<name>/config, which creates the connector on first call
and replaces its config on later calls — safe on every sink start. The REST
target is restricted to an explicit host allowlist (same posture as
soap-service/check_contract.py): scheme, host, port and path are validated
here, in this module, before any request object is built. Credentials come
from the environment only and exist only in the request body sent to the
in-network Connect endpoint.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

from cdc import config

# Only the Kafka Connect control plane of this compose network, by name.
ALLOWED_HOSTS = frozenset({"cdc-connect", "localhost", "127.0.0.1"})


def rest_url(path: str) -> str:
    """Build a validated Connect REST URL for one of this module's fixed paths."""
    base = config.connect_url()
    url = f"{base}{path}"
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "http":
        raise RuntimeError(f"blocked scheme {parsed.scheme!r}; only http allowed")
    if parsed.hostname not in ALLOWED_HOSTS or parsed.username or parsed.password:
        raise RuntimeError(
            f"blocked host {parsed.hostname!r}; allowed hosts: {sorted(ALLOWED_HOSTS)}"
        )
    if parsed.path not in (
        "/connectors",
        f"/connectors/{config.CONNECTOR_NAME}/config",
        f"/connectors/{config.CONNECTOR_NAME}/status",
    ):
        raise RuntimeError(f"blocked path {parsed.path!r}; only fixed Connect endpoints allowed")
    return url


def _request_json(url: str, method: str, body: dict | None = None, timeout: float = 30) -> dict:
    """Perform one allowlisted REST call and decode the JSON response."""
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    return json.loads(payload) if payload else {}


def connector_config() -> dict:
    """The Debezium Postgres connector configuration (ADR-005 §6)."""
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": config.oltp_db_host(),
        "database.port": "5432",
        "database.user": config.cdc_db_user(),
        "database.password": config.cdc_db_password(),
        "database.dbname": config.oltp_settings()["dbname"],
        "plugin.name": "pgoutput",
        "slot.name": "helios_cdc_slot",
        "publication.name": "helios_publication",
        # The publication is pre-created by the table OWNER in scripts/cdc-setup.sh
        # (CREATE PUBLICATION ... FOR TABLE requires owning the tables — the
        # replication role must not, and pgoutput refuses a missing publication).
        "publication.autocreate.mode": "disabled",
        "table.include.list": "public.users,public.orders,public.order_items,public.payments",
        "topic.prefix": config.topic_prefix(),
        "snapshot.mode": "initial",
        "snapshot.fetch.size": "10000",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "true",
        "heartbeat.interval.ms": "10000",
        "max.batch.size": "2048",
        "max.queue.size": "8192",
        "schema.history.internal.kafka.bootstrap.servers": config.kafka_bootstrap(),
        "schema.history.internal.kafka.topic": "schemahistory.helios-oltp",
    }


def connector_status() -> dict:
    return _request_json(rest_url(f"/connectors/{config.CONNECTOR_NAME}/status"), "GET")


def register(wait_timeout_s: float = 600, poll_s: float = 5) -> dict:
    """PUT the config (create-or-replace), then wait for connector + task RUNNING.

    Returns the final status document. Raises RuntimeError on timeout.
    """
    put_url = rest_url(f"/connectors/{config.CONNECTOR_NAME}/config")
    _request_json(put_url, "PUT", connector_config())

    deadline = time.monotonic() + wait_timeout_s
    last = {}
    while time.monotonic() < deadline:
        try:
            last = connector_status()
            states = [last.get("connector", {}).get("state", "")]
            states += [t.get("state", "") for t in last.get("tasks", [])]
            if states and all(s == "RUNNING" for s in states):
                return last
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            pass  # Connect REST can hiccup while the worker settles; keep polling
        time.sleep(poll_s)
    raise RuntimeError(f"connector {config.CONNECTOR_NAME} not RUNNING after {wait_timeout_s}s; last status: {last}")
