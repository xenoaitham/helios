"""Shared fixtures: isolated temp store, live WSGI server, authenticated zeep client.

Credentials are generated per run and injected via the environment BEFORE the
app module is imported — no credential literals in the repo (ADR-001).

The in-process server binds a FIXED loopback port so test files can request
fully literal URLs (no URL is ever assembled from parameters — SSRF-hard by
construction, per the security gate).
"""
import os
import secrets
import tempfile
import threading
from wsgiref.simple_server import make_server, WSGIRequestHandler

import pytest

TEST_BASE_URL = "http://127.0.0.1:18081"
TEST_PORT = 18081

_tmpdir = tempfile.mkdtemp(prefix="helios-soap-test-")
os.environ["SOAP_DB_PATH"] = os.path.join(_tmpdir, "orders.db")
os.environ["SOAP_BASIC_AUTH_USER"] = "test-" + secrets.token_hex(6)
os.environ["SOAP_BASIC_AUTH_PASSWORD"] = secrets.token_hex(24)

from app.db import connect, ensure_schema  # noqa: E402
from app.seed import generate  # noqa: E402
from app.wsgi import application  # noqa: E402


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):  # keep pytest output readable
        pass


@pytest.fixture(scope="session", autouse=True)
def seeded_store():
    conn = connect()
    ensure_schema(conn)
    # Small deterministic window: enough rows to exercise filters/pagination fast.
    generate(conn, days=30, rng_seed=7, first_day_orders=40, last_day_orders=60)
    conn.close()
    yield


@pytest.fixture(scope="session", autouse=True)
def base_url():
    """Start the in-process server for the whole session (tests use literal URLs)."""
    try:
        server = make_server("127.0.0.1", TEST_PORT, application, handler_class=_QuietHandler)
    except OSError as error:
        raise pytest.UsageError(
            f"test server could not bind 127.0.0.1:{TEST_PORT} (port busy?): {error}"
        ) from error
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield TEST_BASE_URL
    server.shutdown()
    server.server_close()


@pytest.fixture(scope="session")
def soap_client(base_url):
    import requests
    from zeep import Client
    from zeep.transports import Transport

    session = requests.Session()
    session.auth = (os.environ["SOAP_BASIC_AUTH_USER"], os.environ["SOAP_BASIC_AUTH_PASSWORD"])
    client = Client(f"{base_url}/?wsdl", transport=Transport(session=session))
    return client.service


@pytest.fixture(scope="session")
def good_auth():
    return (os.environ["SOAP_BASIC_AUTH_USER"], os.environ["SOAP_BASIC_AUTH_PASSWORD"])
