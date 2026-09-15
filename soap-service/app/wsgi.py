"""WSGI stack: /health (open) -> HTTP basic auth -> spyne SOAP 1.1 app.

Credentials come strictly from the environment (``SOAP_BASIC_AUTH_USER`` /
``SOAP_BASIC_AUTH_PASSWORD``); the process refuses to boot without them so a
misconfigured stack fails loudly instead of silently serving an open SOAP
endpoint. No credential literals live in source or tests (ADR-001).
"""
import base64
import hmac
import json
import os

from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from .db import connect, ensure_schema
from .models import TNS
from .service import OrderManagementService


def _required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"environment variable {name} is required (set it in .env / compose; "
            "no in-code fallback by design)"
        )
    return value


_soap_app = Application(
    services=[OrderManagementService],
    tns=TNS,
    name="OrderManagement",
    in_protocol=Soap11(validator="lxml"),
    out_protocol=Soap11(),
)
spyne_wsgi = WsgiApplication(_soap_app)


class _BasicAuthMiddleware:
    def __init__(self, app, username, password, realm="OrderManagement"):
        self._app = app
        self._username = username.encode()
        self._password = password.encode()
        self._realm = realm

    def _authorized(self, environ):
        header = environ.get("HTTP_AUTHORIZATION", "")
        if not header.lower().startswith("basic "):
            return False
        try:
            decoded = base64.b64decode(header[6:].strip()).decode()
        except (ValueError, UnicodeDecodeError):
            return False
        user, _, password = decoded.partition(":")
        return hmac.compare_digest(user.encode(), self._username) and hmac.compare_digest(
            password.encode(), self._password
        )

    def __call__(self, environ, start_response):
        if self._authorized(environ):
            return self._app(environ, start_response)
        body = json.dumps({"error": "unauthorized"}).encode()
        start_response(
            "401 Unauthorized",
            [
                ("Content-Type", "application/json"),
                ("WWW-Authenticate", f'Basic realm="{self._realm}", charset="UTF-8"'),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]


def _health(environ, start_response):
    body = json.dumps({"status": "ok", "service": "OrderManagement"}).encode()
    start_response(
        "200 OK",
        [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
    )
    return [body]


_basic_auth = _BasicAuthMiddleware(
    spyne_wsgi, _required_env("SOAP_BASIC_AUTH_USER"), _required_env("SOAP_BASIC_AUTH_PASSWORD")
)


def application(environ, start_response):
    if environ.get("PATH_INFO") == "/health":
        return _health(environ, start_response)
    return _basic_auth(environ, start_response)


# Fail fast at boot if the store is not writable / schema cannot be ensured.
with connect() as _conn:
    ensure_schema(_conn)
