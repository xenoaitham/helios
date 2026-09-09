"""zeep round-trip smoke client for the OrderManagement service.

Exercises the full published contract against a RUNNING service: WSDL fetch,
CreateOrder (+ idempotent replay), GetOrderStatus, UpdateOrderStatus (legal +
illegal transition fault), GetOrders pagination with a status filter.

Credentials come from the environment (SOAP_BASIC_AUTH_USER / PASSWORD); the
target defaults to the in-container endpoint. Run via `make smoke-soap`.
"""
import os
import sys
import time
import uuid
from decimal import Decimal

import requests
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.exceptions import Fault as SoapFault
from zeep.transports import Transport

EXPECTED_OPERATIONS = {"CreateOrder", "GetOrders", "GetOrderStatus", "UpdateOrderStatus"}


def ok(message):
    print(f"[smoke-soap] ok: {message}")


def main():
    base_url = os.environ.get("SOAP_SMOKE_BASE_URL", "http://localhost:8000")
    user = os.environ["SOAP_BASIC_AUTH_USER"]
    password = os.environ["SOAP_BASIC_AUTH_PASSWORD"]

    session = requests.Session()
    session.auth = HTTPBasicAuth(user, password)
    client = Client(f"{base_url}/?wsdl", transport=Transport(session=session))

    operations = set(client.service._operations)
    missing = EXPECTED_OPERATIONS - operations
    if missing:
        print(f"[smoke-soap] FAIL: WSDL missing operations {sorted(missing)}", file=sys.stderr)
        return 1
    ok(f"WSDL fetched and parsed; operations advertised: {sorted(operations)}")

    started = time.perf_counter()
    reference = f"smoke-{uuid.uuid4().hex[:12]}"
    lines = [
        {"product_id": 7, "quantity": 2, "unit_price": Decimal("19.99")},
        {"product_id": 11, "quantity": 1, "unit_price": Decimal("149.50")},
    ]
    # Interop note (ADR-001): spyne emits named array wrappers; zeep callers pass
    # the repeated child explicitly. Java/.NET clients consume the same WSDL natively.
    items = {"OrderItem": lines}

    created = client.service.CreateOrder(customer_id=42, items=items, client_reference=reference)
    assert created.order_id > 0 and created.status == "NEW" and not created.idempotent_replay
    ok(f"CreateOrder -> order_id={created.order_id} status=NEW (no replay)")

    replay = client.service.CreateOrder(customer_id=42, items=items, client_reference=reference)
    assert replay.order_id == created.order_id and replay.idempotent_replay
    ok("CreateOrder with same client_reference replayed the original order (idempotent)")

    status = client.service.GetOrderStatus(order_id=created.order_id)
    assert status.status == "NEW"
    ok("GetOrderStatus -> NEW")

    updated = client.service.UpdateOrderStatus(
        order_id=created.order_id, new_status="PROCESSING", note="smoke test"
    )
    assert updated.status == "PROCESSING"
    ok("UpdateOrderStatus NEW -> PROCESSING")

    fresh = client.service.CreateOrder(customer_id=43, items={"OrderItem": lines})
    try:
        client.service.UpdateOrderStatus(order_id=fresh.order_id, new_status="DELIVERED")
        print("[smoke-soap] FAIL: NEW -> DELIVERED was accepted (state machine broken)",
              file=sys.stderr)
        return 1
    except SoapFault as fault:
        assert "InvalidStateTransition" in fault.message, fault.message
        ok("UpdateOrderStatus NEW -> DELIVERED correctly rejected with InvalidStateTransition")

    page = client.service.GetOrders(page=1, page_size=5, status="PROCESSING")
    rows = page.orders.Order  # spyne wrapper arrays surface under the child name (ADR-001)
    assert len(rows) == 5 and page.total_results >= 5 and page.total_pages >= 1
    ok(f"GetOrders(status=PROCESSING) page 1/5 rows, total_results={page.total_results}")

    print(f"[smoke-soap] PASS — full round trip in {time.perf_counter() - started:.2f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
