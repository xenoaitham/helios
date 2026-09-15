"""SOAP-over-HTTP integration tests: full contract round trip via zeep.

Interop note (recorded in ADR-001): spyne emits named array wrapper types
(``OrderItemArray`` / ``OrderArray`` with a repeated child). zeep has no
auto-flattening for doc/lit wrappers, so the canonical client-side shapes are:
request ``items={"OrderItem": [ {...}, ... ]}``, response ``page.orders.Order``.
Java/.NET clients consume the same WSDL natively.
"""
import datetime as dt
from decimal import Decimal

import pytest
from zeep.exceptions import Fault as SoapFault

LINES_A = [
    {"product_id": 101, "quantity": 2, "unit_price": Decimal("19.99")},
    {"product_id": 202, "quantity": 1, "unit_price": Decimal("149.50")},
]
LINES_B = [{"product_id": 101, "quantity": 9, "unit_price": Decimal("19.99")}]


def items(lines):
    """Wrap line dicts in the array-wrapper shape the WSDL declares."""
    return {"OrderItem": lines}


def test_create_returns_new_order(soap_client):
    result = soap_client.CreateOrder(customer_id=1, items=items(LINES_A))
    assert result.order_id > 0
    assert result.status == "NEW"
    assert result.idempotent_replay is False
    assert isinstance(result.created_at, dt.datetime)


def test_create_replay_with_same_reference_is_idempotent(soap_client):
    reference = "pytest-replay-001"
    first = soap_client.CreateOrder(customer_id=1, items=items(LINES_A),
                                    client_reference=reference)
    replay = soap_client.CreateOrder(customer_id=1, items=items(LINES_A),
                                     client_reference=reference)
    assert replay.order_id == first.order_id
    assert replay.idempotent_replay is True and first.idempotent_replay is False


def test_replay_with_different_payload_conflicts(soap_client):
    reference = "pytest-replay-002"
    soap_client.CreateOrder(customer_id=1, items=items(LINES_A), client_reference=reference)
    with pytest.raises(SoapFault) as fault:
        soap_client.CreateOrder(customer_id=1, items=items(LINES_B),
                                client_reference=reference)
    assert "ClientReferenceConflict" in fault.value.message


def test_get_order_status_unknown_id_faults(soap_client):
    with pytest.raises(SoapFault) as fault:
        soap_client.GetOrderStatus(order_id=999_999_999)
    assert "OrderNotFound" in fault.value.message


def test_update_status_walks_the_state_machine(soap_client):
    order = soap_client.CreateOrder(customer_id=2, items=items(LINES_A))
    for expected in ("PROCESSING", "SHIPPED", "DELIVERED"):
        result = soap_client.UpdateOrderStatus(order_id=order.order_id, new_status=expected,
                                               note="pytest walk")
        assert result.status == expected


def test_illegal_transition_is_rejected(soap_client):
    order = soap_client.CreateOrder(customer_id=2, items=items(LINES_A))
    with pytest.raises(SoapFault) as fault:
        soap_client.UpdateOrderStatus(order_id=order.order_id, new_status="DELIVERED")
    assert "InvalidStateTransition" in fault.value.message


def test_unknown_target_status_is_a_validation_error(soap_client):
    order = soap_client.CreateOrder(customer_id=2, items=items(LINES_A))
    with pytest.raises(SoapFault) as fault:
        soap_client.UpdateOrderStatus(order_id=order.order_id, new_status="LOST")
    assert "ValidationError" in fault.value.message


def test_get_orders_paginates_stably(soap_client):
    page_one = soap_client.GetOrders(page=1, page_size=5, status="PROCESSING")
    page_two = soap_client.GetOrders(page=2, page_size=5, status="PROCESSING")
    rows_one, rows_two = page_one.orders.Order, page_two.orders.Order
    assert len(rows_one) == 5
    assert page_one.total_results > 5
    assert page_one.total_pages >= 2
    ids_one = [o.order_id for o in rows_one]
    ids_two = [o.order_id for o in rows_two]
    assert ids_one == sorted(ids_one) and ids_two == sorted(ids_two)
    assert not set(ids_one) & set(ids_two)  # stable ORDER BY + OFFSET, no overlap


def test_get_orders_total_amount_is_a_two_dp_decimal(soap_client):
    page = soap_client.GetOrders(page=1, page_size=3)
    assert page.orders.Order
    for order in page.orders.Order:
        assert isinstance(order.total_amount, Decimal)
        assert -order.total_amount.as_tuple().exponent == 2


def test_get_orders_date_window(soap_client):
    total_all = soap_client.GetOrders(page=1, page_size=1).total_results
    future = soap_client.GetOrders(page=1, page_size=1, date_from=dt.datetime(2100, 1, 1))
    assert future.total_results == 0
    past = soap_client.GetOrders(page=1, page_size=1,
                                 date_from=dt.datetime(2000, 1, 1),
                                 date_to=dt.datetime(2100, 1, 1))
    assert past.total_results == total_all


def test_get_orders_rejects_bad_input(soap_client):
    for kwargs in ({"page": 0}, {"page_size": 0}, {"page_size": 501}, {"status": "LOST"}):
        with pytest.raises(SoapFault) as fault:
            soap_client.GetOrders(**kwargs)
        assert "ValidationError" in fault.value.message


def test_create_rejects_empty_item_list(soap_client):
    with pytest.raises(SoapFault) as fault:
        soap_client.CreateOrder(customer_id=1, items=items([]))
    assert "ValidationError" in fault.value.message


def test_create_rejects_more_than_two_decimal_places(soap_client):
    with pytest.raises(SoapFault) as fault:
        soap_client.CreateOrder(
            customer_id=1,
            items=items([{"product_id": 1, "quantity": 1, "unit_price": Decimal("0.001")}]),
        )
    assert "ValidationError" in fault.value.message


def test_create_rejects_non_positive_fields(soap_client):
    with pytest.raises(SoapFault):
        soap_client.CreateOrder(customer_id=0, items=items(LINES_A))
    with pytest.raises(SoapFault):
        soap_client.CreateOrder(
            customer_id=1,
            items=items([{"product_id": 1, "quantity": -1, "unit_price": Decimal("1.00")}]),
        )


def test_blank_client_reference_is_ignored_not_rejected(soap_client):
    """Documented behaviour: a whitespace-only idempotency key means 'none'."""
    result = soap_client.CreateOrder(customer_id=1, items=items(LINES_A), client_reference="   ")
    assert result.order_id > 0 and result.idempotent_replay is False
