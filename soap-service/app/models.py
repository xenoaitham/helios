"""SOAP contract models, faults and the order state machine.

These classes are the single source of truth for the published WSDL (tns
``urn:helios:soap:ordermanagement:v1``). The golden artifact under
``contract/OrderManagement.wsdl`` must stay in sync — enforced by
``tests/test_wsdl_golden.py`` and ``make smoke-soap`` (ADR-001).

All validation that lxml cannot express (positivity, 2-dp money, state
transitions) lives in explicit code paths so it is unit-testable; wire-level
type validation is delegated to ``Soap11(validator="lxml")``.
"""
from spyne import Array, Boolean, ComplexModel, Decimal, DateTime, Integer, Unicode
from spyne.error import Fault

TNS = "urn:helios:soap:ordermanagement:v1"

ORDER_STATUSES = ("NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED")

ALLOWED_TRANSITIONS = {
    "NEW": frozenset({"PROCESSING", "CANCELLED"}),
    "PROCESSING": frozenset({"SHIPPED", "CANCELLED"}),
    "SHIPPED": frozenset({"DELIVERED"}),
    "DELIVERED": frozenset(),
    "CANCELLED": frozenset(),
}

MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 50


class ValidationError(Fault):
    """soap:Client — the request payload failed validation."""

    def __init__(self, message):
        super().__init__(faultcode="Client", faultstring=f"ValidationError: {message}")


class OrderNotFound(Fault):
    """soap:Client — no order with the given id."""

    def __init__(self, order_id):
        super().__init__(
            faultcode="Client", faultstring=f"OrderNotFound: order {order_id} does not exist"
        )


class InvalidStateTransition(Fault):
    """soap:Client — status change violates the order state machine."""

    def __init__(self, current, new):
        super().__init__(
            faultcode="Client",
            faultstring=f"InvalidStateTransition: {current} -> {new} is not allowed",
        )


class ClientReferenceConflict(Fault):
    """soap:Client — client_reference reused with a different payload."""

    def __init__(self, client_reference):
        super().__init__(
            faultcode="Client",
            faultstring=(
                f"ClientReferenceConflict: {client_reference} was already used with a "
                "different payload"
            ),
        )


def validate_transition(current, new):
    """Raise InvalidStateTransition/ValidationError unless current -> new is legal."""
    if new not in ORDER_STATUSES:
        raise ValidationError(f"unknown status {new!r}; expected one of {ORDER_STATUSES}")
    if new == current or new not in ALLOWED_TRANSITIONS[current]:
        raise InvalidStateTransition(current, new)


class OrderItem(ComplexModel):
    __namespace__ = TNS
    product_id = Integer
    quantity = Integer
    unit_price = Decimal


class CreateOrderResult(ComplexModel):
    __namespace__ = TNS
    order_id = Integer
    status = Unicode
    created_at = DateTime
    idempotent_replay = Boolean


class Order(ComplexModel):
    __namespace__ = TNS
    order_id = Integer
    customer_id = Integer
    status = Unicode
    total_amount = Decimal
    currency = Unicode
    created_at = DateTime
    updated_at = DateTime


class OrderPage(ComplexModel):
    __namespace__ = TNS
    page = Integer
    page_size = Integer
    total_results = Integer
    total_pages = Integer
    orders = Array(Order)


class OrderStatus(ComplexModel):
    __namespace__ = TNS
    order_id = Integer
    status = Unicode
    updated_at = DateTime
