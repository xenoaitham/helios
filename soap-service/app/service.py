"""spyne service implementing the OrderManagement contract (ADR-001).

Every SQL statement is written inline at its call site as one static,
fully parameterized string; optional filters use ``(? IS NULL OR col = ?)``
guards so no query text is ever assembled at runtime.
"""
import datetime as dt
import hashlib
import json
from decimal import Decimal, InvalidOperation

from spyne import Array, DateTime, Integer, ServiceBase, Unicode, rpc

from .db import connect, tx
from .models import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    ORDER_STATUSES,
    ClientReferenceConflict,
    CreateOrderResult,
    InvalidStateTransition,
    Order,
    OrderItem,
    OrderNotFound,
    OrderPage,
    OrderStatus,
    ValidationError,
    validate_transition,
)

CURRENCY = "USD"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def _fmt(value):
    return value.strftime(TS_FORMAT)


def _parse(value):
    return dt.datetime.strptime(value, TS_FORMAT)


def _as_utc_naive_str(value):
    """Normalize a SOAP dateTime to the storage format (aware -> naive UTC)."""
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return _fmt(value)


def _price_to_cents(unit_price):
    """Validate a Decimal price and convert to integer cents (max 2 dp)."""
    if unit_price is None:
        raise ValidationError("unit_price is required")
    try:
        scaled = Decimal(unit_price) * 100
    except (InvalidOperation, TypeError) as exc:
        raise ValidationError(f"unit_price is not a valid decimal: {unit_price!r}") from exc
    if scaled != scaled.to_integral_value():
        raise ValidationError(f"unit_price {unit_price} has more than 2 decimal places")
    cents = int(scaled)
    if cents < 0:
        raise ValidationError("unit_price must be >= 0")
    return cents


class OrderManagementService(ServiceBase):
    @rpc(
        Integer,
        Array(OrderItem),
        Unicode(min_occurs=0),
        _returns=CreateOrderResult,
    )
    def CreateOrder(ctx, customer_id, items, client_reference=None):
        if customer_id is None or customer_id <= 0:
            raise ValidationError("customer_id must be a positive integer")
        if not items:
            raise ValidationError("items must contain at least one line")

        parsed_items = []
        for item in items:
            if item.product_id is None or item.product_id <= 0:
                raise ValidationError("product_id must be a positive integer")
            if item.quantity is None or item.quantity <= 0:
                raise ValidationError("quantity must be a positive integer")
            parsed_items.append((item.product_id, item.quantity, _price_to_cents(item.unit_price)))

        if client_reference is not None and not client_reference.strip():
            client_reference = None

        payload_hash = None
        if client_reference is not None:
            canonical = json.dumps(
                {"customer_id": customer_id, "items": parsed_items},
                sort_keys=True,
                separators=(",", ":"),
            )
            payload_hash = hashlib.sha256(canonical.encode()).hexdigest()

        created = _now()
        created_str = _fmt(created)

        conn = connect()
        try:
            with tx(conn):
                if client_reference is not None:
                    row = conn.execute(
                        "SELECT order_id, status, client_ref_hash, created_at FROM orders WHERE client_reference = ?",
                        (client_reference,),
                    ).fetchone()
                    if row is not None:
                        if row["client_ref_hash"] != payload_hash:
                            raise ClientReferenceConflict(client_reference)
                        # Idempotent replay: retries must not double the side effect.
                        return CreateOrderResult(
                            order_id=row["order_id"],
                            status=row["status"],
                            created_at=_parse(row["created_at"]),
                            idempotent_replay=True,
                        )

                cursor = conn.execute(
                    "INSERT INTO orders (customer_id, status, total_cents, currency, client_reference, client_ref_hash, created_at, updated_at) VALUES (?, 'NEW', ?, ?, ?, ?, ?, ?)",
                    (
                        customer_id,
                        sum(qty * cents for _, qty, cents in parsed_items),
                        CURRENCY,
                        client_reference,
                        payload_hash,
                        created_str,
                        created_str,
                    ),
                )
                order_id = cursor.lastrowid
                conn.executemany(
                    "INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price_cents) VALUES (?, ?, ?, ?, ?)",
                    [
                        (order_id, line_no, product_id, qty, cents)
                        for line_no, (product_id, qty, cents) in enumerate(parsed_items, start=1)
                    ],
                )
                conn.execute(
                    "INSERT INTO order_status_history (order_id, from_status, to_status, note, changed_at) VALUES (?, ?, ?, ?, ?)",
                    (order_id, None, "NEW", None, created_str),
                )
        finally:
            conn.close()

        return CreateOrderResult(
            order_id=order_id, status="NEW", created_at=created, idempotent_replay=False
        )

    @rpc(
        Integer(min_occurs=0),
        Integer(min_occurs=0),
        Unicode(min_occurs=0),
        DateTime(min_occurs=0),
        DateTime(min_occurs=0),
        _returns=OrderPage,
    )
    def GetOrders(ctx, page=None, page_size=None, status=None, date_from=None, date_to=None):
        page = 1 if page is None else page
        page_size = DEFAULT_PAGE_SIZE if page_size is None else page_size
        if page < 1:
            raise ValidationError("page must be >= 1")
        if page_size < 1 or page_size > MAX_PAGE_SIZE:
            raise ValidationError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")

        if status is not None and status not in ORDER_STATUSES:
            raise ValidationError(f"unknown status {status!r}; expected one of {ORDER_STATUSES}")
        date_from_str = _as_utc_naive_str(date_from)
        date_to_str = _as_utc_naive_str(date_to)

        filter_params = (status, status, date_from_str, date_from_str, date_to_str, date_to_str)

        conn = connect()
        try:
            total_results = conn.execute(
                "SELECT count(*) FROM orders WHERE (? IS NULL OR status = ?) AND (? IS NULL OR created_at >= ?) AND (? IS NULL OR created_at <= ?)",
                filter_params,
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT order_id, customer_id, status, total_cents, currency, created_at, updated_at FROM orders WHERE (? IS NULL OR status = ?) AND (? IS NULL OR created_at >= ?) AND (? IS NULL OR created_at <= ?) ORDER BY order_id LIMIT ? OFFSET ?",
                [*filter_params, page_size, (page - 1) * page_size],
            ).fetchall()
        finally:
            conn.close()

        orders = [
            Order(
                order_id=row["order_id"],
                customer_id=row["customer_id"],
                status=row["status"],
                total_amount=Decimal(row["total_cents"]).scaleb(-2),
                currency=row["currency"],
                created_at=_parse(row["created_at"]),
                updated_at=_parse(row["updated_at"]),
            )
            for row in rows
        ]
        return OrderPage(
            page=page,
            page_size=page_size,
            total_results=total_results,
            total_pages=(total_results + page_size - 1) // page_size,
            orders=orders,
        )

    @rpc(Integer, _returns=OrderStatus)
    def GetOrderStatus(ctx, order_id):
        conn = connect()
        try:
            row = conn.execute(
                "SELECT status, updated_at FROM orders WHERE order_id = ?", (order_id,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise OrderNotFound(order_id)
        return OrderStatus(
            order_id=order_id, status=row["status"], updated_at=_parse(row["updated_at"])
        )

    @rpc(Integer, Unicode, Unicode(min_occurs=0), _returns=OrderStatus)
    def UpdateOrderStatus(ctx, order_id, new_status, note=None):
        changed = _now()
        changed_str = _fmt(changed)

        conn = connect()
        try:
            with tx(conn):
                row = conn.execute(
                    "SELECT status FROM orders WHERE order_id = ?", (order_id,)
                ).fetchone()
                if row is None:
                    raise OrderNotFound(order_id)
                validate_transition(row["status"], new_status)
                conn.execute(
                    "UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?",
                    (new_status, changed_str, order_id),
                )
                conn.execute(
                    "INSERT INTO order_status_history (order_id, from_status, to_status, note, changed_at) VALUES (?, ?, ?, ?, ?)",
                    (order_id, row["status"], new_status, note, changed_str),
                )
        finally:
            conn.close()

        return OrderStatus(order_id=order_id, status=new_status, updated_at=changed)
