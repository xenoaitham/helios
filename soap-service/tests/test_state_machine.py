"""Pure state-machine tests (no SOAP, no store)."""
import pytest

from app.models import (
    ALLOWED_TRANSITIONS,
    ORDER_STATUSES,
    InvalidStateTransition,
    ValidationError,
    validate_transition,
)


@pytest.mark.parametrize("current,new", [
    ("NEW", "PROCESSING"),
    ("NEW", "CANCELLED"),
    ("PROCESSING", "SHIPPED"),
    ("PROCESSING", "CANCELLED"),
    ("SHIPPED", "DELIVERED"),
])
def test_legal_transitions(current, new):
    assert new in ALLOWED_TRANSITIONS[current]
    validate_transition(current, new)  # must not raise


@pytest.mark.parametrize("current,new", [
    ("NEW", "DELIVERED"),          # skipping the pipeline
    ("NEW", "NEW"),                # no-op
    ("SHIPPED", "CANCELLED"),      # too late to cancel
    ("DELIVERED", "PROCESSING"),   # backwards
    ("CANCELLED", "NEW"),          # resurrection
])
def test_illegal_transitions(current, new):
    with pytest.raises(InvalidStateTransition):
        validate_transition(current, new)


def test_unknown_status_is_a_validation_error():
    with pytest.raises(ValidationError):
        validate_transition("NEW", "LOST")
    assert "LOST" not in ORDER_STATUSES
