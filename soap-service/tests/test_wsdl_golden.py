"""Contract drift test: the served WSDL must match the frozen golden artifact.

The golden copy (contract/OrderManagement.wsdl) is what downstream consumers
generated their clients from. If this test fails, the contract changed —
regenerate client code and re-freeze consciously via `make contract-freeze`.

Comparison uses stdlib xml.etree.ElementTree with explicit hardening: any
document containing a DTD/ENTITY declaration, or larger than the size cap, is
rejected before parsing (the WSDL is strictly data).
"""
import os
import xml.etree.ElementTree as ET
from pathlib import Path

WSDL_NS = "{http://schemas.xmlsoap.org/wsdl/}"
XSD_NS = "{http://www.w3.org/2001/XMLSchema}"
GOLDEN = Path(__file__).resolve().parents[1] / "contract" / "OrderManagement.wsdl"
MAX_WSDL_BYTES = 1_000_000


def _reject_dtd(wsdl_bytes):
    if len(wsdl_bytes) > MAX_WSDL_BYTES:
        raise ValueError(f"WSDL exceeds the {MAX_WSDL_BYTES}-byte size cap")
    lowered = wsdl_bytes.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("DTD/ENTITY declarations are rejected before parsing")


def _sort_children(parent):
    children = list(parent)
    if len(children) < 2:
        return
    keyed = sorted(children, key=ET.tostring)
    for child in children:
        parent.remove(child)
    for child in keyed:
        parent.append(child)


def _normalize(wsdl_bytes):
    """Comparison form: pin endpoint locations and sort the containers whose
    child order spyne emits in id-order (definitions, wsdl:types, xs:schema).
    Semantic element order *inside* a complexType is significant and untouched."""
    _reject_dtd(wsdl_bytes)
    root = ET.fromstring(wsdl_bytes)
    for element in root.iter():
        for key in list(element.attrib):
            if key.endswith("location") or key.endswith("Location"):
                element.set(key, "LOCATION")
    _sort_children(root)
    for types in root.iter(f"{WSDL_NS}types"):
        _sort_children(types)
    for schema in root.iter(f"{XSD_NS}schema"):
        _sort_children(schema)
    return ET.tostring(root)


def test_golden_contract_exists():
    assert GOLDEN.is_file(), (
        f"missing {GOLDEN}: freeze the served WSDL with "
        "`make contract-freeze` before running the suite"
    )


def test_served_wsdl_matches_golden(good_auth):
    import requests

    served = requests.get("http://127.0.0.1:18081/?wsdl", auth=good_auth, timeout=10).content
    with open(GOLDEN, "rb") as handle:
        golden = handle.read()
    assert _normalize(served) == _normalize(golden), (
        "served WSDL drifted from the frozen contract — regenerate clients and "
        "re-freeze via `make contract-freeze` (review the diff like an API change)"
    )


def test_golden_declares_all_operations():
    with open(GOLDEN, "rb") as handle:
        golden = handle.read().decode()
    for marker in ("CreateOrder", "GetOrders", "GetOrderStatus", "UpdateOrderStatus"):
        assert marker in golden
    assert os.path.getsize(GOLDEN) > 1000  # a real WSDL, not a stub
