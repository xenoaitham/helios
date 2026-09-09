"""API-level tests: catalog shape, oltp price coherence, health, detail route."""

from __future__ import annotations

import re

from restmock.data import CATALOG_SEED, PROMOTION_COUNT, build_catalog


def test_health_payload(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_product_shape_and_sku_format(client):
    item = client.get("/products", params={"limit": 1}).json()["data"][0]
    assert re.fullmatch(r"SKU-\d{5}", item["sku"])
    assert item["currency"] == "USD"
    assert isinstance(item["price_cents"], int)  # integer cents, never floats


def test_product_prices_match_the_oltp_formula(client):
    """Coherence contract with ADR-002: same sku space, same price formula."""
    for item in client.get("/products", params={"limit": 100}).json()["data"]:
        sku_num = int(item["sku"].split("-")[1])
        assert item["price_cents"] == 199 + (sku_num * 613) % 14999


def test_promotion_detail_roundtrip(client):
    payload = client.get("/promotions", params={"limit": 1}).json()["data"][0]
    detail = client.get(f"/promotions/{payload['id']}")
    assert detail.status_code == 200
    assert detail.json() == payload


def test_unknown_promotion_is_404(client):
    assert client.get("/promotions/999999").status_code == 404


def test_discounts_are_in_range_and_windows_ordered(client):
    promotions = client.get("/promotions", params={"limit": 100}).json()["data"]
    for promo in promotions:
        assert 5 <= promo["discount_percent"] <= 50
        assert promo["starts_at"] < promo["ends_at"]


def test_catalog_build_is_deterministic():
    assert build_catalog() == build_catalog()


def test_catalog_sizes():
    products, promotions = build_catalog()
    assert len(products) == 5000
    assert len(promotions) == PROMOTION_COUNT
    assert len({p["sku"] for p in products}) == len(products)  # skus unique
