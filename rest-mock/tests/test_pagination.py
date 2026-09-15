"""Pagination tests: the DoD walk (ALL data, no dupes, no gaps), cursor
validation, and limit bounds."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from restmock.app import _encode_cursor, create_app
from restmock.data import PROMOTION_COUNT, PRODUCT_COUNT


def _walk(client: TestClient, path: str, limit: int) -> tuple[list[int], int]:
    seen = []
    cursor = None
    total = None
    while True:
        resp = client.get(path, params={"limit": limit, **({"cursor": cursor} if cursor else {})})
        assert resp.status_code == 200
        payload = resp.json()
        total = payload["total"]
        seen.extend(item["id"] for item in payload["data"])
        cursor = payload["next_cursor"]
        if cursor is None:
            break
    return seen, total


def test_promotions_walk_covers_everything_exactly_once(client):
    seen, total = _walk(client, "/promotions", 7)  # ragged limit on purpose
    assert total == PROMOTION_COUNT
    assert len(seen) == PROMOTION_COUNT
    assert seen == sorted(seen)  # keyset order is stable
    assert len(set(seen)) == PROMOTION_COUNT  # no dupes


def test_products_walk_covers_everything_exactly_once(client):
    seen, total = _walk(client, "/products", 64)
    assert total == PRODUCT_COUNT
    assert seen == list(range(1, PRODUCT_COUNT + 1))


def test_default_page_shape(client):
    resp = client.get("/promotions")
    payload = resp.json()
    assert resp.headers["x-ratelimit-remaining"].isdigit()
    assert payload["total"] == PROMOTION_COUNT
    assert len(payload["data"]) == 25
    assert payload["next_cursor"]
    assert set(payload["data"][0]) == {"id", "product_sku", "discount_percent", "starts_at", "ends_at", "description"}


def test_last_page_has_no_next_cursor(client):
    resp = client.get("/promotions", params={"limit": 100})
    while resp.json()["next_cursor"] is not None:
        resp = client.get("/promotions", params={"limit": 100, "cursor": resp.json()["next_cursor"]})
    assert resp.json()["next_cursor"] is None
    assert resp.json()["data"]  # the final page is non-empty


def test_cursor_from_last_item_returns_empty_page(client):
    tail = client.get("/promotions", params={"limit": 1, "cursor": _encode_cursor(PROMOTION_COUNT)})
    assert tail.status_code == 200
    assert tail.json()["data"] == []
    assert tail.json()["next_cursor"] is None


def test_garbage_cursor_is_400(client):
    assert client.get("/promotions", params={"cursor": "not-a-cursor"}).status_code == 400


def test_cursor_id_beyond_catalog_is_400(client):
    resp = client.get("/promotions", params={"cursor": _encode_cursor(PROMOTION_COUNT + 999)})
    assert resp.status_code == 400


@pytest.mark.parametrize("limit", [0, 101, -3])
def test_limit_bounds_are_enforced(client, limit):
    assert client.get("/promotions", params={"limit": limit}).status_code == 422
