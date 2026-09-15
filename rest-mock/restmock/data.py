"""Deterministic in-memory catalog: 5,000 products + 2,500 promotions (ADR-003).

Products reuse the OLTP sku space and the OLTP unit-price formula
(`199 + (sku_num * 613) % 14999` cents) so Phase-3 warehouse joins are coherent.
All randomness is fixed-seed; windows are anchored to a fixed BASE date so the
built catalog is byte-identical across runs.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

PRODUCT_COUNT = 5000
PROMOTION_COUNT = 2500
CATALOG_SEED = 42
BASE_DATE = datetime(2026, 9, 1)

CATEGORIES = ("garden", "kitchen", "electronics", "toys", "sports", "books", "tools", "apparel")


def price_cents_for_sku(sku_num: int) -> int:
    """Same formula as the OLTP seeder's item prices (ADR-002/ADR-003 coherence)."""
    return 199 + (sku_num * 613) % 14999


def build_products() -> list[dict]:
    products = []
    for i in range(1, PRODUCT_COUNT + 1):
        sku_num = (i * 19) % 100000  # unique: gcd(19, 100000) == 1, spread over the OLTP sku space
        category = CATEGORIES[i % len(CATEGORIES)]
        products.append({
            "id": i,
            "sku": f"SKU-{sku_num:05d}",
            "name": f"{category.title()} Item {i:05d}",
            "category": category,
            "price_cents": price_cents_for_sku(sku_num),
            "currency": "USD",
        })
    return products


def build_promotions(products: list[dict]) -> list[dict]:
    rng = random.Random(CATALOG_SEED)
    promotions = []
    for promo_id in range(1, PROMOTION_COUNT + 1):
        product = products[rng.randrange(len(products))]
        discount = rng.randrange(5, 51)
        start = BASE_DATE + timedelta(days=rng.randrange(0, 60))
        end = start + timedelta(days=rng.randrange(30, 181))
        promotions.append({
            "id": promo_id,
            "product_sku": product["sku"],
            "discount_percent": discount,
            "starts_at": start.date().isoformat(),
            "ends_at": end.date().isoformat(),
            "description": f"{discount}% off {product['name']}",
        })
    return promotions


def build_catalog() -> tuple[list[dict], list[dict]]:
    products = build_products()
    return products, build_promotions(products)
