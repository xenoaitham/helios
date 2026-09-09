"""FastAPI application factory for the Pricing & Promotions mock (ADR-003).

`create_app()` takes injectable flake/rate/catalog params so tests build their
own instances; the compose service uses env defaults. /health is never
rate-limited and never flakes (healthcheck reliability). Data routes apply
rate_guard (429 + Retry-After) then flake_guard (deterministic 500).
"""

from __future__ import annotations

import base64
import json
import math
import os
import time

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response

from restmock.data import build_catalog
from restmock.flake import Flaker
from restmock.limit import RateLimiter

PAGE_LIMIT_DEFAULT = 25
PAGE_LIMIT_MAX = 100
_ANONYMOUS_BUCKET = "anonymous"


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _encode_cursor(last_id: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"i": last_id}).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        last = payload["i"]
        if not isinstance(last, int) or last < 0:
            raise ValueError("cursor id out of range")
        return last
    except Exception as exc:
        raise HTTPException(status_code=400, detail="invalid cursor") from exc


def create_app(
    *,
    flake_percent: int | None = None,
    flake_seed: str | None = None,
    rate_capacity: int | None = None,
    rate_refill_per_sec: int | None = None,
    catalog: tuple[list[dict], list[dict]] | None = None,
) -> FastAPI:
    flake_percent = flake_percent if flake_percent is not None else _env_int("REST_MOCK_FLAKE_PERCENT", 5)
    flake_seed = flake_seed if flake_seed is not None else os.environ.get("REST_MOCK_FLAKE_SEED", "42")
    rate_capacity = rate_capacity if rate_capacity is not None else _env_int("REST_MOCK_RATE_CAPACITY", 30)
    rate_refill = rate_refill_per_sec if rate_refill_per_sec is not None else _env_int("REST_MOCK_RATE_REFILL_PER_SEC", 10)

    products, promotions = catalog if catalog is not None else build_catalog()
    products_by_id = {p["id"]: p for p in products}
    promotions_by_id = {p["id"]: p for p in promotions}
    # keyset cursors need id -> list position, not id -> item
    product_positions = {p["id"]: i for i, p in enumerate(products)}
    promotion_positions = {p["id"]: i for i, p in enumerate(promotions)}
    limiter = RateLimiter(rate_capacity, rate_refill)
    flaker = Flaker(flake_percent, flake_seed)

    app = FastAPI(title="HELIOS Pricing & Promotions (mock)", docs_url=None, redoc_url=None)

    async def rate_guard(request: Request) -> None:
        key = request.headers.get("x-api-key") or _ANONYMOUS_BUCKET
        allowed, retry_after, remaining = limiter.acquire(key, time.time())
        request.state.rate_remaining = remaining
        if not allowed:
            if math.isfinite(retry_after):
                header = str(min(60, max(1, math.ceil(retry_after))))
            else:
                header = "60"  # no refill configured: ask the client to back off
            raise HTTPException(status_code=429, detail="rate limit exceeded", headers={"Retry-After": header})

    async def flake_guard(request: Request) -> None:
        if flaker.should_fail(request.url.path):
            raise HTTPException(
                status_code=500,
                detail={"message": "transient upstream failure", "retryable": True},
            )

    def _mark_remaining(request: Request, response: Response) -> None:
        response.headers["X-RateLimit-Remaining"] = str(getattr(request.state, "rate_remaining", "?"))

    def _page(items: list[dict], id_index: dict[int, int], cursor: str | None, limit: int) -> dict:
        start = 0
        if cursor is not None:
            last = _decode_cursor(cursor)
            if last not in id_index:
                raise HTTPException(status_code=400, detail="invalid cursor")
            start = id_index[last] + 1
        page = items[start : start + limit]
        next_cursor = None
        if page and start + limit < len(items):
            next_cursor = _encode_cursor(page[-1]["id"])
        return {"data": page, "next_cursor": next_cursor, "total": len(items)}

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/products", dependencies=[Depends(rate_guard), Depends(flake_guard)])
    async def list_products(
        request: Request,
        response: Response,
        cursor: str | None = None,
        limit: int = Query(default=PAGE_LIMIT_DEFAULT, ge=1, le=PAGE_LIMIT_MAX),
    ) -> dict:
        payload = _page(products, product_positions, cursor, limit)
        _mark_remaining(request, response)
        return payload

    @app.get("/promotions", dependencies=[Depends(rate_guard), Depends(flake_guard)])
    async def list_promotions(
        request: Request,
        response: Response,
        cursor: str | None = None,
        limit: int = Query(default=PAGE_LIMIT_DEFAULT, ge=1, le=PAGE_LIMIT_MAX),
    ) -> dict:
        payload = _page(promotions, promotion_positions, cursor, limit)
        _mark_remaining(request, response)
        return payload

    @app.get("/promotions/{promo_id}", dependencies=[Depends(rate_guard), Depends(flake_guard)])
    async def get_promotion(promo_id: int, request: Request, response: Response) -> dict:
        promotion = promotions_by_id.get(promo_id)
        if promotion is None:
            raise HTTPException(status_code=404, detail="promotion not found")
        _mark_remaining(request, response)
        return promotion

    return app


app = create_app()
