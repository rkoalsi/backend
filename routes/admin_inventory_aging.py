import os
import json
import logging
import asyncio
import requests
from datetime import datetime

from fastapi import APIRouter, HTTPException, Body, Query
from bson.objectid import ObjectId

from ..config.root import get_database
from .helpers import get_access_token

logger = logging.getLogger(__name__)

db = get_database()
router = APIRouter()

org_id = os.getenv("ORG_ID")
ZOHO_BOOKS_BASE = "https://www.zohoapis.com/books/v3"

# Pupscribe Enterprises Private Limited location in Zoho Books
PUPSCRIBE_LOCATION_ID = "3220178000143298047"

# 60-day interval bucket labels returned by the Zoho aging API
LABEL_SLOW = "121 - 180 days"   # Slow Movers
LABEL_DEAD = "> 180 days"       # Dead Stock


def _fetch_all_aging_items(token: str, to_date: str) -> list:
    """Fetch every inventory-aging item from Zoho Books as of ``to_date``.

    Mirrors the inventory_aging report in purchases_backend: 4 columns of
    60-day intervals, filtered to the Pupscribe location.
    """
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    select_columns = json.dumps([
        {"field": "item_name", "group": "item"},
        {"field": "intervals", "group": "report"},
    ])
    rule = json.dumps({
        "columns": [{
            "index": 1,
            "field": "location_name",
            "value": [PUPSCRIBE_LOCATION_ID],
            "comparator": "in",
            "group": "branch",
        }],
        "criteria_string": "( 1 )",
    })
    base_params = {
        "organization_id": org_id,
        "per_page": 500,
        "interval_type": "days",
        "number_of_columns": 4,
        "interval_range": 60,
        "select_columns": select_columns,
        "to_date": to_date,
        "sort_column": "item_id",
        "sort_order": "A",
        "response_option": 1,
        "rule": rule,
    }

    all_items = []
    page = 1
    while True:
        params = {**base_params, "page": page}
        resp = requests.get(
            f"{ZOHO_BOOKS_BASE}/reports/inventoryagingsummary",
            headers=headers,
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code", 0) != 0:
            raise Exception(f"Zoho API error: {data.get('message')}")

        items = data.get("inventory_aging_summary", [])
        all_items.extend(items)
        logger.info(f"Aging page {page}: {len(items)} items (total {len(all_items)})")

        if not data.get("page_context", {}).get("has_more_page", False):
            break
        page += 1

    return all_items


def _extract_qty(intervals: list, label: str) -> float:
    """Return stock_on_hand for the given interval label (0 if absent)."""
    for entry in intervals:
        if entry.get("interval", "").strip() == label:
            return entry.get("stock_on_hand", 0.0) or 0.0
    return 0.0


def _split_buckets(items: list) -> tuple[dict, dict]:
    """Return {item_id: qty} maps for slow movers and dead stock."""
    slow, dead = {}, {}
    for item in items:
        item_id = item.get("item_id")
        if not item_id:
            continue
        intervals = item.get("intervals", [])
        slow_qty = _extract_qty(intervals, LABEL_SLOW)
        dead_qty = _extract_qty(intervals, LABEL_DEAD)
        if slow_qty > 0:
            slow[item_id] = slow_qty
        if dead_qty > 0:
            dead[item_id] = dead_qty
    return slow, dead


def _build_rows(qty_map: dict, product_map: dict) -> list:
    """Join aging qty with product docs; sorted by brand then name."""
    rows = []
    for item_id, qty in qty_map.items():
        prod = product_map.get(item_id)
        if not prod:
            # Item exists in Zoho but not in our products collection — skip,
            # there's nothing to flip for clearance.
            continue
        images = prod.get("images") or []
        image = prod.get("image_url") or (images[0] if images else None)
        rows.append({
            "product_id": str(prod["_id"]),
            "item_id": item_id,
            "name": prod.get("name", ""),
            "brand": prod.get("brand", ""),
            "image": image,
            "mrp": prod.get("rate") or 0.0,
            "stock": prod.get("stock") or 0,
            "status": prod.get("status") or "",
            "aging_qty": qty,
            "clearance": bool(prod.get("clearance", False)),
            "clearance_margin": prod.get("clearance_margin"),
        })
    rows.sort(key=lambda r: ((r["brand"] or "").lower(), (r["name"] or "").lower()))
    return rows


@router.get("/report")
async def inventory_aging_report(
    to_date: str = Query(..., description="Report date (YYYY-MM-DD)"),
):
    """Slow Movers (121-180 days) and Dead Stock (>180 days) as of ``to_date``.

    Joins the Zoho aging buckets to our products collection so each row carries
    its current clearance flag/margin and can be flipped via /bulk_clearance.
    """
    try:
        datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="to_date must be YYYY-MM-DD")

    token = await asyncio.to_thread(get_access_token, "books")
    if not token:
        raise HTTPException(status_code=502, detail="Could not obtain Zoho access token")

    items = await asyncio.to_thread(_fetch_all_aging_items, token, to_date)

    slow_map, dead_map = _split_buckets(items)
    all_item_ids = list(set(slow_map) | set(dead_map))

    def _load_products():
        return list(db.products.find(
            {"item_id": {"$in": all_item_ids}},
            {"item_id": 1, "name": 1, "brand": 1, "image_url": 1, "images": 1,
             "rate": 1, "stock": 1, "status": 1, "clearance": 1, "clearance_margin": 1},
        ))

    product_docs = await asyncio.to_thread(_load_products)
    product_map = {p["item_id"]: p for p in product_docs}

    return {
        "to_date": to_date,
        "slow_movers": _build_rows(slow_map, product_map),
        "dead_stock": _build_rows(dead_map, product_map),
    }


@router.post("/bulk_clearance")
def bulk_clearance(payload: dict = Body(...)):
    """Bulk set/unset clearance + clearance_margin on the given products.

    Body: { "product_ids": [...], "clearance": true, "clearance_margin": 15 }
    Mirrors the single-product update in admin.update_product.
    """
    product_ids = payload.get("product_ids") or []
    if not product_ids:
        raise HTTPException(status_code=400, detail="product_ids is required")

    object_ids = []
    for pid in product_ids:
        if not ObjectId.is_valid(pid):
            raise HTTPException(status_code=400, detail=f"Invalid product ID: {pid}")
        object_ids.append(ObjectId(pid))

    update_dict = {}
    if "clearance" in payload:
        update_dict["clearance"] = bool(payload["clearance"])
    # clearance_margin sent as 0 to clear it, so presence (not truthiness) gates it
    if payload.get("clearance_margin") is not None:
        update_dict["clearance_margin"] = float(payload["clearance_margin"])

    if not update_dict:
        raise HTTPException(status_code=400, detail="Nothing to update")

    result = db.products.update_many(
        {"_id": {"$in": object_ids}},
        {"$set": update_dict},
    )
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "updated": update_dict,
    }
