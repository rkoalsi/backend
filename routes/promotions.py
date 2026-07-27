"""
Merchandising placements shown inside the order form.

Two placements exist today:

  * ``brand_banner`` — one banner directly above the product grid for the
    selected brand.
  * ``in_scroll``   — a full-width band injected into the product grid after
    every N products.

A note on naming: nothing user-facing here says "ad", "banner" or "sponsored"
in a class name, route or S3 key. Browser extensions hide elements and block
requests on exactly those tokens, so a slot named ``/api/ads`` or ``.ad-banner``
silently disappears for any user running a blocker — the element renders, the
request 200s, and you are left debugging a placement that was never painted.
The user-visible *text* can say whatever you like; only the identifiers matter.
"""
import os
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from jose import JWTError, jwt

from ..config.root import get_database, serialize_mongo_document

router = APIRouter()
db = get_database()
promotions_collection = db["promotions"]
events_collection = db["promotion_events"]

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

PLACEMENTS = ("brand_banner", "in_scroll")
EVENTS = ("view", "click")

try:
    promotions_collection.create_index([("is_active", 1), ("placement", 1)])
    events_collection.create_index([("promotion_id", 1), ("created_at", -1)])
    events_collection.create_index([("event", 1), ("created_at", -1)])
except Exception:
    pass


def _viewer(request: Request) -> dict:
    """
    Identify the caller from the auth token. Mirrors JWTBearer: the HttpOnly
    cookie first, Authorization header second.

    Returns {} rather than raising — a placement event must never break the page
    it was fired from, and an unattributed event is still worth recording.
    """
    token = request.cookies.get("access_token")
    if not token:
        header = request.headers.get("Authorization", "")
        if header.startswith("Bearer "):
            token = header[7:]
    if not token:
        return {}
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        data = payload.get("data", {})
        return data if isinstance(data, dict) else {}
    except JWTError:
        return {}


def _client_info(request: Request):
    ip = request.headers.get("X-Forwarded-For")
    if ip:
        ip = ip.split(",")[0].strip()
    elif request.client:
        ip = request.client.host
    return ip, request.headers.get("User-Agent")


def _live_query(placement: str, brand: Optional[str]) -> dict:
    """Active, inside its schedule, and targeted at this brand (or at all brands)."""
    now = datetime.utcnow()
    query: dict = {
        "placement": placement,
        "is_active": True,
        "$and": [
            {"$or": [{"starts_at": None}, {"starts_at": {"$lte": now}}]},
            {"$or": [{"ends_at": None}, {"ends_at": {"$gte": now}}]},
        ],
    }
    if brand:
        # An empty/missing `brands` list means "every brand".
        query["$and"].append(
            {"$or": [{"brands": {"$in": [brand]}}, {"brands": {"$size": 0}}, {"brands": None}]}
        )
    else:
        query["$and"].append({"$or": [{"brands": {"$size": 0}}, {"brands": None}]})
    return query


@router.get("/active")
def get_active_placements(
    brand: Optional[str] = Query(None, description="Brand tab currently open"),
):
    """
    Everything that should render for the current brand, in one call.

    `brand_banner` is a single placement (highest priority wins); `in_scroll`
    is a list, cycled by the client across the grid.
    """
    try:
        banner_docs = list(
            promotions_collection.find(_live_query("brand_banner", brand))
            .sort([("priority", -1), ("created_at", -1)])
            .limit(1)
        )
        scroll_docs = list(
            promotions_collection.find(_live_query("in_scroll", brand))
            .sort([("priority", -1), ("created_at", -1)])
            .limit(10)
        )
        return {
            "brand_banner": serialize_mongo_document(banner_docs[0]) if banner_docs else None,
            "in_scroll": serialize_mongo_document(scroll_docs),
        }
    except Exception as e:
        print(f"[promotions] active fetch failed: {e}")
        # Never fail the catalogue over merchandising.
        return {"brand_banner": None, "in_scroll": []}


def _record(promotion_id: str, event: str, brand: Optional[str], viewer: dict, ip, ua):
    try:
        events_collection.insert_one(
            {
                "promotion_id": ObjectId(promotion_id),
                "event": event,
                "brand": brand,
                "user_id": viewer.get("_id"),
                "customer_id": viewer.get("customer_id"),
                "name": viewer.get("name"),
                "email": viewer.get("email"),
                "role": viewer.get("role"),
                "ip_address": ip,
                "user_agent": ua,
                "created_at": datetime.utcnow(),
            }
        )
        promotions_collection.update_one(
            {"_id": ObjectId(promotion_id)},
            {"$inc": {"impressions" if event == "view" else "clicks": 1}},
        )
    except Exception as e:
        print(f"[promotions] event record failed: {e}")


@router.post("/{promotion_id}/event")
def record_event(
    promotion_id: str,
    payload: dict,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Fire-and-forget view/click recording. Written in the background so the
    click handler never waits on a database round trip.
    """
    event = (payload or {}).get("event")
    if event not in EVENTS:
        raise HTTPException(status_code=400, detail="event must be 'view' or 'click'")
    if not ObjectId.is_valid(promotion_id):
        raise HTTPException(status_code=400, detail="Invalid promotion id")

    ip, ua = _client_info(request)
    background_tasks.add_task(
        _record, promotion_id, event, (payload or {}).get("brand"), _viewer(request), ip, ua
    )
    return {"status": "ok"}
