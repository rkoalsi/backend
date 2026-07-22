from fastapi import APIRouter, Depends, HTTPException, Request
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from datetime import datetime, timedelta
import os
from jose import jwt, JWTError

router = APIRouter()
db = get_database()
presence_collection = db["user_presence"]

# One document per user (upserted on every heartbeat). TTL index removes users
# who haven't sent a heartbeat in 5 minutes, so the collection never grows
# beyond the number of concurrently active users.
try:
    presence_collection.create_index("last_seen", expireAfterSeconds=300)
    presence_collection.create_index("user_id", unique=True)
except Exception:
    pass

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

# Users whose last heartbeat is within this window count as "online".
ONLINE_WINDOW_SECONDS = 120


def _decode_user(token: str) -> dict:
    """Decode the JWT and return the minimal user payload dict."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_data = payload.get("data", {})
        if not isinstance(user_data, dict) or not user_data.get("_id"):
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return user_data
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/heartbeat")
def heartbeat(payload: dict, request: Request, token: str = Depends(JWTBearer())):
    """
    Presence ping from the frontend (any logged-in role). Upserts the caller's
    single presence document — never inserts more than one doc per user.
    """
    user = _decode_user(token)

    current_page = payload.get("current_page")
    if not isinstance(current_page, str):
        current_page = None

    presence_collection.update_one(
        {"user_id": user["_id"]},
        {
            "$set": {
                "last_seen": datetime.utcnow(),
                "name": user.get("name") or user.get("email") or "",
                "email": user.get("email"),
                "role": user.get("role") or "unknown",
                "customer_id": user.get("customer_id"),
                "code": user.get("code"),
                "current_page": current_page,
            },
            "$setOnInsert": {"first_seen": datetime.utcnow()},
        },
        upsert=True,
    )
    return {"status": "ok"}


@router.post("/heartbeat/guest")
def guest_heartbeat(payload: dict, request: Request):
    """
    Presence ping from anonymous visitors: shared order-form links
    (?shared=true) and public pages (login, register, catalogues, blog...).
    No auth — identified by a client-generated visitor_id (localStorage UUID).
    Upsert-only, so a visitor can never create more than one document.
    """
    visitor_id = payload.get("visitor_id")
    if not isinstance(visitor_id, str) or not (8 <= len(visitor_id) <= 64):
        raise HTTPException(status_code=400, detail="Invalid visitor_id")

    current_page = payload.get("current_page")
    if not isinstance(current_page, str):
        current_page = None
    else:
        current_page = current_page[:200]

    # Shared order-form guests are worth distinguishing from casual visitors.
    role = "shared_guest" if payload.get("shared") else "guest"

    ip = request.headers.get("X-Forwarded-For")
    if ip:
        ip = ip.split(",")[0].strip()
    elif request.client:
        ip = request.client.host

    presence_collection.update_one(
        {"user_id": f"guest:{visitor_id}"},
        {
            "$set": {
                "last_seen": datetime.utcnow(),
                "name": "Shared Order Guest" if role == "shared_guest" else "Visitor",
                "email": None,
                "role": role,
                "current_page": current_page,
                "ip_address": ip,
            },
            "$setOnInsert": {"first_seen": datetime.utcnow()},
        },
        upsert=True,
    )
    return {"status": "ok"}


@router.get("/online")
def online_users(_: str = Depends(JWTBearer())):
    """
    Admin view: everyone whose last heartbeat is within the online window —
    logged-in users plus anonymous guests — with a per-role breakdown. Page access is gated by the /admin/active_users
    permission (admin-only) on the frontend.
    """
    cutoff = datetime.utcnow() - timedelta(seconds=ONLINE_WINDOW_SECONDS)
    docs = list(
        presence_collection.find({"last_seen": {"$gte": cutoff}}).sort("last_seen", -1)
    )

    now = datetime.utcnow()
    role_counts: dict = {}
    users = []
    for d in docs:
        role = d.get("role") or "unknown"
        role_counts[role] = role_counts.get(role, 0) + 1
        doc = serialize_mongo_document(d)
        doc["seconds_ago"] = max(0, int((now - d["last_seen"]).total_seconds()))
        users.append(doc)

    return {
        "online_count": len(users),
        "role_counts": role_counts,
        "window_seconds": ONLINE_WINDOW_SECONDS,
        "users": users,
    }
