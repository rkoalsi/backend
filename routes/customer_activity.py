from fastapi import APIRouter, Request, BackgroundTasks, HTTPException, Query, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from datetime import datetime
from typing import Optional
from bson import ObjectId
import os
from jose import jwt, JWTError

router = APIRouter()
db = get_database()
activity_collection = db["customer_activity_logs"]

# Ensure indexes exist for efficient querying
try:
    activity_collection.create_index([("customer_id", 1), ("timestamp", -1)])
    activity_collection.create_index([("action", 1), ("timestamp", -1)])
    activity_collection.create_index("timestamp")
except Exception:
    pass

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")


def log_activity(
    action: str,
    category: str,
    user_id: str = None,
    customer_id: str = None,
    customer_name: str = None,
    email: str = None,
    metadata: dict = None,
    ip_address: str = None,
    user_agent: str = None,
):
    """Insert a customer activity log entry. Safe to call as a background task."""
    try:
        activity_collection.insert_one({
            "user_id": ObjectId(user_id),
            "customer_id": customer_id,
            "customer_name": customer_name,
            "email": email,
            "action": action,
            "category": category,
            "metadata": metadata or {},
            "ip_address": ip_address,
            "user_agent": user_agent,
            "timestamp": datetime.utcnow(),
        })
    except Exception as e:
        print(f"[activity_log] error: {e}")


def extract_client_info(request: Request):
    """Return (ip_address, user_agent) from the request."""
    ip = request.headers.get("X-Forwarded-For")
    if ip:
        ip = ip.split(",")[0].strip()
    elif request.client:
        ip = request.client.host
    user_agent = request.headers.get("User-Agent")
    return ip, user_agent


def decode_customer_from_token(token: str) -> dict:
    """Decode JWT and return the user data dict, or raise HTTPException."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_data = payload.get("data", {})
        if not isinstance(user_data, dict):
            raise HTTPException(status_code=403, detail="Not a customer account")
        return user_data
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def log_order_activity_for_user(
    action: str,
    user_id: str,
    metadata: dict = None,
    ip_address: str = None,
    user_agent: str = None,
):
    """
    Look up a user by their MongoDB _id and log an order-related activity if they
    are a customer account (i.e. have a customer_id field). Safe to call as a
    background task — silently skips salesperson/admin accounts.
    """
    try:
        user = db.users.find_one({"_id": ObjectId(user_id)})
        if not user or not user.get("customer_id"):
            return
        customer_name = (
            user.get("contact_name")
            or f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        )
        log_activity(
            action=action,
            category="orders",
            user_id=str(user["_id"]),
            customer_id=user.get("customer_id"),
            customer_name=customer_name,
            email=user.get("email"),
            metadata=metadata or {},
            ip_address=ip_address,
            user_agent=user_agent,
        )
    except Exception as e:
        print(f"[activity_log] order activity error: {e}")


# ---------------------------------------------------------------------------
# Public tracking endpoint — called by frontend (requires valid JWT)
# ---------------------------------------------------------------------------

@router.post("/track")
async def track_activity(
    payload: dict,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Receive a customer activity event from the frontend.
    JWT token must be present in the Authorization header.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")

    user_data = decode_customer_from_token(auth_header[7:])

    user_id = user_data.get("_id")
    customer_id = user_data.get("customer_id")
    customer_name = (
        user_data.get("contact_name")
        or f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
    )
    email = user_data.get("email")
    ip, ua = extract_client_info(request)

    background_tasks.add_task(
        log_activity,
        action=payload.get("action", "unknown"),
        category=payload.get("category", "portal"),
        user_id=user_id,
        customer_id=customer_id,
        customer_name=customer_name,
        email=email,
        metadata=payload.get("metadata", {}),
        ip_address=ip,
        user_agent=ua,
    )

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Admin endpoints — require JWT (attached at router level in api.py)
# ---------------------------------------------------------------------------

@router.get("/")
def list_activity(
    customer_id: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    _=Depends(JWTBearer()),
):
    query = {}
    if customer_id:
        query["customer_id"] = customer_id
    if user_id:
        query["user_id"] = user_id
    if action:
        query["action"] = action
    if category:
        query["category"] = category
    if from_date or to_date:
        date_query = {}
        if from_date:
            date_query["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_query["$lte"] = datetime.fromisoformat(to_date)
        query["timestamp"] = date_query

    total = activity_collection.count_documents(query)
    skip = (page - 1) * per_page
    docs = list(
        activity_collection.find(query).sort("timestamp", -1).skip(skip).limit(per_page)
    )

    return {
        "activities": [serialize_mongo_document(d) for d in docs],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
    }


@router.get("/summary")
def activity_summary(_=Depends(JWTBearer())):
    """
    Per-customer rollup: last login, login count, total actions, action breakdown.
    Customers with no customer_id (salesperson accounts) are excluded.
    """
    pipeline = [
        {"$match": {"customer_id": {"$ne": None, "$exists": True}}},
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$last": "$customer_name"},
                "email": {"$last": "$email"},
                "user_id": {"$last": "$user_id"},
                "last_seen": {"$max": "$timestamp"},
                "last_login": {
                    "$max": {
                        "$cond": [{"$eq": ["$action", "login"]}, "$timestamp", None]
                    }
                },
                "login_count": {
                    "$sum": {"$cond": [{"$eq": ["$action", "login"]}, 1, 0]}
                },
                "total_actions": {"$sum": 1},
                "actions": {"$push": "$action"},
            }
        },
        {"$sort": {"last_seen": -1}},
    ]

    results = list(activity_collection.aggregate(pipeline, allowDiskUse=True))

    summaries = []
    for r in results:
        action_counts: dict = {}
        for a in r.get("actions", []):
            action_counts[a] = action_counts.get(a, 0) + 1
        summaries.append(serialize_mongo_document({
            "customer_id": r["_id"],
            "customer_name": r.get("customer_name"),
            "email": r.get("email"),
            "user_id": r.get("user_id"),
            "last_login": r.get("last_login"),
            "login_count": r.get("login_count", 0),
            "total_actions": r.get("total_actions", 0),
            "last_seen": r.get("last_seen"),
            "action_counts": action_counts,
        }))

    return {"summary": summaries, "total_customers": len(summaries)}
