from fastapi import APIRouter, Depends, HTTPException
from bson import ObjectId
import datetime
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import get_current_user

router = APIRouter()

NOTIFICATION_TYPES = {
    # orders
    "order_placed": "Order Placed",
    "order_edited": "Order Edited",
    # catalogue / products
    "new_catalogue": "New Catalogue",
    "product_back_in_stock": "Product Back in Stock",
    # returns
    "return_order_created": "Return Order Created",
    # customer creation requests
    "customer_request_submitted": "New Customer Request",
    "customer_request_status": "Customer Request Updated",
    "customer_request_comment": "Comment on Customer Request",
    "customer_request_reply": "Reply on Customer Request",
    # daily visits
    "daily_visit_created": "Daily Visit Created",
    "daily_visit_updated": "Daily Visit Updated",
    "daily_visit_comment": "Comment on Daily Visit",
    # broadcasts
    "new_training": "New Training Video",
    "new_announcement": "New Announcement",
    # shipments
    "shipment_dispatched": "Shipment Dispatched",
    "shipment_delivered": "Shipment Delivered",
}


def create_notification(
    db,
    recipient_id: str,
    notification_type: str,
    title: str,
    body: str,
    link: str,
    extra: dict = None,
):
    """
    Insert a single notification for one recipient.
    Skips insert if an identical notification (same recipient, type, title)
    already exists, preventing duplicates from double-submits or retries.
    recipient_id: str of the user's _id
    """
    try:
        existing = db.order_form_notifications.find_one({
            "recipient_id": ObjectId(recipient_id),
            "type": notification_type,
            "title": title,
        })
        if existing:
            return
        doc = {
            "recipient_id": ObjectId(recipient_id),
            "type": notification_type,
            "title": title,
            "body": body,
            "link": link,
            "read": False,
            "created_at": datetime.datetime.utcnow(),
        }
        if extra:
            doc["extra"] = extra
        db.order_form_notifications.insert_one(doc)
    except Exception as e:
        print(f"[notifications] Failed to create notification for {recipient_id}: {e}")


def create_notifications_for_role(
    db,
    role: str,
    notification_type: str,
    title: str,
    body: str,
    link: str,
    extra: dict = None,
    extra_query: dict = None,
):
    """Send the same notification to every active user with the given role."""
    query = {"role": role, "status": "active"}
    if extra_query:
        query.update(extra_query)
    users = db.users.find(query, {"_id": 1})
    for user in users:
        create_notification(db, str(user["_id"]), notification_type, title, body, link, extra)


def create_notifications_for_roles(
    db,
    roles: list,
    notification_type: str,
    title: str,
    body: str,
    link: str,
    extra: dict = None,
):
    """Send the same notification to every active user belonging to any of the given roles."""
    users = db.users.find({"role": {"$in": roles}, "status": "active"}, {"_id": 1})
    for user in users:
        create_notification(db, str(user["_id"]), notification_type, title, body, link, extra)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/unread-count")
def get_unread_count(current_user: dict = Depends(get_current_user)):
    db = get_database()
    user_id = current_user.get("data", {}).get("_id") or current_user.get("_id")
    if not user_id:
        return {"count": 0}
    count = db.order_form_notifications.count_documents(
        {"recipient_id": ObjectId(user_id), "read": False}
    )
    return {"count": count}


@router.get("")
def get_notifications(
    page: int = 0,
    limit: int = 20,
    current_user: dict = Depends(get_current_user),
):
    db = get_database()
    user_id = current_user.get("data", {}).get("_id") or current_user.get("_id")
    if not user_id:
        return {"notifications": [], "total": 0}

    query = {"recipient_id": ObjectId(user_id)}
    total = db.order_form_notifications.count_documents(query)
    docs = (
        db.order_form_notifications.find(query)
        .sort([("read", 1), ("created_at", -1)])
        .skip(page * limit)
        .limit(limit)
    )
    return {
        "notifications": serialize_mongo_document(list(docs)),
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.patch("/{notification_id}/read")
def mark_read(
    notification_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_database()
    user_id = current_user.get("data", {}).get("_id") or current_user.get("_id")
    result = db.order_form_notifications.update_one(
        {"_id": ObjectId(notification_id), "recipient_id": ObjectId(user_id)},
        {"$set": {"read": True}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"ok": True}


@router.patch("/read-all")
def mark_all_read(current_user: dict = Depends(get_current_user)):
    db = get_database()
    user_id = current_user.get("data", {}).get("_id") or current_user.get("_id")
    db.order_form_notifications.update_many(
        {"recipient_id": ObjectId(user_id), "read": False},
        {"$set": {"read": True}},
    )
    return {"ok": True}
