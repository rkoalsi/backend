import re
import datetime
from fastapi import APIRouter, Query, Body, HTTPException
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document

router = APIRouter()

# Separate router for the self-building B2C contact registry, mounted at
# /admin/chatbot_customers (B2B clients live in the `customers` collection).
contacts_router = APIRouter()

db = get_database()
chats_col = db["chats"]
templates_col = db["templates"]
chatbot_customers = db["chatbot_customers"]


def _resolve_body(template_body: str, params: dict) -> str:
    """Replace {{1}}, {{2}}, ... placeholders with ordered param values."""
    values = [str(v) for v in params.values()]
    def replacer(m: re.Match) -> str:
        idx = int(m.group(1)) - 1
        return values[idx] if 0 <= idx < len(values) else m.group(0)
    return re.sub(r"\{\{(\d+)\}\}", replacer, template_body)


def _enrich_outgoing(chat: dict) -> dict:
    template_name = chat.get("template_name")
    params = chat.get("params") or {}
    if template_name:
        tmpl = templates_col.find_one({"name": template_name}, {"body": 1, "header": 1})
        if tmpl:
            body = tmpl.get("body", "")
            if body and params:
                body = _resolve_body(body, params)
            chat["resolved_body"] = body
            chat["template_header"] = tmpl.get("header")
    return chat


@router.get("")
def get_admin_chats(
    chat_type: str = Query(None, description="Filter: outgoing, incoming, callback"),
    phone: str = Query(None, description="Filter by phone number (from or to)"),
    limit: int = Query(50, le=100000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if chat_type:
        query["type"] = chat_type
    if phone:
        stripped = phone.strip().lstrip("+")
        query["$or"] = [
            {"from": {"$regex": stripped, "$options": "i"}},
            {"to": {"$regex": stripped, "$options": "i"}},
        ]

    raw = list(
        chats_col.find(query, {"raw_payload": 0})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    total = chats_col.count_documents(query)

    enriched = []
    for chat in raw:
        if chat.get("type") == "outgoing":
            chat = _enrich_outgoing(chat)
        enriched.append(chat)

    return {
        "data": serialize_mongo_document(enriched),
        "total": total,
        "limit": limit,
        "skip": skip,
    }


# ---------------------------------------------------------------------------
# B2C chatbot contact registry  (mounted at /admin/chatbot_customers)
# ---------------------------------------------------------------------------

@contacts_router.get("")
def list_chatbot_customers(
    phone: str = Query(None, description="Filter by phone (partial match)"),
    reviewed: bool = Query(None, description="Filter by reviewed flag"),
    is_b2b: bool = Query(None, description="Filter by B2B match flag"),
    limit: int = Query(50, le=100000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if phone:
        query["phone"] = {"$regex": phone.strip().lstrip("+"), "$options": "i"}
    if reviewed is not None:
        query["reviewed"] = reviewed
    if is_b2b is not None:
        query["is_b2b"] = is_b2b

    raw = list(
        chatbot_customers.find(query)
        .sort("last_seen", -1)
        .skip(skip)
        .limit(limit)
    )
    total = chatbot_customers.count_documents(query)
    return {
        "data": serialize_mongo_document(raw),
        "total": total,
        "limit": limit,
        "skip": skip,
    }


@contacts_router.patch("/{contact_id}")
def update_chatbot_customer(contact_id: str, payload: dict = Body(...)):
    """Admin-editable fields: name, notes, reviewed."""
    try:
        oid = ObjectId(contact_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid contact id")

    allowed = {k: payload[k] for k in ("name", "notes", "reviewed") if k in payload}
    if not allowed:
        raise HTTPException(status_code=400, detail="No editable fields provided")
    allowed["updated_at"] = datetime.datetime.now()

    result = chatbot_customers.update_one({"_id": oid}, {"$set": allowed})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Contact not found")

    updated = chatbot_customers.find_one({"_id": oid})
    return {"data": serialize_mongo_document(updated)}
