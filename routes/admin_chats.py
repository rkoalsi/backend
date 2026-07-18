import re
import datetime
from fastapi import APIRouter, Query, Body, HTTPException
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document
from ..config.whatsapp import send_whatsapp_text

# WhatsApp only allows free-form replies within 24h of the user's last message.
SERVICE_WINDOW_SECONDS = 24 * 60 * 60

router = APIRouter()


def _last10(phone) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits

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


def _enrich_outgoing(chat: dict, templates_map: dict | None = None) -> dict:
    template_name = chat.get("template_name")
    params = chat.get("params") or {}
    if template_name:
        if templates_map is not None:
            tmpl = templates_map.get(template_name)
        else:
            tmpl = templates_col.find_one(
                {"name": template_name}, {"body": 1, "header": 1}
            )
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
    status: str = Query(None, description="Filter by delivery status (e.g. failed, delivered, queued)"),
    phone: str = Query(None, description="Filter by phone number (from or to)"),
    limit: int = Query(50, le=100000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if chat_type:
        query["type"] = chat_type
    if status:
        query["status"] = status
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

    # Batch-load all referenced templates once to avoid an N+1 query per
    # outgoing chat (otherwise large exports do tens of thousands of lookups).
    template_names = {
        chat.get("template_name")
        for chat in raw
        if chat.get("type") == "outgoing" and chat.get("template_name")
    }
    templates_map = {}
    if template_names:
        templates_map = {
            t["name"]: t
            for t in templates_col.find(
                {"name": {"$in": list(template_names)}},
                {"name": 1, "body": 1, "header": 1},
            )
        }

    enriched = []
    for chat in raw:
        if chat.get("type") == "outgoing":
            chat = _enrich_outgoing(chat, templates_map)
        enriched.append(chat)

    return {
        "data": serialize_mongo_document(enriched),
        "total": total,
        "limit": limit,
        "skip": skip,
    }


def _last_inbound_at(phone: str):
    """Most recent inbound message timestamp from this number, or None."""
    tail = _last10(phone)
    if not tail:
        return None
    doc = chats_col.find_one(
        {"type": "incoming", "from": {"$regex": tail}},
        {"created_at": 1},
        sort=[("created_at", -1)],
    )
    return doc.get("created_at") if doc else None


@router.get("/window")
def chat_service_window(phone: str = Query(..., description="Phone number to check")):
    """Whether a free-form reply can be sent now (inside the 24h service window)."""
    last_in = _last_inbound_at(phone)
    if not last_in:
        return {"open": False, "last_inbound_at": None, "reason": "no_inbound"}
    age = (datetime.datetime.now() - last_in).total_seconds()
    return {
        "open": age <= SERVICE_WINDOW_SECONDS,
        "last_inbound_at": serialize_mongo_document({"v": last_in})["v"],
        "seconds_remaining": max(0, int(SERVICE_WINDOW_SECONDS - age)),
    }


@router.get("/conversation")
def get_conversation(
    phone: str = Query(..., description="Phone number to load the thread for"),
    limit: int = Query(200, le=1000),
):
    """Full incoming + outgoing message thread for one number, oldest first."""
    tail = _last10(phone)
    if not tail:
        return {"data": []}
    rx = {"$regex": tail}
    raw = list(
        chats_col.find(
            {"type": {"$in": ["incoming", "outgoing"]}, "$or": [{"from": rx}, {"to": rx}]},
            {"raw_payload": 0},
        )
        .sort("created_at", -1)
        .limit(limit)
    )
    raw.reverse()  # chronological for display
    enriched = [_enrich_outgoing(c) if c.get("type") == "outgoing" else c for c in raw]
    return {"data": serialize_mongo_document(enriched)}


@router.post("/reply")
def reply_to_chat(payload: dict = Body(...)):
    """Send a manual free-form WhatsApp reply (e.g. when the bot has no answer)."""
    phone = (payload.get("phone") or "").strip()
    message = (payload.get("message") or "").strip()
    if not phone or not message:
        raise HTTPException(status_code=400, detail="phone and message are required")

    last_in = _last_inbound_at(phone)
    if not last_in:
        raise HTTPException(
            status_code=409,
            detail="No inbound message from this number — free-form replies aren't allowed. Use an approved template.",
        )
    age = (datetime.datetime.now() - last_in).total_seconds()
    if age > SERVICE_WINDOW_SECONDS:
        raise HTTPException(
            status_code=409,
            detail="Outside the 24-hour WhatsApp service window — free-form reply not allowed. Use an approved template.",
        )

    resp = send_whatsapp_text(phone, message)
    if resp is None:
        raise HTTPException(status_code=502, detail="Failed to send WhatsApp message (see server logs).")

    # A human has now responded -> reset the auto-fallback guard so the bot can
    # send the "team will get back" line again for a future unanswered question.
    chatbot_customers.update_one(
        {"phone_last10": _last10(phone)},
        {"$set": {"awaiting_human": False}},
    )
    return {"status": "sent"}


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
