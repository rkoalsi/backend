from fastapi import APIRouter, Request, Query, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
import datetime

router = APIRouter()

db = get_database()
chats = db["chats"]
chatbot_customers = db["chatbot_customers"]
customers = db["customers"]


def _last10(phone) -> str:
    """Last 10 digits of a phone number, the part that's stable across formats."""
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


def _is_b2b(phone) -> bool:
    """Best-effort: does this sender match an existing B2B customer (phone/mobile)?"""
    tail = _last10(phone)
    if len(tail) < 10:
        return False
    match = customers.find_one(
        {"$or": [
            {"phone": {"$regex": tail}},
            {"mobile": {"$regex": tail}},
        ]},
        {"_id": 1},
    )
    return match is not None


def _register_b2c_contact(phone, body, now):
    """Upsert the inbound sender into the self-building B2C contact registry."""
    if not phone:
        return
    chatbot_customers.update_one(
        {"phone": phone},
        {
            "$set": {"last_seen": now, "last_message": body},
            "$inc": {"message_count": 1},
            "$setOnInsert": {
                "phone": phone,
                "phone_last10": _last10(phone),
                "name": None,
                "source": "whatsapp",
                "is_b2b": _is_b2b(phone),
                "reviewed": False,
                "notes": None,
                "first_seen": now,
            },
        },
        upsert=True,
    )


def _collect_media(payload: dict) -> list:
    """Return any Media0, Media1, ... URLs present in an inbound Plivo payload."""
    media = []
    i = 0
    while True:
        url = payload.get(f"Media{i}")
        if not url:
            break
        media.append(url)
        i += 1
    return media


@router.post("/callback")
async def plivo_callback(request: Request):
    """
    Single endpoint that receives several distinct webhook shapes:
      1. Meta WABA events    -> {"object": "whatsapp_business_account", "entry": [...]}
                                (template approvals/archives, account updates)
      2. Plivo delivery report -> has Status (queued/sent/delivered/read/failed/...)
      3. Plivo error report    -> has ErrorCode, no Status  -> treated as failed
      4. Plivo inbound message -> has Body and/or Media{n}   -> stored as "incoming"
    """
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    now = datetime.datetime.now()

    # 1. Meta WhatsApp Business Account events (template status, etc.) -- not a Plivo
    #    message callback; never touch outgoing message status with these.
    if isinstance(payload, dict) and (payload.get("object") == "whatsapp_business_account" or "entry" in payload):
        chats.insert_one({
            "type": "waba_event",
            "raw_payload": payload,
            "created_at": now,
        })
        print("[callback] stored waba_event")
        return {"message": "ok"}

    message_uuid = payload.get("MessageUUID") or payload.get("message_uuid")
    from_number = payload.get("From") or payload.get("from_number") or payload.get("from")
    to_number = payload.get("To") or payload.get("to_number") or payload.get("to")
    status = payload.get("Status") or payload.get("status")
    error_code = payload.get("ErrorCode") or payload.get("error_code")
    body = payload.get("Body") or payload.get("text")
    media = _collect_media(payload)

    # 4. Inbound customer message (text or media). Distinguished by having content.
    if body or media:
        chats.insert_one({
            "type": "incoming",
            "from": from_number,
            "to": to_number,
            "body": body,
            "media": media or None,
            "message_uuid": message_uuid,
            "raw_payload": payload,
            "created_at": now,
        })
        # Self-building B2C contact registry (B2B clients live in `customers`).
        try:
            _register_b2c_contact(from_number, body, now)
        except Exception as e:
            print(f"[callback] failed to register b2c contact: {e}")
        print(f"[callback] incoming from={from_number} body={str(body)[:40]!r} media={len(media)}")
        return {"message": "ok"}

    # 2/3. Delivery status or error report. Derive a status even when Plivo omits one.
    if not status and error_code and str(error_code) != "0":
        status = "failed"

    if message_uuid:
        set_fields = {"last_callback_at": now}
        # Only overwrite status when we actually have one -- never wipe with None.
        if status:
            set_fields["status"] = status
        if error_code:
            set_fields["error_code"] = str(error_code)

        result = chats.update_one(
            {"type": "outgoing", "message_uuid": message_uuid},
            {
                "$set": set_fields,
                "$setOnInsert": {
                    "type": "outgoing",
                    "message_uuid": message_uuid,
                    "from": from_number,
                    "to": to_number,
                    "raw_payload": payload,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        print(f"[callback] status={status} uuid={message_uuid} matched={result.matched_count} modified={result.modified_count} upserted={result.upserted_id}")
    else:
        # Unrecognised shape -- keep it for inspection rather than dropping it.
        chats.insert_one({
            "type": "callback",
            "from": from_number,
            "to": to_number,
            "status": status,
            "raw_payload": payload,
            "created_at": now,
        })
        print("[callback] stored unmatched callback (no message_uuid)")

    return {"message": "ok"}


@router.get("/", dependencies=[Depends(JWTBearer())])
def get_chats(
    chat_type: str = Query(None, description="Filter by type: outgoing, incoming, callback"),
    phone: str = Query(None, description="Filter by phone number (from or to)"),
    limit: int = Query(50, le=500),
    skip: int = Query(0, ge=0),
):
    query = {}
    if chat_type:
        query["type"] = chat_type
    if phone:
        query["$or"] = [{"from": phone}, {"to": phone}]

    results = list(
        chats.find(query, {"raw_payload": 0})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    total = chats.count_documents(query)

    return {
        "data": serialize_mongo_document(results),
        "total": total,
        "limit": limit,
        "skip": skip,
    }
