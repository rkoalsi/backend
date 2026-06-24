from fastapi import APIRouter, Request, Query, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from ..config.whatsapp import send_whatsapp_text
import datetime

router = APIRouter()

db = get_database()
chats = db["chats"]
chatbot_customers = db["chatbot_customers"]
customers = db["customers"]
chatbot_answers = db["chatbot_answers"]

# --- Auto-responder configuration -----------------------------------------
GREETINGS = {
    "hi", "hii", "hiii", "hey", "heyy", "hello", "helo", "hellow",
    "yo", "namaste", "hi there", "hey there", "hello there", "start",
}
GREETING_REPLY = "Hello! How may I help you today?"
FALLBACK_REPLY = (
    "Thanks for your message! A member of our team will get back to you "
    "with an answer shortly."
)


def _last10(phone) -> str:
    """Last 10 digits of a phone number, the part that's stable across formats."""
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


def _match_b2b_contacts(phone) -> list:
    """Best-effort: all B2B customer contact_ids whose phone/mobile match this sender.
    There may be more than one customer record per number, so we return every match."""
    tail = _last10(phone)
    if len(tail) < 10:
        return []
    matches = customers.find(
        {"$or": [
            {"phone": {"$regex": tail}},
            {"mobile": {"$regex": tail}},
        ]},
        {"contact_id": 1},
    )
    contact_ids = [m.get("contact_id") for m in matches if m.get("contact_id")]
    # de-dupe while preserving order
    seen = set()
    return [cid for cid in contact_ids if not (cid in seen or seen.add(cid))]


def _register_b2c_contact(phone, body, now):
    """Upsert the inbound sender into the self-building B2C contact registry."""
    if not phone:
        return
    b2b_contact_ids = _match_b2b_contacts(phone)
    chatbot_customers.update_one(
        {"phone": phone},
        {
            # is_b2b / b2b_contact_ids kept fresh on every inbound, since a matching
            # customer record may be created in Zoho after this contact first appears.
            "$set": {
                "last_seen": now,
                "last_message": body,
                "is_b2b": len(b2b_contact_ids) > 0,
                "b2b_contact_ids": b2b_contact_ids,
            },
            "$inc": {"message_count": 1},
            "$setOnInsert": {
                "phone": phone,
                "phone_last10": _last10(phone),
                "name": None,
                "source": "whatsapp",
                "reviewed": False,
                "notes": None,
                "first_seen": now,
            },
        },
        upsert=True,
    )


def _is_greeting(text: str) -> bool:
    t = (text or "").strip().lower().strip("!.?, ")
    return t in GREETINGS


def _match_answer(text: str):
    """Look up a canned answer whose keyword appears in the message. Empty KB -> None."""
    t = (text or "").lower()
    if not t:
        return None
    for ans in chatbot_answers.find({"is_active": True}):
        keywords = ans.get("keywords") or []
        if any(str(kw).lower() in t for kw in keywords if kw):
            return ans.get("answer")
    return None


def _auto_respond(phone: str, body: str, now):
    """Greeting -> greeting reply; known question -> canned answer; otherwise a
    'team will get back' reply, sent only once until a human replies (no spam)."""
    if not phone or not body:
        return  # ignore media-only / empty messages

    if _is_greeting(body):
        send_whatsapp_text(phone, GREETING_REPLY, sent_by="bot")
        return

    answer = _match_answer(body)
    if answer:
        send_whatsapp_text(phone, answer, sent_by="bot")
        chatbot_customers.update_one({"phone": phone}, {"$set": {"awaiting_human": False}})
        return

    # No answer available. Tell them a human will follow up -- but only once,
    # so a burst of unanswered messages doesn't repeat the line every time.
    contact = chatbot_customers.find_one({"phone": phone}, {"awaiting_human": 1})
    if contact and contact.get("awaiting_human"):
        return
    send_whatsapp_text(phone, FALLBACK_REPLY, sent_by="bot")
    chatbot_customers.update_one(
        {"phone": phone},
        {"$set": {"awaiting_human": True, "awaiting_human_since": now}},
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
        # Guard against Plivo webhook retries delivering the same message twice.
        if message_uuid and chats.count_documents(
            {"type": "incoming", "message_uuid": message_uuid}, limit=1
        ):
            print(f"[callback] duplicate incoming uuid={message_uuid}, skipping")
            return {"message": "ok (duplicate)"}

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
        # Auto-reply (greeting / canned answer / 'team will respond' fallback).
        try:
            _auto_respond(from_number, body, now)
        except Exception as e:
            print(f"[callback] auto-respond failed: {e}")
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
