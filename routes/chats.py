from fastapi import APIRouter, Request, Query
from ..config.root import get_database, serialize_mongo_document
import datetime

router = APIRouter()

db = get_database()
chats = db["chats"]


@router.post("/callback")
async def plivo_callback(request: Request):
    """
    Receives Plivo WhatsApp delivery status callbacks and incoming messages.
    Plivo sends form-encoded or JSON payloads depending on the event type.
    """
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    message_uuid = payload.get("MessageUUID") or payload.get("message_uuid")
    from_number = payload.get("From") or payload.get("from_number") or payload.get("from")
    to_number = payload.get("To") or payload.get("to_number") or payload.get("to")
    status = payload.get("Status") or payload.get("status")
    body = payload.get("Body") or payload.get("text")

    # Determine if this is an incoming message or a delivery callback
    if body and from_number:
        chat_type = "incoming"
    else:
        chat_type = "callback"

    doc = {
        "type": chat_type,
        "from": from_number,
        "to": to_number,
        "body": body,
        "status": status,
        "message_uuid": message_uuid,
        "raw_payload": payload,
        "created_at": datetime.datetime.now(),
    }

    print(f"[callback] type={chat_type} uuid={message_uuid} status={status} from={from_number} to={to_number}")

    # If it's a delivery callback, try to update the matching outgoing message
    if chat_type == "callback" and message_uuid:
        result = chats.update_one(
            {"type": "outgoing", "message_uuid": message_uuid},
            {"$set": {"status": status, "last_callback_at": datetime.datetime.now()}},
        )
        print(f"[callback] update matched={result.matched_count} modified={result.modified_count}")

    chats.insert_one(doc)
    return {"message": "ok"}


@router.get("/")
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
