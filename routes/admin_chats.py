import re
from fastapi import APIRouter, Query
from ..config.root import get_database, serialize_mongo_document

router = APIRouter()

db = get_database()
chats_col = db["chats"]
templates_col = db["templates"]


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
    limit: int = Query(50, le=500),
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
