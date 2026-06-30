"""
Admin WhatsApp template management (mounted at /admin/templates).

Wraps Plivo's WhatsApp Template API and keeps a local mirror in the existing
`templates` collection (the same collection the send path reads). Templates the
app submits get `created_via_app: True`; the older, externally-created templates
do not — and those can be edited here but NOT deleted, per product requirement.

Approval status (PENDING -> APPROVED/REJECTED/...) is updated two ways:
  1. On demand via POST /sync (pulls the live list from Plivo).
  2. Live via Meta's status webhook, handled in routes/chats.py (the existing
     /api/chats/callback already stores WABA events; see apply_template_status).
"""
import datetime
from fastapi import APIRouter, Body, HTTPException, Query
from bson import ObjectId

from ..config.root import get_database, serialize_mongo_document
from ..config import plivo_templates as pt

router = APIRouter()

db = get_database()
templates_col = db["templates"]

# Status values Meta/Plivo use; anything created locally starts as PENDING.
_VALID_CATEGORIES = {"MARKETING", "UTILITY", "AUTHENTICATION"}


def _now():
    return datetime.datetime.now()


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

@router.get("")
def list_templates(
    status: str = Query(None, description="Filter by status (APPROVED/PENDING/...)"),
    category: str = Query(None, description="Filter by category"),
    search: str = Query(None, description="Match name (partial, case-insensitive)"),
    limit: int = Query(100, le=1000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if status:
        query["status"] = status.upper()
    if category:
        query["category"] = category.upper()
    if search:
        query["name"] = {"$regex": search.strip(), "$options": "i"}

    raw = list(
        templates_col.find(query).sort("name", 1).skip(skip).limit(limit)
    )
    total = templates_col.count_documents(query)
    return {
        "data": serialize_mongo_document(raw),
        "total": total,
        "limit": limit,
        "skip": skip,
    }


@router.get("/{template_id}")
def get_template(template_id: str):
    doc = templates_col.find_one({"_id": _oid(template_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"data": serialize_mongo_document(doc)}


# ---------------------------------------------------------------------------
# Create  (submit to Meta via Plivo, then mirror locally)
# ---------------------------------------------------------------------------

@router.post("")
def create_template(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip().lower().replace(" ", "_")
    language = (payload.get("language") or "en_US").strip()
    category = (payload.get("category") or "MARKETING").strip().upper()
    body = (payload.get("body") or "").strip()
    header = payload.get("header")          # {"format","text","example"} or None
    footer = (payload.get("footer") or "").strip()
    buttons = payload.get("buttons") or []

    if not name:
        raise HTTPException(status_code=400, detail="Template name is required")
    if not body:
        raise HTTPException(status_code=400, detail="Template body is required")
    if category not in _VALID_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"category must be one of {_VALID_CATEGORIES}")
    if templates_col.find_one({"name": name}):
        raise HTTPException(status_code=409, detail=f"A template named '{name}' already exists")

    # Normalize header: callers may pass a plain string for a TEXT header.
    header_obj = _coerce_header(header)
    components = pt.build_components(
        header=header_obj, body=body, footer=footer, buttons=buttons
    )

    try:
        resp = pt.create_template(name, language, category, components)
    except pt.PlivoTemplateError as e:
        raise HTTPException(status_code=502, detail=str(e))

    doc = {
        "name": name,
        "language": language,
        "category": category,
        # Fields the existing send path uses for local preview/enrichment:
        "header": (header_obj or {}).get("text") if header_obj else None,
        "body": body,
        # New structured fields:
        "header_obj": header_obj,
        "footer": footer,
        "buttons": buttons,
        "components": components,
        "status": (resp.get("status") or "PENDING").upper(),
        "plivo_template_id": resp.get("template_id") or resp.get("id"),
        "created_via_app": True,
        "created_at": _now(),
        "updated_at": _now(),
    }
    result = templates_col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"data": serialize_mongo_document(doc), "plivo_response": resp}


# ---------------------------------------------------------------------------
# Update  (edit content; Meta re-reviews, so status goes back to PENDING)
# ---------------------------------------------------------------------------

@router.put("/{template_id}")
def update_template(template_id: str, payload: dict = Body(...)):
    doc = templates_col.find_one({"_id": _oid(template_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Template not found")

    body = (payload.get("body") or doc.get("body") or "").strip()
    header_obj = _coerce_header(payload.get("header")) if "header" in payload else doc.get("header_obj")
    footer = payload.get("footer", doc.get("footer", "")) or ""
    buttons = payload.get("buttons", doc.get("buttons", [])) or []
    category = (payload.get("category") or doc.get("category") or "MARKETING").upper()

    components = pt.build_components(
        header=header_obj, body=body, footer=footer, buttons=buttons
    )

    set_fields = {
        "body": body,
        "header_obj": header_obj,
        "header": (header_obj or {}).get("text") if header_obj else None,
        "footer": footer,
        "buttons": buttons,
        "category": category,
        "components": components,
        "updated_at": _now(),
    }

    # Only push to Plivo for templates we actually own there. Externally-created
    # ones can still be edited locally (for preview/enrichment) but we don't have
    # a Plivo template id to update against.
    plivo_id = doc.get("plivo_template_id")
    if doc.get("created_via_app") and plivo_id:
        try:
            resp = pt.update_template(plivo_id, components, category=category)
            set_fields["status"] = (resp.get("status") or "PENDING").upper()
        except pt.PlivoTemplateError as e:
            raise HTTPException(status_code=502, detail=str(e))

    templates_col.update_one({"_id": doc["_id"]}, {"$set": set_fields})
    updated = templates_col.find_one({"_id": doc["_id"]})
    return {"data": serialize_mongo_document(updated)}


# ---------------------------------------------------------------------------
# Delete  (only for app-created templates; legacy ones are protected)
# ---------------------------------------------------------------------------

@router.delete("/{template_id}")
def delete_template(template_id: str):
    doc = templates_col.find_one({"_id": _oid(template_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Template not found")

    if not doc.get("created_via_app"):
        raise HTTPException(
            status_code=403,
            detail="This template predates campaign management and cannot be deleted here. "
                   "You can edit it, but deletion is disabled to protect existing flows.",
        )

    plivo_id = doc.get("plivo_template_id")
    if plivo_id:
        try:
            pt.delete_template(plivo_id, doc.get("name"))
        except pt.PlivoTemplateError as e:
            raise HTTPException(status_code=502, detail=str(e))

    templates_col.delete_one({"_id": doc["_id"]})
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Sync approval status from Plivo
# ---------------------------------------------------------------------------

@router.post("/sync")
def sync_templates():
    """Pull the live template list from Plivo and refresh local statuses. Returns
    a summary of how many were updated."""
    try:
        resp = pt.list_templates(limit=1000)
    except pt.PlivoTemplateError as e:
        raise HTTPException(status_code=502, detail=str(e))

    remote = resp.get("templates") or resp.get("objects") or resp.get("data") or []
    updated = 0
    for r in remote:
        name = r.get("name")
        if not name:
            continue
        status = (r.get("status") or "").upper()
        set_fields = {"updated_at": _now()}
        if status:
            set_fields["status"] = status
        if r.get("rejected_reason"):
            set_fields["rejected_reason"] = r.get("rejected_reason")
        if r.get("template_id") or r.get("id"):
            set_fields["plivo_template_id"] = r.get("template_id") or r.get("id")
        res = templates_col.update_one({"name": name}, {"$set": set_fields})
        if res.modified_count:
            updated += 1

    return {"status": "ok", "remote_count": len(remote), "updated": updated}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _oid(template_id: str) -> ObjectId:
    try:
        return ObjectId(template_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid template id")


def _coerce_header(header):
    """Accept either a plain string (TEXT header) or a structured dict."""
    if not header:
        return None
    if isinstance(header, str):
        return {"format": "TEXT", "text": header}
    return header


def apply_template_status(name: str, status: str, rejected_reason: str = None):
    """Called by the WABA status webhook (routes/chats.py) when Meta reports a
    template status transition. Safe no-op if the template isn't mirrored."""
    if not name or not status:
        return
    set_fields = {"status": str(status).upper(), "updated_at": _now()}
    if rejected_reason:
        set_fields["rejected_reason"] = rejected_reason
    templates_col.update_one({"name": name}, {"$set": set_fields})
