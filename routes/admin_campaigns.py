"""
WhatsApp marketing campaigns (mounted at /admin/campaigns).

A campaign ties together: an approved template + an audience (a saved segment or
an inline rule) + a parameter mapping + an optional schedule. Sending resolves the
audience, writes one `campaign_recipients` row per person, and dispatches throttled
WhatsApp template messages in the background. Delivery receipts (handled by the
existing /api/chats/callback) update each recipient's status via apply_campaign_status(),
which is what powers the analytics (Phase 4) endpoints below.

Collections:
  campaigns            -- one per send
  campaign_recipients  -- one per (campaign, recipient); message_uuid is the join key
  message_opt_outs     -- phones that replied STOP; filtered out of every send
"""
import re
import time
import datetime
from fastapi import APIRouter, Body, HTTPException, Query, BackgroundTasks
from bson import ObjectId

from ..config.root import get_database, serialize_mongo_document
from ..config.whatsapp import send_template_message
from .admin_segments import resolve_segment_rule

router = APIRouter()

db = get_database()
campaigns_col = db["campaigns"]
recipients_col = db["campaign_recipients"]
templates_col = db["templates"]
segments_col = db["segments"]
opt_outs_col = db["message_opt_outs"]

# Throttle between sends (seconds) to stay within Plivo/WhatsApp rate limits.
SEND_DELAY_SECONDS = 0.2

# Terminal vs in-flight statuses for stats bucketing.
_DELIVERY_ORDER = ["queued", "sent", "delivered", "read"]


def _now():
    return datetime.datetime.now()


def _last10(phone) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


def _ensure_indexes():
    recipients_col.create_index("campaign_id")
    recipients_col.create_index("message_uuid")
    opt_outs_col.create_index("phone_last10", unique=True)


try:
    _ensure_indexes()
except Exception as e:  # pragma: no cover - index creation is best-effort
    print(f"[campaigns] index setup warning: {e}")


# ---------------------------------------------------------------------------
# Param resolution: turn a mapping of placeholder strings into per-recipient params
# ---------------------------------------------------------------------------

def _resolve_params(mapping: list, recipient: dict) -> dict:
    """Each mapping entry is a string that may contain {field} tokens referencing
    recipient fields (name, companyName, tier, salesPerson, billingCurrentFY...).
    Returns an ordered dict p1..pn matching the template's {{1}}..{{n}}."""
    def sub(token_str: str) -> str:
        def repl(m):
            field = m.group(1)
            val = recipient.get(field)
            return "" if val is None else str(val)
        return re.sub(r"\{(\w+)\}", repl, str(token_str or ""))

    params = {}
    for i, entry in enumerate(mapping or [], start=1):
        params[f"p{i}"] = sub(entry)
    return params


# ---------------------------------------------------------------------------
# Recipient resolution for a campaign (segment or inline rule), minus opt-outs
# ---------------------------------------------------------------------------

def _opt_out_tails() -> set:
    return {d["phone_last10"] for d in opt_outs_col.find({}, {"phone_last10": 1})}


def _resolve_campaign_audience(campaign: dict) -> list:
    segment_id = campaign.get("segment_id")
    if segment_id:
        seg = segments_col.find_one({"_id": ObjectId(segment_id)})
        if not seg:
            raise HTTPException(status_code=400, detail="Campaign's segment no longer exists")
        source, rule = seg.get("source", "b2b"), seg.get("rule", {})
    else:
        source = campaign.get("segment_source", "b2b")
        rule = campaign.get("segment_rule", {})

    recipients = resolve_segment_rule(source, rule)
    blocked = _opt_out_tails()
    return [r for r in recipients if _last10(r.get("phone")) not in blocked]


# ---------------------------------------------------------------------------
# The background send worker
# ---------------------------------------------------------------------------

def _run_campaign_send(campaign_id: str):
    """Resolve the audience, persist recipient rows, and dispatch throttled sends.
    Runs in a BackgroundTask (after the HTTP response has been returned)."""
    oid = ObjectId(campaign_id)
    campaign = campaigns_col.find_one({"_id": oid})
    if not campaign:
        return

    template = templates_col.find_one({"name": campaign.get("template_name")})
    if not template:
        campaigns_col.update_one({"_id": oid}, {"$set": {
            "status": "failed", "error": "Template not found", "completed_at": _now()}})
        return

    template_doc = {"name": template.get("name"), "language": template.get("language")}
    mapping = campaign.get("params_mapping", [])
    button_url = campaign.get("button_url")

    try:
        audience = _resolve_campaign_audience(campaign)
    except HTTPException as e:
        campaigns_col.update_one({"_id": oid}, {"$set": {
            "status": "failed", "error": e.detail, "completed_at": _now()}})
        return

    # Fresh start: clear any prior recipient rows for this campaign (e.g. a retry).
    recipients_col.delete_many({"campaign_id": campaign_id})

    campaigns_col.update_one({"_id": oid}, {"$set": {
        "status": "sending",
        "total_recipients": len(audience),
        "started_at": _now(),
        "error": None,
    }})

    sent, failed = 0, 0
    for r in audience:
        params = _resolve_params(mapping, r)
        if button_url:
            params["button_url"] = re.sub(
                r"\{(\w+)\}",
                lambda m: str(r.get(m.group(1), "")),
                button_url,
            )
        result = send_template_message(
            r.get("phone"), template_doc, params, campaign_id=campaign_id
        )
        recipients_col.insert_one({
            "campaign_id": campaign_id,
            "phone": result.get("dst") or r.get("phone"),
            "name": r.get("name"),
            "customerId": r.get("customerId"),
            "params": params,
            "message_uuid": result.get("message_uuid"),
            "status": result.get("status"),
            "error": result.get("error"),
            "sent_at": _now(),
            "created_at": _now(),
        })
        if result.get("status") in ("failed",):
            failed += 1
        else:
            sent += 1
        time.sleep(SEND_DELAY_SECONDS)

    campaigns_col.update_one({"_id": oid}, {"$set": {
        "status": "completed",
        "completed_at": _now(),
        "dispatched": sent,
        "dispatch_failed": failed,
    }})


# ---------------------------------------------------------------------------
# Callback hook (called from routes/chats.py when a delivery receipt arrives)
# ---------------------------------------------------------------------------

def apply_campaign_status(message_uuid: str, status: str):
    """Update a campaign recipient's status from a Plivo delivery receipt. Stamps
    the matching timestamp and never downgrades a more-advanced status."""
    if not message_uuid or not status:
        return
    status = status.lower()
    rec = recipients_col.find_one({"message_uuid": message_uuid}, {"status": 1})
    if not rec:
        return

    set_fields = {"status": status, "last_callback_at": _now()}
    if status == "delivered":
        set_fields["delivered_at"] = _now()
    elif status == "read":
        set_fields["read_at"] = _now()
    elif status in ("failed", "undelivered"):
        set_fields["status"] = "failed"
        set_fields["failed_at"] = _now()

    # Don't let a late 'sent' overwrite a 'read' already recorded.
    current = (rec.get("status") or "").lower()
    if current in _DELIVERY_ORDER and status in _DELIVERY_ORDER:
        if _DELIVERY_ORDER.index(status) < _DELIVERY_ORDER.index(current):
            set_fields.pop("status", None)

    recipients_col.update_one({"message_uuid": message_uuid}, {"$set": set_fields})


# ---------------------------------------------------------------------------
# Stats helper (used by list + detail + analytics)
# ---------------------------------------------------------------------------

def _stats_for(campaign_ids: list) -> dict:
    """Return {campaign_id: {status: count, total: n}} via one aggregation."""
    pipeline = [
        {"$match": {"campaign_id": {"$in": campaign_ids}}},
        {"$group": {"_id": {"c": "$campaign_id", "s": "$status"}, "n": {"$sum": 1}}},
    ]
    out: dict = {cid: {"total": 0} for cid in campaign_ids}
    for row in recipients_col.aggregate(pipeline):
        cid = row["_id"]["c"]
        status = row["_id"]["s"] or "unknown"
        out.setdefault(cid, {"total": 0})
        out[cid][status] = row["n"]
        out[cid]["total"] += row["n"]
    return out


# ---------------------------------------------------------------------------
# CRUD + lifecycle
# ---------------------------------------------------------------------------

@router.get("")
def list_campaigns(
    status: str = Query(None),
    search: str = Query(None),
    limit: int = Query(100, le=1000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if status:
        query["status"] = status
    if search:
        query["name"] = {"$regex": search.strip(), "$options": "i"}
    raw = list(campaigns_col.find(query).sort("created_at", -1).skip(skip).limit(limit))
    total = campaigns_col.count_documents(query)
    stats = _stats_for([str(c["_id"]) for c in raw])
    data = serialize_mongo_document(raw)
    for c in data:
        c["stats"] = stats.get(c["_id"], {"total": 0})
    return {"data": data, "total": total, "limit": limit, "skip": skip}


@router.get("/{campaign_id}")
def get_campaign(campaign_id: str):
    c = campaigns_col.find_one({"_id": _oid(campaign_id)})
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    data = serialize_mongo_document(c)
    data["stats"] = _stats_for([campaign_id]).get(campaign_id, {"total": 0})
    return {"data": data}


@router.post("")
def create_campaign(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    template_name = (payload.get("template_name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Campaign name is required")
    if not template_name:
        raise HTTPException(status_code=400, detail="A template must be selected")

    template = templates_col.find_one({"name": template_name})
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if (template.get("status") or "").upper() != "APPROVED":
        raise HTTPException(
            status_code=400,
            detail=f"Template '{template_name}' is not APPROVED (status: {template.get('status')}).",
        )

    if not payload.get("segment_id") and not payload.get("segment_rule"):
        raise HTTPException(status_code=400, detail="A segment_id or an inline segment_rule is required")

    scheduled_at = payload.get("scheduled_at")
    parsed_schedule = None
    if scheduled_at:
        try:
            parsed_schedule = datetime.datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="scheduled_at must be ISO 8601")

    doc = {
        "name": name,
        "template_name": template_name,
        "template_language": template.get("language"),
        "segment_id": payload.get("segment_id"),
        "segment_source": payload.get("segment_source", "b2b"),
        "segment_rule": payload.get("segment_rule", {}),
        "params_mapping": payload.get("params_mapping", []),
        "button_url": payload.get("button_url"),
        "status": "scheduled" if parsed_schedule else "draft",
        "scheduled_at": parsed_schedule,
        "created_by": payload.get("created_by"),
        "created_at": _now(),
        "updated_at": _now(),
    }
    result = campaigns_col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"data": serialize_mongo_document(doc)}


@router.put("/{campaign_id}")
def update_campaign(campaign_id: str, payload: dict = Body(...)):
    c = campaigns_col.find_one({"_id": _oid(campaign_id)})
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.get("status") in ("sending", "completed"):
        raise HTTPException(status_code=409, detail="Cannot edit a campaign that is sending or completed")

    set_fields = {"updated_at": _now()}
    for key in ("name", "template_name", "segment_id", "segment_source",
                "segment_rule", "params_mapping", "button_url"):
        if key in payload:
            set_fields[key] = payload[key]
    if "scheduled_at" in payload:
        sa = payload["scheduled_at"]
        if sa:
            try:
                set_fields["scheduled_at"] = datetime.datetime.fromisoformat(sa.replace("Z", "+00:00"))
                set_fields["status"] = "scheduled"
            except Exception:
                raise HTTPException(status_code=400, detail="scheduled_at must be ISO 8601")
        else:
            set_fields["scheduled_at"] = None
            set_fields["status"] = "draft"
    campaigns_col.update_one({"_id": c["_id"]}, {"$set": set_fields})
    return {"data": serialize_mongo_document(campaigns_col.find_one({"_id": c["_id"]}))}


@router.delete("/{campaign_id}")
def delete_campaign(campaign_id: str):
    c = campaigns_col.find_one({"_id": _oid(campaign_id)})
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.get("status") == "sending":
        raise HTTPException(status_code=409, detail="Cannot delete a campaign while it is sending")
    recipients_col.delete_many({"campaign_id": campaign_id})
    campaigns_col.delete_one({"_id": c["_id"]})
    return {"status": "deleted"}


@router.post("/{campaign_id}/send")
def send_campaign(campaign_id: str, background_tasks: BackgroundTasks):
    """Dispatch a campaign now (in the background). Returns immediately."""
    c = campaigns_col.find_one({"_id": _oid(campaign_id)})
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.get("status") == "sending":
        raise HTTPException(status_code=409, detail="Campaign is already sending")
    background_tasks.add_task(_run_campaign_send, campaign_id)
    campaigns_col.update_one({"_id": c["_id"]}, {"$set": {"status": "sending", "started_at": _now()}})
    return {"status": "dispatching"}


@router.post("/process_scheduled")
def process_scheduled(background_tasks: BackgroundTasks):
    """Dispatch any scheduled campaigns whose time has come. Idempotent; intended
    to be hit by an external cron (the in-app APScheduler is disabled)."""
    now = _now()
    due = list(campaigns_col.find({"status": "scheduled", "scheduled_at": {"$lte": now}}))
    for c in due:
        cid = str(c["_id"])
        campaigns_col.update_one({"_id": c["_id"]}, {"$set": {"status": "sending", "started_at": now}})
        background_tasks.add_task(_run_campaign_send, cid)
    return {"dispatched": len(due), "campaign_ids": [str(c["_id"]) for c in due]}


# ---------------------------------------------------------------------------
# Analytics (Phase 4)
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/recipients")
def list_recipients(
    campaign_id: str,
    status: str = Query(None, description="Filter by recipient status"),
    limit: int = Query(100, le=5000),
    skip: int = Query(0, ge=0),
):
    query: dict = {"campaign_id": campaign_id}
    if status:
        query["status"] = status
    raw = list(recipients_col.find(query).sort("sent_at", -1).skip(skip).limit(limit))
    total = recipients_col.count_documents(query)
    return {"data": serialize_mongo_document(raw), "total": total, "limit": limit, "skip": skip}


@router.get("/analytics/overview")
def analytics_overview():
    """Cross-campaign rollup for the analytics dashboard: totals by status, plus
    the most recent campaigns with their per-status counts."""
    by_status = {}
    for row in recipients_col.aggregate([
        {"$group": {"_id": "$status", "n": {"$sum": 1}}}
    ]):
        by_status[row["_id"] or "unknown"] = row["n"]

    recent = list(campaigns_col.find().sort("created_at", -1).limit(20))
    stats = _stats_for([str(c["_id"]) for c in recent])
    recent_data = serialize_mongo_document(recent)
    for c in recent_data:
        c["stats"] = stats.get(c["_id"], {"total": 0})

    total_messages = sum(by_status.values())
    delivered = by_status.get("delivered", 0) + by_status.get("read", 0)
    read = by_status.get("read", 0)
    failed = by_status.get("failed", 0)
    return {
        "totals": {
            "messages": total_messages,
            "by_status": by_status,
            "delivered_rate": round(delivered / total_messages, 4) if total_messages else 0,
            "read_rate": round(read / total_messages, 4) if total_messages else 0,
            "failed_rate": round(failed / total_messages, 4) if total_messages else 0,
        },
        "campaign_count": campaigns_col.count_documents({}),
        "recent_campaigns": recent_data,
    }


# ---------------------------------------------------------------------------
# Opt-outs
# ---------------------------------------------------------------------------

@router.get("/opt_outs/list")
def list_opt_outs(limit: int = Query(500, le=10000), skip: int = Query(0, ge=0)):
    raw = list(opt_outs_col.find().sort("created_at", -1).skip(skip).limit(limit))
    total = opt_outs_col.count_documents({})
    return {"data": serialize_mongo_document(raw), "total": total}


@router.post("/opt_outs")
def add_opt_out(payload: dict = Body(...)):
    phone = (payload.get("phone") or "").strip()
    tail = _last10(phone)
    if not tail:
        raise HTTPException(status_code=400, detail="A valid phone is required")
    opt_outs_col.update_one(
        {"phone_last10": tail},
        {"$set": {"phone": phone, "phone_last10": tail, "source": payload.get("source", "manual")},
         "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )
    return {"status": "opted_out", "phone_last10": tail}


@router.delete("/opt_outs/{tail}")
def remove_opt_out(tail: str):
    opt_outs_col.delete_one({"phone_last10": _last10(tail)})
    return {"status": "removed"}


def record_opt_out(phone: str, source: str = "inbound_stop"):
    """Called from the inbound handler when a user replies STOP/UNSUBSCRIBE."""
    tail = _last10(phone)
    if not tail:
        return
    opt_outs_col.update_one(
        {"phone_last10": tail},
        {"$set": {"phone": phone, "phone_last10": tail, "source": source},
         "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )


def _oid(campaign_id: str) -> ObjectId:
    try:
        return ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign id")
