"""
Customer segmentation for WhatsApp campaigns (mounted at /admin/segments).

A segment is a saved *rule*; resolving it returns the concrete list of recipients
(phone + name + context). Rules are built on the existing customer-analytics
machinery so they reuse the same tier / dormancy / billing definitions the rest
of the admin already trusts.

Two sources:
  - "b2b": resolved from `invoices` + `customers` via build_customer_analytics_pipeline
           (tier, dormancy, salesperson, brand, billing thresholds).
  - "b2c": resolved from the `chatbot_customers` registry (consumers who messaged us).

Phase 3 (campaigns) calls resolve_segment_rule() to get the recipient list.
"""
import datetime
from fastapi import APIRouter, Body, HTTPException, Query
from bson import ObjectId

from ..config.root import get_database, serialize_mongo_document
from .admin_customer_analytics import (
    build_customer_analytics_pipeline,
    _get_current_date_info,
    _build_match_and_filters,
)

router = APIRouter()

db = get_database()
segments_col = db["segments"]
customers_col = db["customers"]
products_col = db["products"]
invoices_col = db["invoices"]
chatbot_customers_col = db["chatbot_customers"]

DORMANCY_OPTIONS = {
    "all", "last_month", "last_45_days", "last_2_months", "last_3_months",
    "not_last_month", "not_last_45_days", "not_last_2_months", "not_last_3_months",
}


def _now():
    return datetime.datetime.now()


def _last10(phone) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


# ---------------------------------------------------------------------------
# Brand helper: contact_ids that purchased any of the given brands (since 2023-04)
# ---------------------------------------------------------------------------

def _contact_ids_for_brands(brands: list) -> set:
    if not brands:
        return set()
    # item_id -> brand map, then the set of item_ids belonging to the requested brands.
    wanted = {b.strip() for b in brands if b and b.strip()}
    item_ids = [
        p.get("item_id")
        for p in products_col.find(
            {"brand": {"$in": list(wanted)}}, {"item_id": 1, "_id": 0}
        )
        if p.get("item_id")
    ]
    if not item_ids:
        return set()
    pipeline = [
        {"$match": {"date": {"$gte": "2023-04-01"}, "status": {"$nin": ["void", "draft"]}}},
        {"$unwind": "$line_items"},
        {"$match": {"line_items.item_id": {"$in": item_ids}}},
        {"$group": {"_id": "$customer_id"}},
    ]
    return {r["_id"] for r in invoices_col.aggregate(pipeline, allowDiskUse=True) if r.get("_id")}


# ---------------------------------------------------------------------------
# Core resolver (also imported by the campaigns route in Phase 3)
# ---------------------------------------------------------------------------

def resolve_segment_rule(source: str, rule: dict) -> list:
    """Return a de-duplicated list of recipients for a segment rule.

    Each recipient: {phone, name, customerId?, companyName?, tier?, lastBillDate?,
    salesPerson?, billingCurrentFY?}. Recipients without a usable phone are dropped.
    """
    source = (source or "b2b").lower()
    rule = rule or {}

    if source == "b2c":
        return _resolve_b2c(rule)
    return _resolve_b2b(rule)


def _resolve_b2c(rule: dict) -> list:
    query = {}
    if rule.get("only_non_b2b"):
        query["is_b2b"] = False
    if rule.get("reviewed_only"):
        query["reviewed"] = True
    recipients = []
    seen = set()
    for c in chatbot_customers_col.find(query, {"phone": 1, "name": 1, "is_b2b": 1}):
        phone = c.get("phone")
        tail = _last10(phone)
        if not tail or tail in seen:
            continue
        seen.add(tail)
        recipients.append({
            "phone": phone,
            "name": c.get("name"),
            "source": "b2c",
            "is_b2b": c.get("is_b2b", False),
        })
    return recipients


def _resolve_b2b(rule: dict) -> list:
    tier = rule.get("tier")
    dormancy = rule.get("dormancy") or "all"
    if dormancy not in DORMANCY_OPTIONS:
        raise HTTPException(status_code=400, detail=f"Invalid dormancy; must be one of {sorted(DORMANCY_OPTIONS)}")
    salespersons = {s.strip().lower() for s in (rule.get("salespersons") or []) if s and s.strip()}
    brands = rule.get("brands") or []
    min_billing = rule.get("min_billing_current_fy")
    max_billing = rule.get("max_billing_current_fy")

    match_stage, customer_status_match_stage, _, sales_person_logic = _build_match_and_filters(
        status="all", tier=tier, sort_by=False
    )
    pipeline = build_customer_analytics_pipeline(
        match_stage=match_stage,
        customer_status_match_stage=customer_status_match_stage,
        sales_person_logic=sales_person_logic,
        due_status="all",
        last_billed=dormancy,
        current_date_info=_get_current_date_info(),
        include_all_invoices=False,
    )
    rows = list(invoices_col.aggregate(pipeline, allowDiskUse=True))

    brand_ids = _contact_ids_for_brands(brands) if brands else None

    # Phone lookup for the resolved contact_ids.
    contact_ids = [r.get("customerId") for r in rows if r.get("customerId")]
    phone_map = {}
    for cust in customers_col.find(
        {"contact_id": {"$in": contact_ids}},
        {"contact_id": 1, "phone": 1, "mobile": 1, "first_name": 1, "company_name": 1},
    ):
        phone_map[cust.get("contact_id")] = cust

    recipients = []
    seen = set()
    for r in rows:
        cid = r.get("customerId")
        if brand_ids is not None and cid not in brand_ids:
            continue
        if salespersons:
            sp = (r.get("salesPerson") or "").strip().lower()
            if sp not in salespersons:
                continue
        billing = r.get("billingTillDateCurrentYear") or 0
        if min_billing is not None and billing < min_billing:
            continue
        if max_billing is not None and billing > max_billing:
            continue

        cust = phone_map.get(cid, {})
        phone = cust.get("phone") or cust.get("mobile")
        tail = _last10(phone)
        if not tail or tail in seen:
            continue
        seen.add(tail)
        recipients.append({
            "phone": phone,
            "name": r.get("customerName") or cust.get("first_name"),
            "customerId": cid,
            "companyName": r.get("companyName") or cust.get("company_name"),
            "tier": r.get("tier"),
            "lastBillDate": r.get("lastBillDate"),
            "salesPerson": r.get("salesPerson"),
            "billingCurrentFY": round(billing, 2),
            "source": "b2b",
        })
    return recipients


# ---------------------------------------------------------------------------
# Filter option helpers (for building rules in the UI)
# ---------------------------------------------------------------------------

@router.get("/options")
def get_filter_options():
    """Brands, tiers, salespeople and dormancy options the UI needs to build rules."""
    brands = sorted({
        b for b in products_col.distinct("brand") if b and str(b).strip()
    })
    salespeople = sorted({
        u.get("first_name") or u.get("name")
        for u in db.users.find(
            {"role": "sales_person", "status": "active"},
            {"first_name": 1, "name": 1},
        )
        if (u.get("first_name") or u.get("name"))
    })
    return {
        "brands": brands,
        "tiers": ["A", "B", "C"],
        "salespeople": salespeople,
        "dormancy_options": sorted(DORMANCY_OPTIONS),
        "sources": ["b2b", "b2c"],
    }


# ---------------------------------------------------------------------------
# Resolve (preview, no save) + resolve saved segment
# ---------------------------------------------------------------------------

@router.post("/resolve")
def resolve_preview(payload: dict = Body(...)):
    """Resolve an unsaved rule and return a count + a sample of recipients."""
    source = payload.get("source", "b2b")
    rule = payload.get("rule", {})
    sample_size = int(payload.get("sample_size", 50))
    recipients = resolve_segment_rule(source, rule)
    return {
        "count": len(recipients),
        "sample": recipients[:sample_size],
    }


@router.post("/{segment_id}/resolve")
def resolve_saved(segment_id: str, sample_size: int = Query(50, le=1000)):
    seg = segments_col.find_one({"_id": _oid(segment_id)})
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")
    recipients = resolve_segment_rule(seg.get("source", "b2b"), seg.get("rule", {}))
    # Cache the last resolved count for quick display in lists.
    segments_col.update_one(
        {"_id": seg["_id"]},
        {"$set": {"last_resolved_count": len(recipients), "last_resolved_at": _now()}},
    )
    return {"count": len(recipients), "sample": recipients[:sample_size]}


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@router.get("")
def list_segments(
    search: str = Query(None),
    limit: int = Query(100, le=1000),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if search:
        query["name"] = {"$regex": search.strip(), "$options": "i"}
    raw = list(segments_col.find(query).sort("updated_at", -1).skip(skip).limit(limit))
    total = segments_col.count_documents(query)
    return {"data": serialize_mongo_document(raw), "total": total, "limit": limit, "skip": skip}


@router.get("/{segment_id}")
def get_segment(segment_id: str):
    seg = segments_col.find_one({"_id": _oid(segment_id)})
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")
    return {"data": serialize_mongo_document(seg)}


@router.post("")
def create_segment(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Segment name is required")
    if segments_col.find_one({"name": name}):
        raise HTTPException(status_code=409, detail=f"A segment named '{name}' already exists")
    doc = {
        "name": name,
        "description": payload.get("description", ""),
        "source": (payload.get("source") or "b2b").lower(),
        "rule": payload.get("rule", {}),
        "created_at": _now(),
        "updated_at": _now(),
    }
    result = segments_col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"data": serialize_mongo_document(doc)}


@router.put("/{segment_id}")
def update_segment(segment_id: str, payload: dict = Body(...)):
    seg = segments_col.find_one({"_id": _oid(segment_id)})
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")
    set_fields = {"updated_at": _now()}
    for key in ("name", "description", "source", "rule"):
        if key in payload:
            set_fields[key] = payload[key]
    if "source" in set_fields:
        set_fields["source"] = str(set_fields["source"]).lower()
    segments_col.update_one({"_id": seg["_id"]}, {"$set": set_fields})
    return {"data": serialize_mongo_document(segments_col.find_one({"_id": seg["_id"]}))}


@router.delete("/{segment_id}")
def delete_segment(segment_id: str):
    res = segments_col.delete_one({"_id": _oid(segment_id)})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Segment not found")
    return {"status": "deleted"}


def _oid(segment_id: str) -> ObjectId:
    try:
        return ObjectId(segment_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid segment id")
