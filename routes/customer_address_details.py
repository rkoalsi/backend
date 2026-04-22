from fastapi import APIRouter, HTTPException, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from datetime import datetime
from difflib import SequenceMatcher
import re
router = APIRouter()
db = get_database()
collection = db["customer_address_details"]


def _norm_city(city: str) -> str:
    c = (city or "").lower().strip().rstrip(",. ")
    if re.match(r"^(bangalore|bengaluru)$", c):
        return "bengaluru"
    if re.match(r"^(bombay|mumbai)$", c):
        return "mumbai"
    return c


def _norm_state(s: str) -> str:
    s = (s or "").lower().strip().rstrip(",. ")
    s = re.sub(r"\s*-\s*\d[\d\s]*$", "", s).strip()
    return s


def _norm_zip(z: str) -> str:
    return (z or "").replace(" ", "")


def _norm_street(line1: str, line2: str = "") -> str:
    s = ((line1 or "") + " " + (line2 or "")).lower()
    s = s.replace(",", " ").replace(".", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()

try:
    collection.create_index([("customer_id", 1), ("address_id", 1)], unique=True)
except Exception:
    pass

VALID_STATUSES = {"open", "closed", "warehouse"}


@router.get("/{customer_id}/billed")
def get_billed_addresses(customer_id: str):
    """
    Return a dict {address_id: bool} for each of the customer's addresses
    indicating whether that address appears on at least one invoice.
    Matches by shipping_address.address_id when available, then falls back
    to city + state + zip normalization (same logic used in customer analytics).
    """
    customer = db["customers"].find_one({"_id": __import__("bson").ObjectId(customer_id)})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    contact_id = customer.get("contact_id", "")
    addresses = customer.get("addresses", [])
    if not addresses or not contact_id:
        return {"billed": {}}

    invoices = list(
        db["invoices"].find(
            {"customer_id": contact_id},
            {"shipping_address": 1, "_id": 0},
        )
    )

    # Build lookup from customer addresses: (nc, ns, nz) -> address_id
    czs_lookup: dict = {}  # (nc, ns, nz) -> address_id
    for addr in addresses:
        aid = addr.get("address_id", "")
        if not aid:
            continue
        nc = _norm_city(addr.get("city", ""))
        ns = _norm_state(addr.get("state", ""))
        nz = _norm_zip(addr.get("zip", ""))
        czs_lookup[(nc, ns, nz)] = aid

    billed_ids: set = set()
    for inv in invoices:
        sa = inv.get("shipping_address") or {}
        # Direct address_id match
        aid = sa.get("address_id", "")
        if aid:
            billed_ids.add(aid)
            continue
        # Fallback: city + state + zip
        nc = _norm_city(sa.get("city", ""))
        ns = _norm_state(sa.get("state", ""))
        nz = _norm_zip(sa.get("zip", ""))
        matched = czs_lookup.get((nc, ns, nz))
        if matched:
            billed_ids.add(matched)

    result = {addr["address_id"]: addr["address_id"] in billed_ids for addr in addresses if addr.get("address_id")}
    return {"billed": result}


@router.get("/{customer_id}/in_analytics")
def get_in_analytics_addresses(customer_id: str):
    """
    Return a dict {address_id: bool} indicating whether each of the customer's
    addresses appears as a shipping_address on at least one invoice.
    Uses the same city+state+zip+street matching logic (with fuzzy fallbacks)
    as the customer analytics report, so results match what appears in analytics.
    """
    from bson import ObjectId
    customer = db["customers"].find_one({"_id": ObjectId(customer_id)})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    contact_id = customer.get("contact_id", "")
    addresses = customer.get("addresses", [])
    if not addresses or not contact_id:
        return {"in_analytics": {}}

    invoices = list(
        db["invoices"].find(
            {"customer_id": contact_id},
            {"shipping_address": 1, "_id": 0},
        )
    )

    # Build matching lookup tables from the customer's known addresses.
    # Zoho contact addresses use "address" for street line 1; invoices use "street".
    addr_match_lookup: dict = {}    # (nc, ns, nz, nst) -> aid  (full match)
    addr_match_czs: dict = {}       # (nc, ns, nz)       -> [aid, ...]
    addr_match_cs: dict = {}        # (nc, ns)            -> [aid, ...]
    addr_match_cst: dict = {}       # (nc, nst)           -> [aid, ...]
    addr_match_no_city: dict = {}   # (ns, nz, nst)       -> aid
    addr_match_no_city_nz: dict = {}  # (ns, nst)         -> aid
    aid_to_nst: dict = {}           # aid -> nst

    for addr in addresses:
        aid = addr.get("address_id", "")
        if not aid:
            continue
        line1 = addr.get("address", "") or addr.get("street", "")
        nc = _norm_city(addr.get("city", ""))
        ns = _norm_state(addr.get("state", ""))
        nz = _norm_zip(addr.get("zip", ""))
        nst = _norm_street(line1, addr.get("street2", ""))
        addr_match_lookup[(nc, ns, nz, nst)] = aid
        for bucket, key in [
            (addr_match_czs, (nc, ns, nz)),
            (addr_match_cs, (nc, ns)),
            (addr_match_cst, (nc, nst)),
        ]:
            bucket.setdefault(key, [])
            if aid not in bucket[key]:
                bucket[key].append(aid)
        if not nc:
            addr_match_no_city[(ns, nz, nst)] = aid
            addr_match_no_city_nz[(ns, nst)] = aid
        aid_to_nst[aid] = nst

    def _resolve(sa: dict) -> str:
        nc = _norm_city(sa.get("city", ""))
        ns = _norm_state(sa.get("state", ""))
        nz = _norm_zip(sa.get("zip", ""))
        # invoices use "street" for line 1
        nst = _norm_street(sa.get("street", ""), sa.get("street2", ""))

        aid = addr_match_lookup.get((nc, ns, nz, nst), "")
        if not aid:
            candidates = addr_match_czs.get((nc, ns, nz), [])
            if len(candidates) == 1:
                aid = candidates[0]
        if not aid and nst:
            candidates = addr_match_cst.get((nc, nst), [])
            if len(candidates) == 1:
                aid = candidates[0]
        if not aid:
            candidates = addr_match_cs.get((nc, ns), [])
            if len(candidates) == 1:
                aid = candidates[0]
        if not aid and nc:
            street_with_city = nst + " " + nc
            aid = addr_match_no_city.get((ns, nz, street_with_city), "")
            if not aid:
                aid = addr_match_no_city_nz.get((ns, street_with_city), "")
        if not aid and nst:
            candidates = addr_match_czs.get((nc, ns, nz), [])
            if candidates:
                best_aid, best_ratio, second_ratio = "", 0.0, 0.0
                for cand_aid in candidates:
                    cand_nst = aid_to_nst.get(cand_aid, "")
                    ratio = SequenceMatcher(None, nst, cand_nst).ratio()
                    if ratio > best_ratio:
                        second_ratio = best_ratio
                        best_ratio, best_aid = ratio, cand_aid
                    elif ratio > second_ratio:
                        second_ratio = ratio
                if best_ratio >= 0.4 and (best_ratio - second_ratio) >= 0.2:
                    aid = best_aid
        return aid

    in_analytics_ids: set = set()
    for inv in invoices:
        sa = inv.get("shipping_address") or {}
        if not sa:
            continue
        aid = _resolve(sa)
        if aid:
            in_analytics_ids.add(aid)

    result = {
        addr["address_id"]: addr["address_id"] in in_analytics_ids
        for addr in addresses
        if addr.get("address_id")
    }
    return {"in_analytics": result}


@router.get("/{customer_id}")
def get_address_details(customer_id: str):
    """Return all address detail records for a given customer."""
    docs = list(collection.find({"customer_id": customer_id}))
    return {"address_details": [serialize_mongo_document(d) for d in docs]}


@router.put("/{customer_id}/{address_id}", dependencies=[Depends(JWTBearer())])
def upsert_address_detail(customer_id: str, address_id: str, payload: dict):
    """
    Create or update the extra metadata for a specific customer address.
    Accepted payload fields: status, notes (and any future fields).
    """
    status = payload.get("status")
    if status and status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{status}'. Must be one of: {', '.join(sorted(VALID_STATUSES))}",
        )

    update_fields: dict = {"updated_at": datetime.utcnow()}
    if status is not None:
        update_fields["status"] = status
    if "notes" in payload:
        update_fields["notes"] = payload["notes"]
    # Support arbitrary future fields passed in the payload
    reserved = {"status", "notes", "customer_id", "address_id"}
    for key, value in payload.items():
        if key not in reserved:
            update_fields[key] = value

    result = collection.find_one_and_update(
        {"customer_id": customer_id, "address_id": address_id},
        {
            "$set": update_fields,
            "$setOnInsert": {
                "customer_id": customer_id,
                "address_id": address_id,
                "created_at": datetime.utcnow(),
            },
        },
        upsert=True,
        return_document=True,
    )
    return {"address_detail": serialize_mongo_document(result)}


@router.delete("/{customer_id}/{address_id}", dependencies=[Depends(JWTBearer())])
def delete_address_detail(customer_id: str, address_id: str):
    """Remove the extra metadata for a specific customer address."""
    result = collection.delete_one(
        {"customer_id": customer_id, "address_id": address_id}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Address detail not found")
    return {"status": "deleted"}
