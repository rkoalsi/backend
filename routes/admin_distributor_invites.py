"""Admin management of distributor registration invites (/admin/distributor_invites).

Distributor onboarding is invite-only: an admin creates an invite here, which
mints an unguessable token, and sends the resulting link to the brand. The link
is the only way to reach the registration form (routes/distributor_registrations.py).

An invite stays live until it is revoked, so a brand can return to the same link
to correct what they submitted; revoking is how an admin closes it off.
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime, timedelta, timezone
from bson import ObjectId
import re
import secrets

from ..config.root import get_database, serialize_mongo_document
from .distributor_registrations import STEP_LABELS, invite_progress

router = APIRouter()
db = get_database()
invites_collection = db["distributor_invites"]

try:
    # The token is the only credential on the registration form, so lookups by
    # it have to be indexed and it can never be issued twice.
    invites_collection.create_index("token", unique=True)
except Exception:
    pass

IST = timezone(timedelta(hours=5, minutes=30))

STATUSES = ("active", "revoked")

# 32 url-safe characters — ~190 bits, so the link cannot be found by guessing.
TOKEN_BYTES = 24


def now_ist():
    return datetime.now(IST)


class InviteRequest(BaseModel):
    """Everything is optional except the brand — the invite exists to collect
    these details, so pre-filling is a convenience, not a requirement."""

    brand_name: str
    company_name: str = ""
    contact_person_name: str = ""
    email: str = ""
    phone: str = ""
    note: str = ""

    @validator("brand_name")
    def brand_required(cls, v):
        v = (v or "").strip()
        if not v:
            raise ValueError("Brand name is required")
        return v

    @validator("company_name", "contact_person_name", "note")
    def trim(cls, v):
        return (v or "").strip()

    @validator("email")
    def validate_email(cls, v):
        v = (v or "").strip()
        if v and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v):
            raise ValueError("Enter a valid email address")
        return v

    @validator("phone")
    def validate_phone(cls, v):
        digits = re.sub(r"\D", "", v or "")
        if not digits:
            return ""
        digits = digits[-10:]
        if len(digits) != 10:
            raise ValueError("Enter a valid 10-digit mobile number")
        return digits


class InviteStatusRequest(BaseModel):
    status: str

    @validator("status")
    def validate_status(cls, v):
        if v not in STATUSES:
            raise ValueError(f"Status must be one of: {', '.join(STATUSES)}")
        return v


def _serialize(invite: dict) -> dict:
    doc = serialize_mongo_document(invite)
    doc["submission_count"] = invite.get("submission_count", 0)
    doc["submitted"] = bool(invite.get("registration_id"))
    doc["progress"] = invite_progress(invite)
    return doc


# Stages an admin actually wants to filter on, mapped to the query that finds
# them. Derived from the same fields as invite_progress, so the filter and the
# column can never disagree.
_STAGE_QUERIES = {
    "not_opened": {"opened_at": None, "registration_id": None},
    "opened": {
        "opened_at": {"$ne": None},
        "phone_verified_at": None,
        "draft_step": None,
        "registration_id": None,
    },
    "verified": {
        "phone_verified_at": {"$ne": None},
        "draft_step": None,
        "registration_id": None,
    },
    "in_progress": {"draft_step": {"$ne": None}, "registration_id": None},
    "submitted": {"registration_id": {"$ne": None}},
}


@router.get("")
def list_invites(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    search: Optional[str] = Query(
        None, description="Search by brand, company, contact person, email or phone"
    ),
    status: Optional[str] = Query(None, description="Filter by invite status"),
    stage: Optional[str] = Query(
        None,
        description=f"Filter by how far the brand got: {', '.join(_STAGE_QUERIES)}",
    ),
):
    try:
        match: dict = {}
        if search and search.strip():
            term = re.escape(search.strip())
            match["$or"] = [
                {"brand_name": {"$regex": term, "$options": "i"}},
                {"company_name": {"$regex": term, "$options": "i"}},
                {"contact_person_name": {"$regex": term, "$options": "i"}},
                {"email": {"$regex": term, "$options": "i"}},
                {"phone": {"$regex": term, "$options": "i"}},
            ]
        if status:
            if status not in STATUSES:
                raise HTTPException(status_code=400, detail="Invalid status value")
            match["status"] = status
        if stage:
            if stage not in _STAGE_QUERIES:
                raise HTTPException(status_code=400, detail="Invalid stage value")
            match.update(_STAGE_QUERIES[stage])

        total_count = invites_collection.count_documents(match)
        cursor = (
            invites_collection.find(match)
            .sort("created_at", -1)
            .skip(page * limit)
            .limit(limit)
        )
        invites = [_serialize(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        return {
            "invites": invites,
            "step_labels": STEP_LABELS,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_invite(body: InviteRequest):
    try:
        doc = {
            "token": secrets.token_urlsafe(TOKEN_BYTES),
            "brand_name": body.brand_name,
            "company_name": body.company_name,
            "contact_person_name": body.contact_person_name,
            "email": body.email,
            "phone": body.phone,
            "note": body.note,
            "status": "active",
            "registration_id": None,
            "submission_count": 0,
            # Set once the agreement PDF upload ships; the form already renders a
            # download link when it is present.
            "agreement_url": "",
            "created_at": now_ist(),
        }
        result = invites_collection.insert_one(doc)
        return {"success": True, "id": str(result.inserted_id), "token": doc["token"]}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{invite_id}")
def update_invite(invite_id: str, body: InviteRequest):
    """Edit the pre-filled details. The token is deliberately untouched so a
    link already sent to the brand keeps working."""
    try:
        result = invites_collection.update_one(
            {"_id": ObjectId(invite_id)},
            {
                "$set": {
                    "brand_name": body.brand_name,
                    "company_name": body.company_name,
                    "contact_person_name": body.contact_person_name,
                    "email": body.email,
                    "phone": body.phone,
                    "note": body.note,
                }
            },
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Invite not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.patch("/{invite_id}/status")
def set_invite_status(invite_id: str, body: InviteStatusRequest):
    """Revoke a link (or put a revoked one back in service)."""
    try:
        update = {"status": body.status}
        update["revoked_at"] = now_ist() if body.status == "revoked" else None
        result = invites_collection.update_one(
            {"_id": ObjectId(invite_id)}, {"$set": update}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Invite not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/{invite_id}/regenerate")
def regenerate_invite_token(invite_id: str):
    """Mint a fresh token, killing the old link — for when an invite has been
    forwarded to the wrong person.

    The draft and the progress timestamps go with it: they describe whoever held
    the old link, and the draft is handed back to anyone opening the new one.
    """
    try:
        token = secrets.token_urlsafe(TOKEN_BYTES)
        result = invites_collection.update_one(
            {"_id": ObjectId(invite_id)},
            {
                "$set": {"token": token, "status": "active", "revoked_at": None},
                "$unset": {
                    "draft": "",
                    "draft_step": "",
                    "draft_updated_at": "",
                    "opened_at": "",
                    "last_opened_at": "",
                    "phone_verified_at": "",
                    "verified_phone": "",
                },
            },
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Invite not found")
        return {"success": True, "token": token}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{invite_id}")
def delete_invite(invite_id: str):
    """Only ever removes the invite. An application that came through it stays
    in /admin/leads — deleting the link must not delete the lead."""
    try:
        oid = ObjectId(invite_id)
        invite = invites_collection.find_one({"_id": oid})
        if not invite:
            raise HTTPException(status_code=404, detail="Invite not found")
        if invite.get("registration_id"):
            raise HTTPException(
                status_code=400,
                detail="This invite has been filled in — revoke it instead of deleting it.",
            )
        invites_collection.delete_one({"_id": oid})
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
