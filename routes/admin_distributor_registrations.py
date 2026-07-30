from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from bson import ObjectId
from passlib.hash import bcrypt
import secrets
import string
from ..config.root import get_database, serialize_mongo_document
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
db = get_database()
distributor_registrations_collection = db["distributor_registrations"]

STATUSES = ("not_contacted", "contacted", "onboarded", "declined")


class UpdateDistributorRegistrationRequest(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None


class CreateDistributorLoginRequest(BaseModel):
    email: Optional[str] = None
    password: Optional[str] = None


def _generate_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _login_summary(user: dict) -> dict:
    """Shape returned to the admin UI. Never includes the password hash."""
    return {
        "user_id": str(user["_id"]),
        "email": user.get("email", ""),
        "name": user.get("name", ""),
        "status": user.get("status", "active"),
        "created_at": user.get("created_at"),
    }


@router.get("")
def get_distributor_registrations(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    search: Optional[str] = Query(
        None, description="Search by company, brand, contact person, email or phone"
    ),
    status: Optional[str] = Query(None, description="Filter by follow-up status"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    state: Optional[str] = Query(None, description="Filter by distribution state"),
):
    try:
        match_statement: dict = {}
        if search and search.strip():
            term = search.strip()
            match_statement["$or"] = [
                {"company_name": {"$regex": term, "$options": "i"}},
                {"brand_name": {"$regex": term, "$options": "i"}},
                {"contact_person_name": {"$regex": term, "$options": "i"}},
                {"email": {"$regex": term, "$options": "i"}},
                {"phone": {"$regex": term, "$options": "i"}},
            ]
        if status:
            match_statement["status"] = status
        if category:
            match_statement["categories"] = category
        if state:
            match_statement["distribution_states"] = state

        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        total_count = distributor_registrations_collection.count_documents(
            match_statement
        )
        cursor = distributor_registrations_collection.aggregate(pipeline)
        registrations = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "distributor_registrations": registrations,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.patch("/{registration_id}")
def update_distributor_registration(
    registration_id: str,
    body: UpdateDistributorRegistrationRequest,
):
    try:
        update_fields = {}
        if body.status is not None:
            if body.status not in STATUSES:
                raise HTTPException(status_code=400, detail="Invalid status value")
            update_fields["status"] = body.status
        if body.notes is not None:
            update_fields["notes"] = body.notes

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = distributor_registrations_collection.update_one(
            {"_id": ObjectId(registration_id)},
            {"$set": update_fields},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Registration not found")

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Distributor logins
#
# The registration flow captures an application but never creates an account.
# These endpoints are what turn an approved application into a login for the
# distributor portal. A login is deliberately its own step rather than a side
# effect of status="onboarded", so nobody gets portal access by a mis-click on
# a status dropdown.
# ---------------------------------------------------------------------------


@router.get("/{registration_id}/login")
def get_distributor_login(registration_id: str):
    """Whether this registration already has a portal login."""
    if not ObjectId.is_valid(registration_id):
        raise HTTPException(status_code=400, detail="Invalid registration id")

    user = db.users.find_one(
        {"role": "distributor", "distributor_id": ObjectId(registration_id)}
    )
    return {"login": _login_summary(user) if user else None}


@router.post("/{registration_id}/login")
def create_distributor_login(
    registration_id: str, body: CreateDistributorLoginRequest
):
    """Create the portal login for an approved distributor.

    Returns the password in plain text exactly once — it is stored hashed and
    cannot be read back afterwards, so the admin must hand it over now or use
    the reset endpoint later.
    """
    if not ObjectId.is_valid(registration_id):
        raise HTTPException(status_code=400, detail="Invalid registration id")

    registration = distributor_registrations_collection.find_one(
        {"_id": ObjectId(registration_id)}
    )
    if not registration:
        raise HTTPException(status_code=404, detail="Registration not found")

    existing = db.users.find_one(
        {"role": "distributor", "distributor_id": ObjectId(registration_id)}
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail="This distributor already has a login. Reset the password instead.",
        )

    email = (body.email or registration.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(
            status_code=400,
            detail="No email on the application — provide one to create the login",
        )

    # Guard across every role, not just distributors: `users.email` is the
    # login key, so a collision with a customer or staff account would let one
    # person's password authenticate into the other's record.
    if db.users.find_one({"email": email}):
        raise HTTPException(
            status_code=409, detail=f"A user with the email {email} already exists"
        )

    password = body.password or _generate_password()

    doc = {
        "name": registration.get("contact_person_name")
        or registration.get("company_name", ""),
        "email": email,
        "phone": registration.get("phone", ""),
        "role": "distributor",
        "status": "active",
        "password": bcrypt.hash(password),
        # The scoping key. `_distributor_scope` in the portal router reads this
        # off the JWT to decide which brand's rows the caller may see.
        "distributor_id": ObjectId(registration_id),
        "company_name": registration.get("company_name", ""),
        "brand_name": registration.get("brand_name", ""),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    result = db.users.insert_one(doc)

    distributor_registrations_collection.update_one(
        {"_id": ObjectId(registration_id)},
        {"$set": {"login_user_id": result.inserted_id, "login_created_at": datetime.utcnow()}},
    )

    return {
        "success": True,
        "user_id": str(result.inserted_id),
        "email": email,
        # Shown once; not recoverable afterwards.
        "password": password,
    }


@router.post("/{registration_id}/login/reset-password")
def reset_distributor_password(
    registration_id: str, body: CreateDistributorLoginRequest
):
    """Issue a new password. Returned in plain text once, as above."""
    if not ObjectId.is_valid(registration_id):
        raise HTTPException(status_code=400, detail="Invalid registration id")

    user = db.users.find_one(
        {"role": "distributor", "distributor_id": ObjectId(registration_id)}
    )
    if not user:
        raise HTTPException(status_code=404, detail="No login exists for this distributor")

    password = body.password or _generate_password()
    db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"password": bcrypt.hash(password), "updated_at": datetime.utcnow()}},
    )
    return {"success": True, "email": user.get("email", ""), "password": password}


@router.patch("/{registration_id}/login/status")
def set_distributor_login_status(registration_id: str, body: dict):
    """Enable or disable portal access without deleting the account."""
    status_value = (body or {}).get("status")
    if status_value not in ("active", "inactive"):
        raise HTTPException(status_code=400, detail="status must be active or inactive")
    if not ObjectId.is_valid(registration_id):
        raise HTTPException(status_code=400, detail="Invalid registration id")

    result = db.users.update_one(
        {"role": "distributor", "distributor_id": ObjectId(registration_id)},
        {"$set": {"status": status_value, "updated_at": datetime.utcnow()}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="No login exists for this distributor")
    return {"success": True, "status": status_value}
