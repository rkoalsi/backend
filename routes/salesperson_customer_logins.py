from fastapi import APIRouter, HTTPException, Query, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import get_current_user
from bson.objectid import ObjectId
from passlib.hash import bcrypt
from datetime import datetime
from typing import Optional
import secrets
import string
import re

router = APIRouter()
db = get_database()


def hash_password(password: str) -> str:
    return bcrypt.hash(password)


def generate_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def _build_salesperson_customer_query(user_code: str) -> dict:
    """Build a MongoDB query matching customers assigned to this salesperson."""
    escaped = re.escape(user_code)
    return {
        "$or": [
            {"cf_sales_person": user_code},
            {"cf_sales_person": {"$elemMatch": {"$eq": user_code}}},
            {"cf_sales_person": {"$regex": f"(^\\s*|,\\s*){escaped}(\\s*,|\\s*$)", "$options": "i"}},
        ]
    }


def _assert_salesperson(current_user: dict):
    """Return (user_id_str, user_code) or raise 403."""
    user_data = current_user.get("data", current_user)
    role = user_data.get("role", "")
    if role not in ("sales_person", "sales_admin", "admin"):
        raise HTTPException(status_code=403, detail="Not authorised")
    return str(user_data.get("_id", "")), user_data.get("code", "")


@router.get("/generate-password")
def generate_new_password(current_user: dict = Depends(get_current_user)):
    _assert_salesperson(current_user)
    return {"password": generate_password()}


@router.get("/search/customers")
def search_customers(
    search: str = Query(..., min_length=1),
    current_user: dict = Depends(get_current_user),
):
    """Search customers assigned to this salesperson."""
    user_id, user_code = _assert_salesperson(current_user)

    base_query: dict = {}
    if user_code:
        base_query = _build_salesperson_customer_query(user_code)

    name_filter = {
        "$or": [
            {"contact_name": {"$regex": re.escape(search), "$options": "i"}},
            {"company_name": {"$regex": re.escape(search), "$options": "i"}},
            {"customer_name": {"$regex": re.escape(search), "$options": "i"}},
        ]
    }

    query = {"$and": [base_query, name_filter]} if base_query else name_filter

    customers = db.customers.find(
        query,
        {"_id": 1, "contact_id": 1, "contact_name": 1, "company_name": 1, "email": 1},
    ).limit(20)

    seen = set()
    results = []
    for c in customers:
        cid = c.get("contact_id")
        if cid in seen:
            continue
        seen.add(cid)
        results.append({
            "_id": str(c["_id"]),
            "contact_id": cid,
            "contact_name": c.get("contact_name"),
            "company_name": c.get("company_name"),
            "email": c.get("email"),
            "display_name": c.get("company_name") or c.get("contact_name") or c.get("email", "Unknown"),
        })

    return {"customers": results}


@router.get("")
def get_my_customer_logins(
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    user_id, _ = _assert_salesperson(current_user)

    query: dict = {
        "role": "customer",
        "created_by_salesperson_id": user_id,
    }

    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
        ]

    if status:
        query["status"] = status

    total = db.users.count_documents(query)
    skip = (page - 1) * per_page
    users_cursor = db.users.find(query).sort("created_at", -1).skip(skip).limit(per_page)
    users = serialize_mongo_document(list(users_cursor))
    for u in users:
        u.pop("password", None)

    return {
        "users": users,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.post("")
def create_customer_login(
    user_data: dict,
    current_user: dict = Depends(get_current_user),
):
    user_id, user_code = _assert_salesperson(current_user)

    required = ["email", "password", "name", "phone", "customer_id"]
    missing = [f for f in required if not user_data.get(f)]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")

    if db.users.find_one({"email": user_data["email"]}):
        raise HTTPException(status_code=400, detail="Email already exists")

    # Verify the customer belongs to this salesperson
    customer = db.customers.find_one({"contact_id": user_data["customer_id"]})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    if user_code:
        cf = customer.get("cf_sales_person", "")
        cf_str = ", ".join(cf) if isinstance(cf, list) else (cf or "")
        escaped = re.escape(user_code)
        if not re.search(f"(^|,)\\s*{escaped}\\s*(,|$)", cf_str, re.IGNORECASE):
            raise HTTPException(status_code=403, detail="Customer is not assigned to you")

    try:
        user_data["phone"] = int(user_data["phone"])
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Phone must be a valid number")

    doc = {
        "name": user_data.get("name", ""),
        "first_name": user_data.get("first_name", ""),
        "last_name": user_data.get("last_name", ""),
        "email": user_data["email"],
        "phone": user_data["phone"],
        "role": "customer",
        "status": user_data.get("status", "active"),
        "password": hash_password(user_data["password"]),
        "customer_id": user_data["customer_id"],
        "customer_name": user_data.get("customer_name", ""),
        "created_by_salesperson_id": user_id,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    result = db.users.insert_one(doc)
    return {"message": "Customer login created successfully", "user_id": str(result.inserted_id)}


@router.put("/{user_id}")
def update_customer_login(
    user_id: str,
    user_data: dict,
    current_user: dict = Depends(get_current_user),
):
    sp_id, _ = _assert_salesperson(current_user)

    existing = db.users.find_one({"_id": ObjectId(user_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    if str(existing.get("created_by_salesperson_id", "")) != sp_id:
        raise HTTPException(status_code=403, detail="Not authorised to edit this user")

    user_data.pop("_id", None)
    user_data.pop("role", None)
    user_data.pop("created_by_salesperson_id", None)

    if user_data.get("email") and user_data["email"] != existing.get("email"):
        if db.users.find_one({"email": user_data["email"], "_id": {"$ne": ObjectId(user_id)}}):
            raise HTTPException(status_code=400, detail="Email already exists")

    if "phone" in user_data and user_data["phone"] is not None:
        try:
            user_data["phone"] = int(user_data["phone"])
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Phone must be a valid number")

    if user_data.get("password"):
        user_data["password"] = hash_password(user_data["password"])
    else:
        user_data.pop("password", None)

    user_data["updated_at"] = datetime.utcnow()
    update_data = {k: v for k, v in user_data.items() if v is not None}

    db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})
    return {"message": "Customer login updated successfully"}


@router.delete("/{user_id}")
def delete_customer_login(
    user_id: str,
    current_user: dict = Depends(get_current_user),
):
    sp_id, _ = _assert_salesperson(current_user)

    existing = db.users.find_one({"_id": ObjectId(user_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    if str(existing.get("created_by_salesperson_id", "")) != sp_id:
        raise HTTPException(status_code=403, detail="Not authorised to delete this user")

    db.users.delete_one({"_id": ObjectId(user_id)})
    return {"message": "Customer login deleted successfully"}


@router.post("/{user_id}/reset-password")
def reset_password(
    user_id: str,
    password_data: dict,
    current_user: dict = Depends(get_current_user),
):
    sp_id, _ = _assert_salesperson(current_user)

    if not password_data.get("password"):
        raise HTTPException(status_code=400, detail="Password is required")

    existing = db.users.find_one({"_id": ObjectId(user_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    if str(existing.get("created_by_salesperson_id", "")) != sp_id:
        raise HTTPException(status_code=403, detail="Not authorised")

    db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"password": hash_password(password_data["password"]), "updated_at": datetime.utcnow()}},
    )
    return {"message": "Password reset successfully"}


@router.put("/{user_id}/status")
def update_status(
    user_id: str,
    status_data: dict,
    current_user: dict = Depends(get_current_user),
):
    sp_id, _ = _assert_salesperson(current_user)
    new_status = status_data.get("status")
    if new_status not in ("active", "inactive"):
        raise HTTPException(status_code=400, detail="Status must be 'active' or 'inactive'")

    existing = db.users.find_one({"_id": ObjectId(user_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    if str(existing.get("created_by_salesperson_id", "")) != sp_id:
        raise HTTPException(status_code=403, detail="Not authorised")

    db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"status": new_status, "updated_at": datetime.utcnow()}},
    )
    return {"message": f"Status updated to {new_status}"}
