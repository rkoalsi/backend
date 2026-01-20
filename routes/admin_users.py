from fastapi import APIRouter, HTTPException, Query
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from passlib.hash import bcrypt
from datetime import datetime
from typing import Optional
import secrets
import string

router = APIRouter()
db = get_database()


def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hash(password)


def generate_password(length: int = 12) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(secrets.choice(alphabet) for _ in range(length))
    return password


@router.get("")
def get_all_users(
    search: Optional[str] = Query(None, description="Search by name or email"),
    role: Optional[str] = Query(None, description="Filter by role"),
    status: Optional[str] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100)
):
    """
    Get all users with optional filtering and pagination.
    """
    query = {}

    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"first_name": {"$regex": search, "$options": "i"}},
            {"last_name": {"$regex": search, "$options": "i"}},
        ]

    if role:
        query["role"] = role

    if status:
        query["status"] = status

    # Get total count for pagination
    total = db.users.count_documents(query)

    # Get paginated users
    skip = (page - 1) * per_page
    users_cursor = db.users.find(query).sort("created_at", -1).skip(skip).limit(per_page)
    users = serialize_mongo_document(list(users_cursor))

    # Remove password field from response
    for user in users:
        user.pop("password", None)

    # Get statistics
    stats = {
        "total": db.users.count_documents({}),
        "active": db.users.count_documents({"status": "active"}),
        "inactive": db.users.count_documents({"status": "inactive"}),
        "by_role": {}
    }

    # Count by role
    roles = ["admin", "sales_admin", "sales_person", "warehouse", "catalogue_manager", "hr", "customer"]
    for r in roles:
        stats["by_role"][r] = db.users.count_documents({"role": r})

    return {
        "users": users,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "stats": stats
    }


@router.get("/roles")
def get_available_roles():
    """Get list of available user roles."""
    return {
        "roles": [
            {"value": "admin", "label": "Admin"},
            {"value": "sales_admin", "label": "Sales Admin"},
            {"value": "sales_person", "label": "Sales Person"},
            {"value": "warehouse", "label": "Warehouse"},
            {"value": "catalogue_manager", "label": "Catalogue Manager"},
            {"value": "hr", "label": "HR"},
            {"value": "customer", "label": "Customer"},
        ]
    }


@router.get("/generate-password")
def generate_new_password():
    """Generate a new random password."""
    return {"password": generate_password()}


@router.get("/{user_id}")
def get_user(user_id: str):
    """Get a single user by ID."""
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user = serialize_mongo_document(user)
    user.pop("password", None)
    return {"user": user}


@router.post("")
def create_user(user_data: dict):
    """
    Create a new user with all required fields.
    Required fields: email, password, name, phone, role, status
    Optional fields: first_name, last_name, code, designation, department, customer_id
    """
    # Validate required fields
    required_fields = ["email", "password", "name", "phone", "role", "status"]
    missing_fields = [f for f in required_fields if not user_data.get(f)]
    if missing_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required fields: {', '.join(missing_fields)}"
        )

    # Check if email already exists
    existing_user = db.users.find_one({"email": user_data["email"]})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already exists")

    # Check if code already exists (if provided)
    if user_data.get("code"):
        existing_code = db.users.find_one({"code": user_data["code"]})
        if existing_code:
            raise HTTPException(status_code=400, detail="User code already exists")

    # Hash the password
    user_data["password"] = hash_password(user_data["password"])

    # Add timestamps
    user_data["created_at"] = datetime.utcnow()
    user_data["updated_at"] = datetime.utcnow()

    # Insert the user
    result = db.users.insert_one(user_data)

    return {
        "message": "User created successfully",
        "user_id": str(result.inserted_id)
    }


@router.put("/{user_id}")
def update_user(user_id: str, user_data: dict):
    """
    Update an existing user.
    If password is provided, it will be hashed before saving.
    """
    # Check if user exists
    existing_user = db.users.find_one({"_id": ObjectId(user_id)})
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Remove _id from update data if present
    user_data.pop("_id", None)

    # If email is being changed, check for duplicates
    if user_data.get("email") and user_data["email"] != existing_user.get("email"):
        email_exists = db.users.find_one({
            "email": user_data["email"],
            "_id": {"$ne": ObjectId(user_id)}
        })
        if email_exists:
            raise HTTPException(status_code=400, detail="Email already exists")

    # If code is being changed, check for duplicates
    if user_data.get("code") and user_data["code"] != existing_user.get("code"):
        code_exists = db.users.find_one({
            "code": user_data["code"],
            "_id": {"$ne": ObjectId(user_id)}
        })
        if code_exists:
            raise HTTPException(status_code=400, detail="User code already exists")

    # Hash password if provided and not empty
    if user_data.get("password"):
        user_data["password"] = hash_password(user_data["password"])
    else:
        user_data.pop("password", None)  # Don't update password if not provided

    # Add updated timestamp
    user_data["updated_at"] = datetime.utcnow()

    # Filter out None values
    update_data = {k: v for k, v in user_data.items() if v is not None}

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields provided for update")

    # Perform the update
    result = db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": "User updated successfully"}


@router.delete("/{user_id}")
def delete_user(user_id: str):
    """Delete a user by ID."""
    result = db.users.delete_one({"_id": ObjectId(user_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "User deleted successfully"}


@router.post("/{user_id}/reset-password")
def reset_user_password(user_id: str, password_data: dict):
    """
    Reset a user's password (admin function).
    Expects: { "password": "new_password" }
    """
    if not password_data.get("password"):
        raise HTTPException(status_code=400, detail="Password is required")

    # Check if user exists
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Hash and update password
    hashed_password = hash_password(password_data["password"])

    result = db.users.update_one(
        {"_id": ObjectId(user_id)},
        {
            "$set": {
                "password": hashed_password,
                "updated_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": "Password reset successfully"}


@router.put("/{user_id}/status")
def update_user_status(user_id: str, status_data: dict):
    """
    Update user status (active/inactive).
    Expects: { "status": "active" | "inactive" }
    """
    new_status = status_data.get("status")
    if new_status not in ["active", "inactive"]:
        raise HTTPException(status_code=400, detail="Status must be 'active' or 'inactive'")

    result = db.users.update_one(
        {"_id": ObjectId(user_id)},
        {
            "$set": {
                "status": new_status,
                "updated_at": datetime.utcnow()
            }
        }
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": f"User status updated to {new_status}"}


@router.get("/search/customers")
def search_customers_for_assignment(
    search: str = Query(..., min_length=1, description="Search term for customer name")
):
    """
    Search customers from the contacts collection for assignment to a user.
    Returns contact_id and contact_name for selection.
    """
    customers = db.customers.find(
        {
            "$or": [
                {"contact_name": {"$regex": search, "$options": "i"}},
                {"company_name": {"$regex": search, "$options": "i"}},
                {"email": {"$regex": search, "$options": "i"}},
            ],
            "status": "active"
        },
        {
            "_id": 1,
            "contact_id": 1,
            "contact_name": 1,
            "company_name": 1,
            "email": 1
        }
    ).limit(20)

    results = []
    for customer in customers:
        results.append({
            "_id": str(customer.get("_id")),
            "contact_id": customer.get("contact_id"),
            "contact_name": customer.get("contact_name"),
            "company_name": customer.get("company_name"),
            "email": customer.get("email"),
            "display_name": customer.get("company_name") or customer.get("contact_name") or customer.get("email", "Unknown")
        })

    return {"customers": results}
