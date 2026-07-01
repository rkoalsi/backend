from fastapi import APIRouter, HTTPException, Query, File, UploadFile
from fastapi.responses import StreamingResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from passlib.hash import bcrypt
from datetime import datetime
from typing import Optional
import secrets
import string
import re
import openpyxl
from io import BytesIO

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


@router.get("/bulk-upload/template")
def download_bulk_upload_template():
    """Download the XLSX template for bulk customer upload."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Customers"
    headers = ["First Name", "Last Name", "Email", "Whatsapp Phone Number", "Zoho Customer Name"]
    ws.append(headers)

    # Style header row
    from openpyxl.styles import Font, PatternFill, Alignment
    header_fill = PatternFill(start_color="4F46E5", end_color="4F46E5", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    for col_idx, _ in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    # Set column widths
    col_widths = [20, 20, 30, 25, 35]
    for i, width in enumerate(col_widths, start=1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = width

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=customer_upload_template.xlsx"},
    )


@router.post("/bulk-upload/preview")
def preview_bulk_upload(file: UploadFile = File(...)):
    """Parse uploaded XLSX and match Zoho Customer Name against the customers collection."""
    contents = file.file.read()
    try:
        wb = openpyxl.load_workbook(BytesIO(contents))
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read file. Please upload a valid .xlsx file.")

    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")

    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    expected = ["First Name", "Last Name", "Email", "Whatsapp Phone Number", "Zoho Customer Name"]
    if header != expected:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid template headers. Expected: {', '.join(expected)}. Got: {', '.join(header)}"
        )

    found = []
    not_found = []

    for i, row in enumerate(rows[1:], start=2):
        if not any(row):
            continue

        first_name = str(row[0]).strip() if row[0] is not None else ""
        last_name = str(row[1]).strip() if row[1] is not None else ""
        email = str(row[2]).strip() if row[2] is not None else ""
        phone = str(row[3]).strip() if row[3] is not None else ""
        zoho_name = str(row[4]).strip() if row[4] is not None else ""

        entry = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "zoho_customer_name": zoho_name,
            "row": i,
        }

        if not zoho_name:
            not_found.append({**entry, "reason": "No Zoho Customer Name provided"})
            continue

        customer = db.customers.find_one(
            {
                "$or": [
                    {"customer_name": {"$regex": re.escape(zoho_name), "$options": "i"}},
                    {"company_name": {"$regex": re.escape(zoho_name), "$options": "i"}},
                    {"contact_name": {"$regex": re.escape(zoho_name), "$options": "i"}},
                ]
            },
            {"_id": 0, "contact_id": 1, "contact_name": 1, "company_name": 1, "customer_name": 1},
        )

        if customer:
            matched_name = (
                customer.get("company_name")
                or customer.get("contact_name")
                or customer.get("customer_name")
                or zoho_name
            )
            found.append({
                **entry,
                "customer_id": customer.get("contact_id"),
                "matched_customer_name": matched_name,
            })
        else:
            not_found.append({**entry, "reason": "No matching customer found in database"})

    return {"found": found, "not_found": not_found}


@router.post("/bulk-upload/create")
def create_bulk_users(data: dict):
    """Create user accounts for confirmed entries from the bulk upload preview."""
    entries = data.get("entries", [])
    if not entries:
        raise HTTPException(status_code=400, detail="No entries provided.")

    created = []
    errors = []

    for entry in entries:
        email = (entry.get("email") or "").strip()
        first_name = (entry.get("first_name") or "").strip()
        last_name = (entry.get("last_name") or "").strip()
        phone = (entry.get("phone") or "").strip()
        customer_id = entry.get("customer_id") or ""
        matched_customer_name = entry.get("matched_customer_name") or ""

        if not email:
            errors.append({"entry": entry, "reason": "Email is required"})
            continue

        if db.users.find_one({"email": email}):
            errors.append({"entry": entry, "reason": f"Email {email} already exists"})
            continue

        name = f"{first_name} {last_name}".strip() or email

        plain_password = generate_password()
        try:
            phone_int = int(phone) if phone else 0
        except ValueError:
            phone_int = 0

        user_doc = {
            "name": name,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone_int,
            "role": "customer",
            "status": "active",
            "password": hash_password(plain_password),
            "customer_id": customer_id,
            "customer_name": matched_customer_name,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        result = db.users.insert_one(user_doc)
        created.append({
            "user_id": str(result.inserted_id),
            "name": name,
            "email": email,
            "password": plain_password,
            "customer_name": matched_customer_name,
        })

    return {"created": created, "errors": errors, "total_created": len(created)}


@router.get("/by-customer/{contact_id}")
def get_user_by_customer(contact_id: str):
    """Get the user account linked to a Zoho customer by their contact_id."""
    user = db.users.find_one({"customer_id": contact_id, "role": "customer"})
    if not user:
        raise HTTPException(status_code=404, detail="No user account found for this customer")
    user = serialize_mongo_document(user)
    return {"user": {"email": user.get("email"), "name": user.get("name"), "_id": user.get("_id")}}


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

    # Convert phone to integer
    try:
        user_data["phone"] = int(user_data["phone"])
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Phone must be a valid number")

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

    # Convert phone to integer if provided
    if "phone" in user_data and user_data["phone"] is not None:
        try:
            user_data["phone"] = int(user_data["phone"])
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Phone must be a valid number")

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
            "contact_name": {"$regex": search, "$options": "i"},
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

    seen = set()
    results = []
    for customer in customers:
        contact_id = customer.get("contact_id")
        if contact_id in seen:
            continue
        seen.add(contact_id)
        results.append({
            "_id": str(customer.get("_id")),
            "contact_id": contact_id,
            "contact_name": customer.get("contact_name"),
            "company_name": customer.get("company_name"),
            "email": customer.get("email"),
            "display_name": customer.get("company_name") or customer.get("contact_name") or customer.get("email", "Unknown")
        })

    return {"customers": results}
