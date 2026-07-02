from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks, File, UploadFile
from bson import ObjectId
import boto3, datetime, os, uuid, requests as req_lib
from ..config.root import get_database, get_client, serialize_mongo_document
from ..config.auth import get_current_user
from .notifications import create_notification

router = APIRouter()
db = get_database()

_s3 = boto3.client(
    "s3",
    region_name=os.getenv("S3_REGION", "ap-south-1"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)
_S3_BUCKET = os.getenv("S3_BUCKET_NAME")
_S3_URL = os.getenv("S3_URL")

_client = get_client()
attendance_db = _client.get_database("attendance")
employees_collection = attendance_db.get_collection("employees")

ACTIVE_STATUSES = ["Pending Review", "Pending Second Review", "Pending Payment", "Draft"]

APPROVER_CHAIN = [
    {
        "email": "events@barkbutler.in",
        "stage": "Pending Review",
        "next_status": "Pending Second Review",
        "label": "Rahul",
    },
    {
        "email": "barksalesamit@gmail.com",
        "stage": "Pending Second Review",
        "next_status": "Pending Payment",
        "label": "Amit",
    },
    {
        "email": "barkbutleracs01@gmail.com",
        "stage": "Pending Payment",
        "next_status": "Draft",
        "label": "Yogesh",
    },
]

RESEND_FROM = "no-reply@no-reply.pupscribe.in"
RESEND_URL = "https://api.resend.com/emails"


def _send_email(to_email: str, subject: str, html: str):
    try:
        r = req_lib.post(
            RESEND_URL,
            headers={
                "Authorization": f"Bearer {os.getenv('RESEND_API_KEY')}",
                "Content-Type": "application/json",
            },
            json={"from": RESEND_FROM, "to": [to_email], "subject": subject, "html": html},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"[expense email] failed to {to_email}: {e}")


def _notify_user_by_email(user_email: str, subject: str, html: str):
    _send_email(user_email, subject, html)


def _get_user_by_email(email: str):
    return db.users.find_one({"email": email})


def _notify_approver(approver_email: str, estimate: dict, subject: str, body_html: str):
    user = _get_user_by_email(approver_email)
    if user:
        create_notification(
            db,
            str(user["_id"]),
            "expense_submitted",
            subject,
            f"Expense estimate from {estimate.get('created_by_name')} for trip on {estimate.get('travel_start_date', '')[:10] if isinstance(estimate.get('travel_start_date'), str) else ''}",
            f"/admin/expense_estimates",
        )
    _send_email(approver_email, subject, body_html)


def _notify_salesperson(estimate: dict, notif_type: str, title: str, body: str):
    creator_id = str(estimate.get("created_by"))
    create_notification(
        db,
        creator_id,
        notif_type,
        title,
        body,
        "/expenses",
    )
    creator = db.users.find_one({"_id": estimate.get("created_by")})
    if creator and creator.get("email"):
        _send_email(creator["email"], title, f"<p>{body}</p>")


# ── helpers ────────────────────────────────────────────────────────────────────

def _current_user_id(current_user: dict) -> str:
    return current_user.get("data", {}).get("_id") or current_user.get("_id")


def _get_estimate_or_404(estimate_id: str) -> dict:
    est = db.expense_estimates.find_one({"_id": ObjectId(estimate_id)})
    if not est:
        raise HTTPException(status_code=404, detail="Expense estimate not found")
    return est


def _compute_totals(expense_items: list) -> dict:
    travel = sum(float(i.get("amount") or 0) for i in expense_items if i.get("expense_type") == "Travel")
    stay = sum(float(i.get("amount") or 0) for i in expense_items if i.get("expense_type") == "Stay")
    da = sum(float(i.get("daily_allowance") or 0) for i in expense_items)
    total = travel + stay + da
    return {"estimated_travel": travel, "estimated_stay": stay, "estimated_da": da, "estimated_total": total}


def _compute_actual_totals(expense_items: list) -> dict:
    travel = sum(float(i.get("amount") or 0) for i in expense_items if i.get("expense_type") == "Travel")
    stay = sum(float(i.get("amount") or 0) for i in expense_items if i.get("expense_type") == "Stay")
    da = sum(float(i.get("daily_allowance") or 0) for i in expense_items)
    total = travel + stay + da
    return {"actual_travel": travel, "actual_stay": stay, "actual_da": da, "actual_total": total}


# ── endpoints ──────────────────────────────────────────────────────────────────

@router.post("/upload-bill")
async def upload_bill(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    allowed = {"image/jpeg", "image/png", "image/jpg", "application/pdf"}
    if file.content_type not in allowed:
        raise HTTPException(status_code=400, detail="Only JPEG, PNG, and PDF files are allowed")
    ext = file.filename.rsplit(".", 1)[-1].lower() if file.filename and "." in file.filename else "bin"
    key = f"expense-bills/{uuid.uuid4()}.{ext}"
    try:
        _s3.upload_fileobj(file.file, _S3_BUCKET, key, ExtraArgs={"ContentType": file.content_type, "ACL": "public-read"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
    return {"url": f"{_S3_URL}/{key}"}


@router.get("")
def list_estimates(
    page: int = 0,
    limit: int = 10,
    status: str = None,
    current_user: dict = Depends(get_current_user),
):
    user_id = _current_user_id(current_user)
    query = {"created_by": ObjectId(user_id)}
    if status:
        query["status"] = status
    total = db.expense_estimates.count_documents(query)
    docs = list(
        db.expense_estimates.find(query)
        .sort("created_at", -1)
        .skip(page * limit)
        .limit(limit)
    )
    return {
        "estimates": serialize_mongo_document(docs),
        "total_count": total,
        "total_pages": max(1, -(-total // limit)),
    }


@router.get("/employee-info")
def get_employee_info(current_user: dict = Depends(get_current_user)):
    """Return read-only employee fields to pre-fill Step 1 of the estimate form."""
    user_id = _current_user_id(current_user)
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    email = user.get("email", "")
    employee = employees_collection.find_one({"email": email}, {"_id": 0}) or {}

    return {
        "name": user.get("name", ""),
        "employee_number": employee.get("employee_number", "") or user.get("employee_number", "") or user.get("employee_id", ""),
        "designation": employee.get("designation") or user.get("designation") or "",
        "department": employee.get("department") or user.get("department") or "Sales",
        "reporting_manager": user.get("reporting_manager", ""),
        "current_location": user.get("current_location", ""),
    }


@router.get("/active")
def get_active_estimate(current_user: dict = Depends(get_current_user)):
    """Return the single active (blocking) estimate for this salesperson, if any."""
    user_id = _current_user_id(current_user)
    doc = db.expense_estimates.find_one({
        "created_by": ObjectId(user_id),
        "status": {"$in": ACTIVE_STATUSES},
    })
    return serialize_mongo_document(doc) if doc else None


@router.post("")
async def create_estimate(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
):
    user_id = _current_user_id(current_user)
    user = db.users.find_one({"_id": ObjectId(user_id)})

    # Block if active estimate exists
    existing = db.expense_estimates.find_one({
        "created_by": ObjectId(user_id),
        "status": {"$in": ACTIVE_STATUSES},
    })
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"You already have an active expense estimate (status: {existing['status']}). "
                   "Please complete or submit the existing one first.",
        )

    body = await request.json()

    if "travel_start_date" not in body:
        raise HTTPException(status_code=400, detail="travel_start_date is required (ISO format)")

    expense_items = body.get("expense_items", [])
    totals = _compute_totals(expense_items)

    doc = {
        "created_by": ObjectId(user_id),
        "created_by_name": user.get("name", ""),
        "employee_number": body.get("employee_number", user.get("employee_number", "") or user.get("employee_id", "")),
        "designation": body.get("designation", user.get("designation", "")),
        "department": body.get("department", "Sales"),
        "reporting_manager": body.get("reporting_manager", ""),
        "current_location": body.get("current_location", ""),
        "travel_start_date": body.get("travel_start_date"),
        "travel_end_date": body.get("travel_end_date"),
        "purpose_of_trip": body.get("purpose_of_trip", ""),
        "locations_visited": body.get("locations_visited", ""),
        "mode_of_travel": body.get("mode_of_travel", ""),
        "expense_items": expense_items,
        **totals,
        "advance_requested": float(body.get("advance_requested") or 0),
        "customer_visits": body.get("customer_visits", []),
        "planned_existing_visits": int(body.get("planned_existing_visits") or 0),
        "planned_new_visits": int(body.get("planned_new_visits") or 0),
        # actuals — empty until after trip
        "actual_expense_items": [],
        "actual_travel": 0,
        "actual_stay": 0,
        "actual_da": 0,
        "actual_total": 0,
        "actual_existing_visits": 0,
        "actual_new_visits": 0,
        "approved_total": 0,
        "amount_to_reimburse": 0,
        "amount_to_return": 0,
        "status": "Pending Review",
        "rejection_reason": None,
        "rahul_approved_at": None,
        "rahul_remarks": None,
        "amit_approved_at": None,
        "amit_remarks": None,
        "yogesh_processed_at": None,
        "yogesh_remarks": None,
        "yogesh_advance_released": False,
        "created_at": datetime.datetime.utcnow(),
        "updated_at": datetime.datetime.utcnow(),
        "actuals_submitted_at": None,
    }

    result = db.expense_estimates.insert_one(doc)
    est_id = str(result.inserted_id)

    first_approver = APPROVER_CHAIN[0]
    subject = f"New Expense Estimate – {user.get('name', '')} (Trip: {body.get('travel_start_date', '')[:10]})"
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
        <h2>New Expense Estimate for Review</h2>
        <p><b>{user.get('name', '')}</b> has submitted an expense estimate.</p>
        <ul>
            <li><b>Trip Dates:</b> {body.get('travel_start_date','')[:10]} to {body.get('travel_end_date','')[:10]}</li>
            <li><b>Locations:</b> {body.get('locations_visited','')}</li>
            <li><b>Estimated Total:</b> ₹{totals['estimated_total']:,.2f}</li>
            <li><b>Advance Requested:</b> ₹{float(body.get('advance_requested') or 0):,.2f}</li>
        </ul>
        <p>Please log in to review and approve.</p>
    </div>"""
    background_tasks.add_task(
        _notify_approver,
        first_approver["email"],
        {"_id": result.inserted_id, "created_by_name": user.get("name", ""), "travel_start_date": body.get("travel_start_date", "")},
        subject,
        html,
    )

    doc["_id"] = result.inserted_id
    return serialize_mongo_document(doc)


@router.get("/last-trip-summary")
def last_trip_summary(current_user: dict = Depends(get_current_user)):
    """
    Returns potential customer tracking stats from the SP's most recent completed trip.
    """
    user_id = _current_user_id(current_user)

    last_est = db.expense_estimates.find_one(
        {"created_by": ObjectId(user_id), "status": {"$in": ["Submitted", "Completed"]}},
        sort=[("travel_start_date", -1)],
    )
    if not last_est:
        return {"has_last_trip": False}

    potential_visits = [
        v for v in last_est.get("customer_visits", [])
        if v.get("potential_customer_id") or v.get("customer_type") == "potential"
    ]

    potential_ids = [
        ObjectId(v["potential_customer_id"])
        for v in potential_visits
        if v.get("potential_customer_id")
    ]

    onboarded_ids = []
    if potential_ids:
        onboarded_pcs = list(db.potential_customers.find(
            {"_id": {"$in": potential_ids}, "status": {"$in": ["Onboarded", "onboarded", "Customer"]}},
            {"_id": 1, "name": 1, "contact_id": 1},
        ))
        onboarded_ids = [pc["_id"] for pc in onboarded_pcs]
        onboarded_names = [pc.get("name", "") for pc in onboarded_pcs]
        # Map potential_customer_id → contact_id for invoice lookup
        zoho_ids = [pc.get("contact_id") for pc in onboarded_pcs if pc.get("contact_id")]
    else:
        onboarded_names = []
        zoho_ids = []

    orders_count = 0
    orders_total = 0.0
    if zoho_ids:
        pipeline = [
            {"$match": {"customer_id": {"$in": zoho_ids}}},
            {"$group": {"_id": None, "count": {"$sum": 1}, "total": {"$sum": {"$toDouble": {"$ifNull": ["$total", 0]}}}}}
        ]
        agg = list(db.invoices.aggregate(pipeline))
        if agg:
            orders_count = agg[0].get("count", 0)
            orders_total = agg[0].get("total", 0.0)

    return {
        "has_last_trip": True,
        "trip_start": last_est.get("travel_start_date", "")[:10] if last_est.get("travel_start_date") else "",
        "trip_end": last_est.get("travel_end_date", "")[:10] if last_est.get("travel_end_date") else "",
        "locations": last_est.get("locations_visited", ""),
        "potential_customers_visited": len(potential_visits),
        "onboarded_count": len(onboarded_ids),
        "onboarded_names": onboarded_names,
        "orders_received_count": orders_count,
        "orders_received_total": orders_total,
    }


@router.get("/{estimate_id}")
def get_estimate(estimate_id: str, current_user: dict = Depends(get_current_user)):
    user_id = _current_user_id(current_user)
    est = _get_estimate_or_404(estimate_id)
    if str(est["created_by"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your estimate")
    return serialize_mongo_document(est)


@router.put("/{estimate_id}")
async def update_estimate(
    estimate_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = _current_user_id(current_user)
    est = _get_estimate_or_404(estimate_id)

    if str(est["created_by"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your estimate")
    if est["status"] == "Rejected":
        raise HTTPException(status_code=400, detail="Rejected estimates cannot be edited")
    if est["status"] not in ["Pending Review", "Pending Second Review", "Pending Payment", "Draft"]:
        raise HTTPException(status_code=400, detail=f"Cannot edit estimate in status '{est['status']}'")

    body = await request.json()

    if est["status"] == "Pending Review":
        expense_items = body.get("expense_items", est.get("expense_items", []))
        totals = _compute_totals(expense_items)
        update = {
            "employee_number": body.get("employee_number", est.get("employee_number")),
            "designation": body.get("designation", est.get("designation")),
            "department": body.get("department", est.get("department")),
            "reporting_manager": body.get("reporting_manager", est.get("reporting_manager")),
            "current_location": body.get("current_location", est.get("current_location")),
            "travel_start_date": body.get("travel_start_date", est.get("travel_start_date")),
            "travel_end_date": body.get("travel_end_date", est.get("travel_end_date")),
            "purpose_of_trip": body.get("purpose_of_trip", est.get("purpose_of_trip")),
            "locations_visited": body.get("locations_visited", est.get("locations_visited")),
            "mode_of_travel": body.get("mode_of_travel", est.get("mode_of_travel")),
            "expense_items": expense_items,
            **totals,
            "advance_requested": float(body.get("advance_requested") or est.get("advance_requested") or 0),
            "customer_visits": body.get("customer_visits", est.get("customer_visits", [])),
            "planned_existing_visits": int(body.get("planned_existing_visits") or est.get("planned_existing_visits") or 0),
            "planned_new_visits": int(body.get("planned_new_visits") or est.get("planned_new_visits") or 0),
            "updated_at": datetime.datetime.utcnow(),
        }

    elif est["status"] in ("Pending Second Review", "Pending Payment"):
        # Post-approval: SP can only update visit data (not expense amounts or trip details)
        update = {
            "customer_visits": body.get("customer_visits", est.get("customer_visits", [])),
            "planned_existing_visits": int(body.get("planned_existing_visits") or est.get("planned_existing_visits") or 0),
            "planned_new_visits": int(body.get("planned_new_visits") or est.get("planned_new_visits") or 0),
            "updated_at": datetime.datetime.utcnow(),
        }

    else:
        # Draft — only actuals fields editable
        actual_items = body.get("actual_expense_items", est.get("actual_expense_items", []))
        actual_totals = _compute_actual_totals(actual_items)
        update = {
            "actual_expense_items": actual_items,
            **actual_totals,
            "actual_existing_visits": int(body.get("actual_existing_visits") or est.get("actual_existing_visits") or 0),
            "actual_new_visits": int(body.get("actual_new_visits") or est.get("actual_new_visits") or 0),
            "customer_visits": body.get("customer_visits", est.get("customer_visits", [])),
            "updated_at": datetime.datetime.utcnow(),
        }

    db.expense_estimates.update_one({"_id": ObjectId(estimate_id)}, {"$set": update})
    return serialize_mongo_document(db.expense_estimates.find_one({"_id": ObjectId(estimate_id)}))


@router.post("/{estimate_id}/submit-actuals")
async def submit_actuals(
    estimate_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
):
    user_id = _current_user_id(current_user)
    est = _get_estimate_or_404(estimate_id)

    if str(est["created_by"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your estimate")
    if est["status"] != "Draft":
        raise HTTPException(status_code=400, detail="Actuals can only be submitted when status is 'Draft'")

    body = await request.json()
    actual_items = body.get("actual_expense_items", est.get("actual_expense_items", []))
    actual_totals = _compute_actual_totals(actual_items)

    update = {
        "actual_expense_items": actual_items,
        **actual_totals,
        "actual_existing_visits": int(body.get("actual_existing_visits") or 0),
        "actual_new_visits": int(body.get("actual_new_visits") or 0),
        "customer_visits": body.get("customer_visits", est.get("customer_visits", [])),
        "status": "Submitted",
        "actuals_submitted_at": datetime.datetime.utcnow(),
        "updated_at": datetime.datetime.utcnow(),
    }
    db.expense_estimates.update_one({"_id": ObjectId(estimate_id)}, {"$set": update})

    updated = db.expense_estimates.find_one({"_id": ObjectId(estimate_id)})

    name = est.get("created_by_name", "")
    subject = f"Expense Actuals Submitted – {name}"
    html = f"<p>{name} has submitted their actual expenses for the trip starting {est.get('travel_start_date','')[:10]}. Please review and process settlement.</p>"
    for approver_email in ["events@barkbutler.in", "barksalesamit@gmail.com"]:
        approver_user = _get_user_by_email(approver_email)
        if approver_user:
            create_notification(
                db,
                str(approver_user["_id"]),
                "expense_actuals_submitted",
                subject,
                f"{name} submitted actuals for trip {est.get('travel_start_date','')[:10]}",
                f"/admin/expense_estimates",
            )
        background_tasks.add_task(_send_email, approver_email, subject, html)

    return serialize_mongo_document(updated)


@router.post("/{estimate_id}/sync-daily-visits")
def sync_daily_visits(estimate_id: str, current_user: dict = Depends(get_current_user)):
    """
    Pull outcome/follow_up_date/order_value from daily visit shop records into
    this estimate's customer_visits, matching by customer_id or potential_customer_id
    within the trip date range for the same salesperson.
    """
    user_id = _current_user_id(current_user)
    est = _get_estimate_or_404(estimate_id)
    if str(est["created_by"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your estimate")

    start_str = est.get("travel_start_date", "")
    end_str = est.get("travel_end_date", "")
    if not start_str or not end_str:
        raise HTTPException(status_code=400, detail="Trip dates not set on estimate")

    # Parse trip window (dates are stored as ISO strings)
    try:
        trip_start = datetime.datetime.fromisoformat(start_str).replace(hour=0, minute=0, second=0, microsecond=0)
        trip_end = datetime.datetime.fromisoformat(end_str).replace(hour=23, minute=59, second=59, microsecond=999999)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid trip date format")

    # Fetch all daily_visits for this SP within the trip window (+/- 1 day buffer)
    buffer = datetime.timedelta(days=1)
    daily_visits = list(db.daily_visits.find({
        "created_by": ObjectId(user_id),
        "created_at": {"$gte": trip_start - buffer, "$lte": trip_end + buffer},
    }))

    # Build lookup maps: customer_id → shop, potential_customer_id → shop
    cid_to_shop: dict = {}
    pcid_to_shop: dict = {}
    for dv in daily_visits:
        for shop in dv.get("shops", []):
            cid = shop.get("customer_id")
            if cid:
                cid_to_shop[str(cid)] = shop
            pcid = shop.get("potential_customer_id")
            if pcid:
                pcid_to_shop[str(pcid)] = shop
        # Also include update entries
        for upd in dv.get("updates", []):
            cid = upd.get("customer_id")
            if cid:
                cid_to_shop[str(cid)] = upd
            pcid = upd.get("potential_customer_id")
            if pcid:
                pcid_to_shop[str(pcid)] = upd

    updated_visits = []
    synced = 0
    for visit in est.get("customer_visits", []):
        matched_shop = None
        if visit.get("customer_id"):
            matched_shop = cid_to_shop.get(str(visit["customer_id"]))
        if not matched_shop and visit.get("potential_customer_id"):
            matched_shop = pcid_to_shop.get(str(visit["potential_customer_id"]))

        if matched_shop:
            synced += 1
            visit = {
                **visit,
                "outcome": matched_shop.get("reason") or visit.get("outcome", ""),
                "follow_up_date": (
                    matched_shop.get("potential_customer_follow_up_date")
                    or matched_shop.get("follow_up_date")
                    or visit.get("follow_up_date", "")
                ),
                "order_value": (
                    str(matched_shop.get("order_amount", ""))
                    or visit.get("order_value", "")
                ),
            }
        updated_visits.append(visit)

    db.expense_estimates.update_one(
        {"_id": ObjectId(estimate_id)},
        {"$set": {"customer_visits": updated_visits, "updated_at": datetime.datetime.utcnow()}},
    )
    updated = db.expense_estimates.find_one({"_id": ObjectId(estimate_id)})
    return {**serialize_mongo_document(updated), "synced_count": synced}
