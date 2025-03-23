from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    File,
    UploadFile,
    Form,
    Depends,
)
from fastapi.responses import JSONResponse, Response, StreamingResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from .helpers import get_access_token
from typing import Optional
import re, requests, os, json
from dotenv import load_dotenv
import boto3, io, csv, openpyxl
from botocore.exceptions import BotoCoreError, NoCredentialsError
from pytz import timezone
from datetime import date, timedelta, datetime
from .admin_trainings import router as admin_trainings_router
from .admin_catalogues import router as admin_catalogues_router
from .admin_salespeople import router as admin_salespeople_router
from .admin_special_margins import router as admin_special_margins_router
from .admin_announcements import router as admin_announcements_router
from .admin_daily_visits import router as admin_daily_visits_router
from .admin_hooks_categories import router as admin_hooks_categories_router
from .admin_hooks import router as admin_hooks_router
from .admin_potential_customers import router as admin_potential_customers_router
from .admin_expected_reorders import router as admin_expected_reorders_router
from .admin_targeted_customers import router as admin_targeted_customers_router
from backend.config.auth import JWTBearer  # type: ignore

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")  # Default to ap-south-1
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

MAX_FILE_SIZE_MB = 10


@router.get("/stats")
async def get_stats():
    try:
        # Products Statistics
        active_stock_products = db["products"].count_documents({"stock": {"$gt": 0}})
        inactive_products = db["products"].count_documents({"status": "inactive"})
        total_products = db["products"].count_documents({})
        active_products = db["products"].count_documents({"status": "active"})
        out_of_stock_products = db["products"].count_documents({"stock": {"$lte": 0}})

        # Customers Statistics
        assigned_customers = db["customers"].count_documents(
            {"cf_sales_person": {"$exists": True, "$ne": "", "$ne": None}}
        )
        unassigned_customers = db["customers"].count_documents(
            {
                "$or": [
                    {"cf_sales_person": {"$exists": False}},
                    {"cf_sales_person": ""},
                    {"cf_sales_person": None},
                ]
            }
        )
        active_customers = db["customers"].count_documents({"status": "active"})
        inactive_customers = db["customers"].count_documents({"status": "inactive"})

        # Sales People Statistics
        active_sales_people = db["users"].count_documents(
            {"status": "active", "role": "sales_person"}
        )
        inactive_sales_people = db["users"].count_documents(
            {"status": "inactive", "role": "sales_person"}
        )
        total_sales_people = active_sales_people + inactive_sales_people

        # Orders Statistics
        ist = timezone("Asia/Kolkata")
        now_ist = datetime.now(ist)
        start_of_today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        recent_orders = db["orders"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        orders_draft = db["orders"].count_documents(
            {"status": "draft", "created_at": {"$gte": start_of_today_ist}}
        )
        orders_accepted = db["orders"].count_documents(
            {"status": "accepted", "created_at": {"$gte": start_of_today_ist}}
        )
        orders_declined = db["orders"].count_documents(
            {"status": "declined", "created_at": {"$gte": start_of_today_ist}}
        )
        orders_invoiced = db["orders"].count_documents(
            {"status": "invoiced", "created_at": {"$gte": start_of_today_ist}}
        )
        active_catalogues = db["catalogues"].count_documents({"is_active": True})
        inactive_catalogues = db["catalogues"].count_documents({"is_active": False})

        active_trainings = db["trainings"].count_documents({"is_active": True})
        inactive_trainings = db["trainings"].count_documents({"is_active": False})

        active_announcements = db["announcements"].count_documents({"is_active": True})
        inactive_announcements = db["announcements"].count_documents(
            {"is_active": False}
        )
        # Get today's date
        today = date.today()

        day_before_yesterday = today - timedelta(days=2)

        # Convert both dates to ISO format (YYYY-MM-DD)
        day_before_yesterday_str = day_before_yesterday.isoformat()
        today_str = today.isoformat()
        # Get the overdue invoices for yesterday
        total_due_payments = db["invoices"].count_documents(
            {
                "due_date": {"$lt": today_str},
                "status": {"$nin": ["paid", "void"]},
            }
        )
        total_due_payments_today = db["invoices"].count_documents(
            {
                "due_date": {"$gt": day_before_yesterday_str, "$lt": today_str},
                "status": {"$nin": ["paid", "void"]},
            }
        )
        submitted_daily_visits = db["daily_visits"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        updated_daily_visits = db["daily_visits"].count_documents(
            {"updates": {"$exists": True}, "created_at": {"$gte": start_of_today_ist}}
        )
        active_hook_categories = db["hooks_category"].count_documents(
            {"is_active": True}
        )
        inactive_hook_categories = db["hooks_category"].count_documents(
            {"is_active": False}
        )
        submitted_shop_hooks = db["shop_hooks"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        submitted_potential_customers = db["potential_customers"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        submitted_targeted_customers = db["targeted_customers"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        submitted_expected_reorders = db["expected_reorders"].count_documents(
            {"created_at": {"$gte": start_of_today_ist}}
        )
        return {
            "active_stock_products": active_stock_products,
            "active_products": active_products,
            "inactive_products": inactive_products,
            "total_products": total_products,
            "out_of_stock_products": out_of_stock_products,
            "assigned_customers": assigned_customers,
            "unassigned_customers": unassigned_customers,
            "active_customers": active_customers,
            "inactive_customers": inactive_customers,
            "active_sales_people": active_sales_people,
            "inactive_sales_people": inactive_sales_people,
            "total_sales_people": total_sales_people,
            "orders_draft": orders_draft,
            "orders_accepted": orders_accepted,
            "orders_declined": orders_declined,
            "orders_invoiced": orders_invoiced,
            "recent_orders": recent_orders,
            "active_catalogues": active_catalogues,
            "inactive_catalogues": inactive_catalogues,
            "active_trainings": active_trainings,
            "inactive_trainings": inactive_trainings,
            "active_announcements": active_announcements,
            "inactive_announcements": inactive_announcements,
            "total_due_payments": total_due_payments,
            "total_due_payments_today": total_due_payments_today,
            "submitted_daily_visits": submitted_daily_visits,
            "updated_daily_visits": updated_daily_visits,
            "active_hook_categories": active_hook_categories,
            "inactive_hook_categories": inactive_hook_categories,
            "submitted_shop_hooks": submitted_shop_hooks,
            "submitted_potential_customers": submitted_potential_customers,
            "submitted_targeted_customers": submitted_targeted_customers,
            "submitted_expected_reorders": submitted_expected_reorders,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brands")
def get_all_brands():
    """
    Retrieve a list of all distinct brands.
    """
    try:
        brands = products_collection.distinct(
            "brand", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        brands = [brand for brand in brands if brand]  # Remove empty or null brands
        return {"brands": brands}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.get("/categories")
def get_all_categories():
    """
    Retrieve a list of all distinct categories.
    """
    try:
        categories = products_collection.distinct(
            "category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        categories = [
            category for category in categories if category
        ]  # Remove empty or null categories
        return {"categories": categories}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch categories.")


@router.get("/sub_categories")
def get_all_sub_categories():
    """
    Retrieve a list of all distinct sub_categories.
    """
    try:
        sub_categories = products_collection.distinct(
            "sub_category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        sub_categories = [
            sub_category for sub_category in sub_categories if sub_category
        ]  # Remove empty or null sub_categories
        return {"sub_categories": sub_categories}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch sub_categories.")


@router.get("/products")
def get_products(
    page: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    # New query params
    status: Optional[str] = None,  # e.g. 'active' or 'inactive'
    stock: Optional[str] = None,  # e.g. 'zero' or 'gt_zero'
    new_arrivals: Optional[bool] = None,
    missing_info_products: Optional[bool] = None,
    sort_by: Optional[str] = None,
):
    """
    Retrieve products with optional search, brand, status, stock, and new_arrivals filtering.
    """

    try:
        query = {}

        # 1) Status Filter
        if status == "active":
            query["status"] = "active"
        elif status == "inactive":
            query["status"] = "inactive"

        # 2) Stock Filter
        if stock == "zero":
            # products where stock = 0
            query["stock"] = {"$lte": 0}
        elif stock == "gt_zero":
            # products where stock > 0
            query["stock"] = {"$gt": 0}

        # 3) New Arrivals (depending on how you define "new")
        if new_arrivals:
            # If your DB has a boolean field `is_new`
            # query["is_new"] = True

            # Or if it's based on creation date (last 30 days, etc.)
            from datetime import datetime, timedelta

            ninty_days_ago = datetime.now() - timedelta(days=90)
            query["created_at"] = {"$gte": ninty_days_ago}

        if missing_info_products:
            query["$and"] = [
                {"$or": [{"series": {"$exists": False}}, {"series": ""}]},
                {"$or": [{"category": {"$exists": False}}, {"category": ""}]},
                {"$or": [{"sub_category": {"$exists": False}}, {"sub_category": ""}]},
            ]

        # 4) Search Filter
        if search and search.strip() != "":
            regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # 5) Brand Filter
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}

        if category and category.lower() != "all":
            query["category"] = {"$regex": f"^{category}$", "$options": "i"}

        if sub_category and sub_category.lower() != "all":
            query["sub_category"] = {"$regex": f"^{sub_category}$", "$options": "i"}

        # Pagination
        skip = page * limit

        docs_cursor = (
            products_collection.find(query)
            .sort(
                [("catalogue_order", 1)]
                if sort_by == "catalogue"
                else [("status", 1), ("name", 1)]
            )
            .skip(skip)
            .limit(limit)
        )
        print(json.dumps(query, indent=4))
        total_count = products_collection.count_documents(query)
        products = [serialize_mongo_document(doc) for doc in docs_cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return JSONResponse(
            {
                "products": products,
                "total_count": total_count,
                "page": page,
                "per_page": limit,
                "total_pages": total_pages,
            }
        )

    except Exception as e:
        print(e)
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.get("/products/download")
def download_products(
    search: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    status: Optional[str] = None,  # e.g. 'active' or 'inactive'
    stock: Optional[str] = None,  # e.g. 'zero' or 'gt_zero'
    new_arrivals: Optional[bool] = None,
    missing_info_products: Optional[bool] = None,
):
    """
    Download products in XLSX format using the same filters.
    """
    try:
        query = {}

        # 1) Status Filter
        if status == "active":
            query["status"] = "active"
        elif status == "inactive":
            query["status"] = "inactive"

        # 2) Stock Filter
        if stock == "zero":
            query["stock"] = {"$lte": 0}
        elif stock == "gt_zero":
            query["stock"] = {"$gt": 0}

        # 3) New Arrivals
        if new_arrivals:
            ninety_days_ago = datetime.now() - timedelta(days=90)
            query["created_at"] = {"$gte": ninety_days_ago}

        # 4) Missing Info Filter for series, category, sub_category
        if missing_info_products:
            query["$and"] = [
                {"$or": [{"series": {"$exists": False}}, {"series": ""}]},
                {"$or": [{"category": {"$exists": False}}, {"category": ""}]},
                {"$or": [{"sub_category": {"$exists": False}}, {"sub_category": ""}]},
            ]

        # 5) Search Filter
        if search and search.strip() != "":
            regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # 6) Brand, Category, Sub Category Filters
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}
        if category and category.lower() != "all":
            query["category"] = {"$regex": f"^{category}$", "$options": "i"}
        if sub_category and sub_category.lower() != "all":
            query["sub_category"] = {"$regex": f"^{sub_category}$", "$options": "i"}

        docs_cursor = products_collection.find(query).sort([("status", 1), ("name", 1)])
        products = [serialize_mongo_document(doc) for doc in docs_cursor]

        # Create a new workbook and worksheet using openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Products"

        # Define headers (adjust these fields as needed)
        headers = [
            "Name",
            "Brand",
            "Category",
            "Sub Category",
            "Series",
            "SKU",
            "Price",
            "Stock",
            "Status",
            "Created At",
        ]
        ws.append(headers)

        for product in products:
            # Convert created_at to a formatted string if present
            created_at = ""
            if product.get("created_at"):
                if isinstance(product["created_at"], datetime):
                    created_at = product["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    created_at = str(product["created_at"])

            row = [
                product.get("name", ""),
                product.get("brand", ""),
                product.get("category", ""),
                product.get("sub_category", ""),
                product.get("series", ""),
                product.get("cf_sku_code", ""),
                product.get("rate", ""),
                product.get("stock", ""),
                product.get("status", ""),
                created_at,
            ]
            ws.append(row)

        # Save workbook to a BytesIO stream
        stream = io.BytesIO()
        wb.save(stream)
        stream.seek(0)

        filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        headers_response = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers_response,
        )

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/customers")
def get_customers(
    name: Optional[str] = None,
    page: int = Query(1, ge=1, description="1-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sort: Optional[str] = None,
    status: Optional[str] = Query(
        None, description="Filter by customer status: active or inactive"
    ),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    unassigned: Optional[bool] = Query(
        None, description="Filter for unassigned customers"
    ),
    gst_type: Optional[str] = Query(
        None, description="Filter by customer type: exclusive or inclusive"
    ),
):
    """
    Returns a paginated list of customers, optionally filtered by name.
    Sorting is optional:
      - Default sort is ascending by 'status'
      - If ?sort=desc, it will sort descending by 'status'
    """
    try:
        query = {}
        # Filter by name if provided
        if name:
            query["contact_name"] = re.compile(re.escape(name), re.IGNORECASE)
        # Sort logic
        sort_order = [("status", 1)]  # default ascending by status
        if sort and sort.lower() == "desc":
            sort_order = [("status", -1)]

        # Filter by status if provided
        if status:
            if status.lower() not in ["active", "inactive"]:
                raise HTTPException(
                    status_code=400, detail="Invalid status filter value"
                )
            query["status"] = status.lower()

        # Filter by sales_person if provided
        if sales_person:
            escaped_sales_person = re.escape(sales_person)
            query["$or"] = [
                {
                    "cf_sales_person": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
                {
                    "salesperson_name": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
            ]

        # Filter for unassigned customers if true
        if unassigned:
            query["$or"] = [
                {"cf_sales_person": {"$exists": False}},
                {"cf_sales_person": ""},
                {"cf_sales_person": None},
            ]
        if gst_type:
            if str(gst_type).capitalize() == "Inclusive":
                query["$and"] = [
                    {"cf_in_ex": {"$exists": True}},
                    {"cf_in_ex": "Inclusive"},
                ]
            else:
                query["$or"] = [
                    {"cf_in_ex": {"$exists": False}},
                    {"cf_in_ex": "Exclusive"},
                ]

        # print(json.dumps(query, indent=4))
        # Calculate skip based on 1-based indexing
        skip = (page - 1) * limit
        cursor = (
            customers_collection.find(query).sort(sort_order).skip(skip).limit(limit)
        )
        # Count total matching documents for pagination
        total_count = customers_collection.count_documents(query)
        customers = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")

        return {
            "customers": customers,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.get("/customers/report")
def get_customers_report(
    name: Optional[str] = None,
    sort: Optional[str] = None,
    status: Optional[str] = Query(
        None, description="Filter by customer status: active or inactive"
    ),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    unassigned: Optional[bool] = Query(
        None, description="Filter for unassigned customers"
    ),
    gst_type: Optional[str] = Query(
        None, description="Filter by customer type: exclusive or inclusive"
    ),
):
    # Build the query similar to your /customers endpoint.
    query = {}
    if name:
        query["contact_name"] = re.compile(re.escape(name), re.IGNORECASE)

    sort_order = [("status", 1)]
    if sort and sort.lower() == "desc":
        sort_order = [("status", -1)]

    if status:
        if status.lower() not in ["active", "inactive"]:
            raise HTTPException(status_code=400, detail="Invalid status filter value")
        query["status"] = status.lower()

    if sales_person:
        escaped_sales_person = re.escape(sales_person)
        query["$or"] = [
            {
                "cf_sales_person": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
            {
                "salesperson_name": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
        ]

    if unassigned:
        query["$or"] = [
            {"cf_sales_person": {"$exists": False}},
            {"cf_sales_person": ""},
            {"cf_sales_person": None},
        ]

    if gst_type:
        if str(gst_type).capitalize() == "Inclusive":
            query["$and"] = [
                {"cf_in_ex": {"$exists": True}},
                {"cf_in_ex": "Inclusive"},
            ]
        else:
            query["$or"] = [
                {"cf_in_ex": {"$exists": False}},
                {"cf_in_ex": "Exclusive"},
            ]

    # Fetch matching customers (adjust as necessary for your setup)
    customers_cursor = customers_collection.find(query).sort(sort_order)
    customers = [serialize_mongo_document(doc) for doc in customers_cursor]

    # Create an Excel workbook using openpyxl.
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Customers Report"

    # Define the header row.
    headers = [
        "Customer Name",
        "Sales Person",
        "GST Number",
        "Status",
        "Place Of Supply",
    ]
    ws.append(headers)

    for cust in customers:
        # Extract state codes from each address
        addresses = cust.get("addresses", [])
        state_codes = set()
        for addr in addresses:
            state_value = addr.get("state", "")
            if state_value:
                state_codes.add(state_value.title())
        place_of_supply = ", ".join(state_codes)

        # Handle sales person conversion if it's a list.
        sales_person_val = cust.get("cf_sales_person", "") or cust.get(
            "salesperson_name", ""
        )
        if isinstance(sales_person_val, list):
            sales_person_val = ", ".join(sales_person_val)

        row = [
            cust.get("contact_name", ""),
            sales_person_val,
            cust.get("gst_no", "-"),
            cust.get("status", ""),
            place_of_supply,
        ]
        ws.append(row)

    # Save the workbook to a binary stream.
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=customers_report.xlsx"},
    )


@router.get("/orders")
def read_all_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    status: Optional[str] = Query(None, description="Filter by order status"),
    estimate_created: Optional[bool] = Query(
        None, description="Filter by whether estimate was created"
    ),
    amount: Optional[str] = Query(None, description="Filter by amount"),
):
    """
    Retrieve all orders for admin, with pagination and optional filters,
    converting created_at to IST in MongoDB.
    """
    # Build the match stage based on filters
    match_stage = {"$match": {}}
    second_match_stage = {"$match": {}}

    if status:
        match_stage["$match"]["status"] = status.lower()

    if estimate_created is not None:
        match_stage["$match"]["estimate_created"] = estimate_created

    if amount:
        match_stage["$match"]["total_amount"] = {"$gt": 0}

    if sales_person:
        # Assuming 'created_by_info.name' is the field to filter
        second_match_stage["$match"]["created_by_info.code"] = sales_person
    # Count total orders (for the frontend) without pagination but with filters
    total_count = orders_collection.count_documents(match_stage["$match"])
    # Now build our aggregation pipeline
    pipeline = [
        match_stage,
        {"$sort": {"created_at": -1}},  # sort descending by created_at
        {"$skip": page * limit},  # skip
        {"$limit": limit},  # limit
        # Optional: Join user info from "users" collection
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        # Unwind the created_by_info array so it's a single object
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
        # Convert created_at (UTC) to a string in IST
        {
            "$project": {
                # Keep the original fields (except created_by_info is now an object)
                "created_by": 1,
                "total_amount": 1,
                "total_gst": 1,
                "gst_type": 1,
                "status": 1,
                "products": 1,
                "shipping_address": 1,
                "billing_address": 1,
                "customer_id": 1,
                "customer_name": 1,
                "estimate_url": 1,
                "estimate_created": 1,
                "estimate_number": 1,
                "estimate_id": 1,
                "reference_number": 1,
                "spreadsheet_url": 1,
                "spreadsheet_created": 1,
                # ... include any other fields you want
                # Convert the "created_at" date to a string in IST
                "created_at": {
                    "$dateToString": {
                        "date": "$created_at",
                        "format": "%Y-%m-%d %H:%M:%S",  # date/time format
                        "timezone": "Asia/Kolkata",
                    }
                },
                "updated_at": {
                    "$dateToString": {
                        "date": "$updated_at",
                        "format": "%Y-%m-%d %H:%M:%S",  # date/time format
                        "timezone": "Asia/Kolkata",
                    }
                },
                # Flatten out or rename fields from created_by_info:
                "created_by_info.id": {"$toString": "$created_by_info._id"},
                "created_by_info.name": "$created_by_info.name",
                "created_by_info.email": "$created_by_info.email",
                "created_by_info.code": "$created_by_info.code",
                # Or keep the entire object if you prefer:
                # "created_by_info": 1
                # but then you'd still need to convert _id to string, if you want
            }
        },
        second_match_stage,
    ]

    # Execute the pipeline
    orders_cursor = orders_collection.aggregate(pipeline)

    # Convert each Mongo document to JSON-serializable Python dict
    orders_with_user_info = [serialize_mongo_document(doc) for doc in orders_cursor]
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
    # Validate page number
    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "orders": orders_with_user_info,
        "total_count": total_count,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
    }


@router.get("/payments_due")
def read_all_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sales_person: str = Query(None, description="Filter by sales person"),
    invoice_number: str = Query(None, description="Filter by Invoice number"),
):
    """
    Retrieve all invoices past their due_date with pagination.
    page:  0-based page index
    limit: number of invoices per page
    """
    # Get today's date in ISO format (YYYY-MM-DD)
    today_str = date.today().isoformat()

    # Query to match invoices with a due_date less than today
    query = {"due_date": {"$lt": today_str}, "status": {"$nin": ["paid", "void"]}}
    # If you also want to ensure the invoice has a specific status (e.g., "overdue"),
    # you can combine conditions like this:
    # query = {"due_date": {"$lt": today_str}, "status": "overdue"}
    if sales_person:
        escaped_sales_person = re.escape(sales_person)
        query["$or"] = [
            {
                "cf_sales_person": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
            {
                "salesperson_name": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
        ]

    if invoice_number:
        query["invoice_number"] = {
            "$regex": f"^{invoice_number.strip()}$",
            "$options": "i",
        }

    # Basic query stage for the aggregation pipeline
    match_stage = {"$match": query}

    # Count total invoices matching the query (for frontend pagination)
    total_count = db.invoices.count_documents(query)

    # Build the aggregation pipeline
    pipeline = [
        match_stage,
        # Project only the necessary fields
        {
            "$lookup": {
                "from": "invoice_notes",  # Collection to join
                "localField": "invoice_number",  # Field from the invoices collection
                "foreignField": "invoice_number",  # Field from the invoice_notes collection
                "as": "invoice_notes",  # The result will be an array of matching documents
            }
        },
        {
            "$unwind": {
                "path": "$invoice_notes",  # Unwind the array of invoice_notes
                "preserveNullAndEmptyArrays": True,  # Keep invoices even if no notes exist
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "invoice_notes.created_by",
                "foreignField": "_id",
                "as": "user_created_by",
            }
        },
        {
            "$unwind": {
                "path": "$user_created_by",  # note the change from "$note_created_by" to "$user_created_by"
                "preserveNullAndEmptyArrays": True,
            }
        },
        # Project the necessary fields and replace created_by with the user's first name
        {
            "$project": {
                "created_at": 1,
                "total": 1,
                "due_date": {"$dateFromString": {"dateString": "$due_date"}},
                "balance": 1,
                "status": {"$toString": "overdue"},
                "cf_sales_person": 1,
                "salesperson_name": 1,
                "customer_id": 1,
                "customer_name": 1,
                "invoice_url": 1,
                "invoice_number": 1,
                "invoice_id": 1,
                "line_items": 1,
                "created_by_name": 1,
                "overdue_by_days": {
                    "$dateDiff": {
                        "startDate": {"$dateFromString": {"dateString": "$due_date"}},
                        "endDate": "$$NOW",
                        "unit": "day",
                    }
                },
                "invoice_notes": 1,
                # Replace the invoice note's created_by with the user's first name
                "note_created_by_name": "$user_created_by.first_name",
            }
        },
        # Now sort by the converted due_date
        {"$sort": {"due_date": -1}},
        {"$skip": page * limit},  # Skip the appropriate number of documents
        {"$limit": limit},  # Limit the number of documents returned
    ]
    # Execute the aggregation pipeline
    invoices_cursor = db.invoices.aggregate(pipeline)

    # Convert each Mongo document to a JSON-serializable Python dict
    inv = [serialize_mongo_document(doc) for doc in invoices_cursor]
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

    # Validate page number
    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "invoices": inv,
        "total_count": total_count,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
    }


@router.get("/payments_due/download_csv")
def download_payments_due_csv(sales_person: str):
    """
    Download all invoices past their due_date (and not paid) as a CSV file.
    """
    today_str = date.today().isoformat()

    # Query to match invoices with a due_date less than today and status not in ["paid"]
    query = {"due_date": {"$lt": today_str}, "status": {"$nin": ["paid", "void"]}}
    if sales_person:
        query["$or"] = [
            {"cf_sales_person": sales_person},
            {"salesperson_name": sales_person},
        ]
    match_stage = {"$match": query}

    # Build the aggregation pipeline similar to the table data route
    pipeline = [
        match_stage,
        {"$sort": {"due_date": -1}},
        {
            "$lookup": {
                "from": "invoice_notes",  # Collection to join
                "localField": "invoice_number",  # Field from the invoices collection
                "foreignField": "invoice_number",  # Field from the invoice_notes collection
                "as": "invoice_notes",  # The result will be an array of matching documents
            }
        },
        {
            "$unwind": {
                "path": "$invoice_notes",  # Unwind the array of invoice_notes
                "preserveNullAndEmptyArrays": True,  # Keep invoices even if no notes exist
            }
        },
        {
            "$project": {
                "created_at": 1,
                "total": 1,
                "due_date": {"$dateFromString": {"dateString": "$due_date"}},
                "balance": 1,
                # For CSV purposes, you may output the status directly if needed
                "status": {"$toString": "overdue"},
                "cf_sales_person": 1,
                "created_by_name": 1,
                "salesperson_name": 1,
                "customer_id": 1,
                "customer_name": 1,
                "invoice_url": 1,
                "invoice_number": 1,
                "invoice_id": 1,
                "line_items": 1,
                "overdue_by_days": {
                    "$dateDiff": {
                        "startDate": {"$dateFromString": {"dateString": "$due_date"}},
                        "endDate": "$$NOW",
                        "unit": "day",
                    }
                },
                "invoice_notes": 1,
            }
        },
    ]

    # Execute the aggregation pipeline
    invoices_cursor = db.invoices.aggregate(pipeline)
    invoices = [serialize_mongo_document(doc) for doc in invoices_cursor]

    # Create a CSV in memory
    output = io.StringIO()
    fieldnames = [
        "Created At",
        "Due Date",
        "Invoice Number",
        "Overdue by Days",
        "Customer Name",
        "Status",
        "CF Sales Person",
        "Invoice Sales Person",
        "Created By",
        "Total",
        "Balance",
        "Additional Information",
        "Images",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for invoice in invoices:
        writer.writerow(
            {
                "Created At": invoice.get("created_at"),
                "Due Date": invoice.get("due_date"),
                "Invoice Number": invoice.get("invoice_number"),
                "Overdue by Days": invoice.get("overdue_by_days"),
                "Customer Name": invoice.get("customer_name"),
                "Status": invoice.get("status"),
                "CF Sales Person": invoice.get("cf_sales_person")
                or invoice.get("salesperson_name", "-"),
                "Invoice Sales Person": invoice.get("salesperson_name"),
                "Created By": invoice.get("created_by_name"),
                "Total": invoice.get("total"),
                "Balance": invoice.get("balance"),
                "Additional Information": invoice.get("invoice_notes", {}).get(
                    "additional_info", ""
                ),
                "Images": ", ".join(invoice.get("invoice_notes", {}).get("images", [])),
            }
        )

    csv_data = output.getvalue()

    # Return CSV file as attachment
    response = Response(content=csv_data, media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=payments_due.csv"
    return response


@router.get("/sales-people")
def get_sales_people():
    """
    Retrieve a list of sales people from the users collection.
    Assuming users have a role or designation that identifies them as sales people.
    """
    # Replace 'sales' with the actual role identifier
    sales_people_cursor = users_collection.find(
        {"role": "sales_person"}, {"code": 1, "_id": 0}
    )
    sales_people = [f"{user['code']}" for user in sales_people_cursor]
    return {"sales_people": sales_people}


@router.put("/customers/bulk-update")
async def bulk_update_customers(payload: dict):
    """
    Bulk update multiple customers in one request.
    Expects a JSON body:
    {
      "updates": [
        {
          "_id": "someCustomerId",
          "cf_sales_person": "SP1, SP2",
          ...
        },
        ...
      ]
    }
    """
    updates = payload.get("updates", [])
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    updated_count = 0
    results = []

    for item in updates:
        customer_id = item.get("_id")
        if not customer_id:
            # Skip if no _id
            results.append(
                {
                    "status": "skipped - missing _id",
                }
            )
            continue

        # ------------------------------------------------------------
        # 1) Prepare the update_data (exclude _id, skip None)
        # ------------------------------------------------------------
        update_data = {k: v for k, v in item.items() if k != "_id" and v is not None}
        if not update_data:
            results.append(
                {"customer_id": customer_id, "status": "skipped - no valid fields"}
            )
            continue

        # Convert cf_sales_person from comma-string to a list, if present
        if "cf_sales_person" in update_data:
            if isinstance(update_data["cf_sales_person"], str):
                update_data["cf_sales_person"] = [
                    s.strip() for s in update_data["cf_sales_person"].split(",")
                ]

        # ------------------------------------------------------------
        # 2) Check existing record & compare old vs new cf_sales_person
        # ------------------------------------------------------------
        existing_customer = db.customers.find_one({"_id": ObjectId(customer_id)})
        if not existing_customer:
            results.append({"customer_id": customer_id, "status": "not found"})
            continue

        old_cf_sales_person = existing_customer.get("cf_sales_person", [])
        if not isinstance(old_cf_sales_person, list):
            # If old was a string, convert it for consistent comparison
            old_cf_sales_person = [
                s.strip() for s in str(old_cf_sales_person).split(",") if s.strip()
            ]

        new_cf_sales_person = update_data.get("cf_sales_person", old_cf_sales_person)

        # ------------------------------------------------------------
        # 3) Perform the MongoDB update
        # ------------------------------------------------------------
        result = db.customers.update_one(
            {"_id": ObjectId(customer_id)},
            {"$set": update_data},
        )
        if result.matched_count == 0:
            results.append({"customer_id": customer_id, "status": "not found"})
            continue

        updated_count += 1
        results.append(
            {
                "customer_id": customer_id,
                "status": "updated",
                "update_data": update_data,
            }
        )

        # ------------------------------------------------------------
        # 4) Only if new cf_sales_person differs from old, call Zoho
        # ------------------------------------------------------------
        if old_cf_sales_person != new_cf_sales_person:
            payload_zoho = {
                "custom_fields": [
                    {
                        "value": (
                            new_cf_sales_person
                            if new_cf_sales_person and new_cf_sales_person[0] != ""
                            else []
                        ),
                        "customfield_id": "3220178000221198007",
                        "label": "Sales person",
                        "index": 11,
                    }
                ]
            }
            zoho_response = requests.put(
                url=f"https://www.zohoapis.com/books/v3/contacts/{existing_customer.get('contact_id')}?organization_id={org_id}",
                headers={
                    "Authorization": f"Zoho-oauthtoken {get_access_token('books')}"
                },
                json=payload_zoho,
            )
            print(payload_zoho)
            print(zoho_response.json().get("message"))

    return {
        "message": f"Bulk update complete. {updated_count} customers updated.",
        "results": results,
    }


@router.post("/upload-image")
async def upload_image(file: UploadFile = File(...), product_id: str = Form(...)):
    # Validate file type
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Invalid file type.")
    # Validate file size
    file.file.seek(0, 2)  # Move the cursor to the end of the file
    file_size = file.file.tell()  # Get the current position (file size in bytes)
    file.file.seek(0)  # Reset the cursor to the beginning of the file

    if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"File is too large. Maximum allowed size is {MAX_FILE_SIZE_MB} MB.",
        )
    try:
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        # Generate a unique filename
        file_extension = os.path.splitext(file.filename)[1]
        unique_filename = f"product_images/{product.get('item_id')}{file_extension}"

        # Upload the file to S3
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            unique_filename,
            ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
        )

        # Construct the S3 URL
        s3_url = f"{AWS_S3_URL}/{unique_filename}"
        if s3_url:
            products_collection.update_one(
                {"_id": ObjectId(product_id)}, {"$set": {"image_url": s3_url}}
            )
            return {"image_url": s3_url}

    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading file to S3.")
    finally:
        file.file.close()


router.include_router(
    admin_special_margins_router,
    prefix="/customer/special_margins",
    tags=["Admin Sales People"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_salespeople_router,
    prefix="/salespeople",
    tags=["Admin Sales People"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_catalogues_router,
    prefix="/catalogues",
    tags=["Admin Catalogues"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_trainings_router,
    prefix="/trainings",
    tags=["Admin Trainings"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_announcements_router,
    prefix="/announcements",
    tags=["Admin Announcments"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_daily_visits_router,
    prefix="/daily_visits",
    tags=["Admin Daily Visits"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_hooks_router,
    prefix="/hooks",
    tags=["Admin Hooks And Categories"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_hooks_categories_router,
    prefix="/hooks_categories",
    tags=["Admin Hooks And Categories"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_potential_customers_router,
    prefix="/potential_customers",
    tags=["Admin Potential Customers"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_expected_reorders_router,
    prefix="/expected_reorders",
    tags=["Admin Expected Reorders"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_targeted_customers_router,
    prefix="/targeted_customers",
    tags=["Admin Targeted Customers"],
    dependencies=[Depends(JWTBearer())],
)
