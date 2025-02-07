from fastapi import APIRouter, Body, HTTPException, Query, File, UploadFile, Form
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .helpers import get_access_token
from typing import Optional
import re, requests, os
from collections import defaultdict
from dotenv import load_dotenv
import boto3, datetime
from botocore.exceptions import BotoCoreError, NoCredentialsError
from pytz import timezone
from datetime import date

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


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


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
        orders_draft = db["orders"].count_documents({"status": "draft"})
        orders_accepted = db["orders"].count_documents({"status": "accepted"})
        orders_declined = db["orders"].count_documents({"status": "declined"})
        orders_invoiced = db["orders"].count_documents({"status": "invoiced"})
        ist = timezone("Asia/Kolkata")
        now_ist = datetime.datetime.now(ist)
        start_of_today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        recent_orders = db["orders"].count_documents(
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


@router.get("/products")
def get_products(
    page: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: Optional[str] = None,
    brand: Optional[str] = None,
    # New query params
    status: Optional[str] = None,  # e.g. 'active' or 'inactive'
    stock: Optional[str] = None,  # e.g. 'zero' or 'gt_zero'
    new_arrivals: Optional[bool] = None,
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

            ninty_days_ago = datetime.utcnow() - timedelta(days=90)
            query["created_at"] = {"$gte": ninty_days_ago}

        # 4) Search Filter
        if search and search.strip() != "":
            regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # 5) Brand Filter
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}

        # Pagination
        skip = page * limit

        docs_cursor = (
            products_collection.find(query)
            .sort([("status", 1), ("name", 1)])
            .skip(skip)
            .limit(limit)
        )

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
            query["contact_name"] = re.compile(name, re.IGNORECASE)

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
):
    """
    Retrieve all invoices past their due_date with pagination.
    page:  0-based page index
    limit: number of invoices per page
    """
    # Get today’s date in ISO format (YYYY-MM-DD)
    today_str = date.today().isoformat()

    # Query to match invoices with a due_date less than today
    query = {"due_date": {"$lt": today_str}, "status": {"$nin": ["paid"]}}
    # If you also want to ensure the invoice has a specific status (e.g., "overdue"),
    # you can combine conditions like this:
    # query = {"due_date": {"$lt": today_str}, "status": "overdue"}

    # Basic query stage for the aggregation pipeline
    match_stage = {"$match": query}

    # Count total invoices matching the query (for frontend pagination)
    total_count = db.invoices.count_documents(query)

    # Build the aggregation pipeline
    pipeline = [
        match_stage,
        {"$skip": page * limit},  # Skip the appropriate number of documents
        {"$limit": limit},  # Limit the number of documents returned
        # Project only the necessary fields
        {
            "$project": {
                "created_at": 1,
                "total": 1,
                "due_date": 1,
                "balance": 1,
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
            }
        },
        {"$sort": {"due_date": -1}},  # Sort descending by created_at
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


@router.get("/salespeople")
def home():
    # 1. Fetch all salespeople and gather their codes
    sales_people = list(db.users.find({"role": "sales_person"}))
    sales_people = serialize_mongo_document(sales_people)
    codes = [sp["code"] for sp in sales_people if sp.get("code")]

    # 2. Build a single regex pattern that includes all codes: \b(CODE1|CODE2)\b
    if codes:
        escaped_codes = [re.escape(c) for c in codes]
        combined_pattern = rf"\b({'|'.join(escaped_codes)})\b"
    else:
        combined_pattern = None

    # 3. Single query for all 'active' customers that are either:
    #    - "Defaulter"
    #    - "Company customers"
    #    - or match the combined regex (case-insensitive) in cf_sales_person
    or_conditions = [
        {"cf_sales_person": "Defaulter"},
        {"cf_sales_person": "Company customers"},
    ]
    if combined_pattern:
        or_conditions.append(
            {"cf_sales_person": {"$regex": combined_pattern, "$options": "i"}}
        )

    customers_cursor = db.customers.find({"status": "active", "$or": or_conditions})
    all_customers = serialize_mongo_document(list(customers_cursor))

    # 4. Group customers by salesperson code
    grouped_by_code = defaultdict(list)
    defaulters = []
    company_customers = []

    for cust in all_customers:
        cf_value = cust.get("cf_sales_person")

        # If exactly "Defaulter" or "Company customers", store in special lists
        if cf_value == "Defaulter":
            defaulters.append(cust)
            continue
        elif cf_value == "Company customers":
            company_customers.append(cust)
            continue

        # Otherwise, cf_sales_person could be a string or array
        # Normalize to a list so we can handle both in one pass
        items = cf_value if isinstance(cf_value, list) else [cf_value]

        # Check each item against each code to see if there's a match
        for item in items:
            for code in codes:
                # \b ensures we match code as a separate word or token
                if re.search(rf"\b{re.escape(code)}\b", str(item), re.IGNORECASE):
                    grouped_by_code[code].append(cust)
                    # If a customer can match multiple codes, remove this `break`
                    break

    # 5. Attach customers to each salesperson
    for sp in sales_people:
        code = sp.get("code")
        if code:
            # Their specific matches + universal defaulters + company customers
            sp["customers"] = (
                grouped_by_code.get(code, []) + defaulters + company_customers
            )
        else:
            sp["customers"] = defaulters + company_customers

    return {"users": sales_people}


@router.get("/salespeople/customers")
def get_salespeople_customers():
    users_cursor = db.users.find({"role": "sales_person"})
    users = serialize_mongo_document(list(users_cursor))
    return {"users": users}


@router.post("/salespeople")
async def create_salesperson(salesperson: dict):
    # Check if salesperson code or email already exists
    existing_person = next(
        (
            sp
            for sp in db.users.find({})
            if sp.get("email") == salesperson.get("email")
            or sp.get("code") == salesperson.get("code")
        ),
        None,
    )
    if existing_person:
        raise HTTPException(
            status_code=400,
            detail="Salesperson with this email or code already exists.",
        )

    # Add salesperson to the collection
    db.users.insert_one(salesperson)
    return "Sales Person Created"


@router.get("/salespeople/{salesperson_id}")
def salesperson(salesperson_id: str):
    users_cursor = db.users.find_one({"_id": ObjectId(salesperson_id)})
    sales_person = serialize_mongo_document(dict(users_cursor))

    # Prepare the result
    sales_person_code = sales_person.get("code")

    if sales_person_code:
        escaped_sales_person = re.escape(sales_person_code)
        # Fetch customers assigned to the salesperson
        customers_cursor = db.customers.find(
            {
                "$or": [
                    {
                        "cf_sales_person": {
                            "$regex": f"^{escaped_sales_person}$",
                            "$options": "i",
                        }
                    },
                    {"cf_sales_person": "Defaulter"},
                    {"cf_sales_person": "Company customers"},
                ],
                "status": "active",
            }
        )
        sales_person["customers"] = serialize_mongo_document(list(customers_cursor))
    else:
        # Assign customers with "Defaulter" or "Company customers" to all salespeople
        customers_cursor = db.customers.find(
            {
                "$or": [
                    {"cf_sales_person": "Defaulter"},
                    {"cf_sales_person": "Company customers"},
                ],
                "status": "active",
            }
        )
        sales_person["customers"] = serialize_mongo_document(list(customers_cursor))

    return {"sales_person": sales_person}


@router.put("/salespeople/{salesperson_id}")
def salespeople_id(salesperson_id: str, salesperson: dict):
    update_data = {k: v for k, v in salesperson.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # Perform the update
    result = db.users.update_one(
        {"_id": ObjectId(salesperson_id)},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sales Person not found")
    return {"message": "Sales Person Updated"}


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


@router.get("/customer/special_margins/{customer_id}")
def get_customer_special_margins(customer_id: str):
    """
    Retrieve all special margin products for the given customer.
    """
    special_margins = [
        serialize_mongo_document(doc)
        for doc in db.special_margins.find({"customer_id": ObjectId(customer_id)})
    ]
    # Convert ObjectIds to strings for JSON serializability
    return {"products": special_margins}


@router.post("/customer/special_margins/bulk/{customer_id}")
def bulk_create_or_update_special_margins(customer_id: str, data: list = Body(...)):
    """
    Create or update multiple special margin entries in bulk for a given customer using update_many.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer_id")
        customer_obj_id = ObjectId(customer_id)

        for item in data:
            if not all(k in item for k in ("product_id", "name", "margin")):
                raise HTTPException(
                    status_code=400,
                    detail="Each item must have 'product_id', 'name', and 'margin'.",
                )

            if not ObjectId.is_valid(item["product_id"]):
                raise HTTPException(
                    status_code=400, detail=f"Invalid product_id: {item['product_id']}"
                )

            product_obj_id = ObjectId(item["product_id"])

            # Use update_many to update or insert
            db.special_margins.update_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id},
                {
                    "$set": {
                        "name": item["name"],
                        "margin": item["margin"],
                        "customer_id": customer_obj_id,
                        "product_id": product_obj_id,
                    }
                },
                upsert=True,
            )

        return {"message": "Bulk operation completed successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/customer/special_margins/{customer_id}")
def create_customer_special_margin(customer_id: str, data: dict = Body(...)):
    """
    Create a new special margin entry for a given customer.
    Expects data like:
      {
        "product_id": "XYZ123",
        "name": "Some Product",
        "margin": "50%"
      }
    """
    if not data.get("product_id") or not data.get("name") or not data.get("margin"):
        raise HTTPException(
            status_code=400, detail="product_id, name, and margin are required."
        )
    existing = db.special_margins.find_one(
        {
            "customer_id": ObjectId(customer_id),
            "product_id": ObjectId(data["product_id"]),
        }
    )
    if existing:
        # Already exists -> return 409 conflict
        return "Product Margin Already Exists"

    # Optionally validate that the passed customer_id & product_id are valid ObjectIds
    # if not ObjectId.is_valid(customer_id) or not ObjectId.is_valid(data["product_id"]):
    #     raise HTTPException(status_code=400, detail="Invalid ObjectId")

    # Insert into DB as actual ObjectIds
    new_margin = {
        "customer_id": ObjectId(customer_id),
        "product_id": ObjectId(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
    }

    result = db.special_margins.insert_one(new_margin)

    # Convert for the response
    response_margin = {
        "_id": str(result.inserted_id),
        "customer_id": str(customer_id),
        "product_id": str(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
    }
    return {
        "message": "Special margin created successfully.",
        "product": response_margin,
    }


@router.delete("/customer/special_margins/{customer_id}/bulk")
def delete_all_customer_special_margins(customer_id: str):
    """
    Delete all special margin entries for a specific customer.
    """
    result = db.special_margins.delete_many({"customer_id": ObjectId(customer_id)})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404,
            detail="No special margins found for the specified customer or already deleted.",
        )
    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s)."
    }


@router.delete("/customer/special_margins/{customer_id}/{special_margin_id}")
def delete_customer_special_margin(customer_id: str, special_margin_id: str):
    """
    Delete a specific special margin entry by _id (special_margin_id).
    """
    result = db.special_margins.delete_one(
        {"_id": ObjectId(special_margin_id), "customer_id": ObjectId(customer_id)}
    )
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404, detail="Special margin not found or already deleted."
        )
    return {"message": "Special margin deleted successfully."}


MAX_FILE_SIZE_MB = 10


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
