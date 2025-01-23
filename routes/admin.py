from fastapi import APIRouter, Body, HTTPException, Query, status
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, parse_data, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .helpers import validate_file, process_upload, get_access_token
from typing import Optional
import re, requests, os
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


@router.get("/stats")
async def get_stats():
    try:
        active_products = db["products"].count_documents({"status": "active"})
        active_customers = db["customers"].count_documents({"status": "active"})
        active_sales_people = db["users"].count_documents(
            {"status": "active", "role": "sales_person"}
        )

        orders_draft = db["orders"].count_documents({"status": "draft"})
        orders_accepted = db["orders"].count_documents({"status": "accepted"})
        orders_sent = db["orders"].count_documents({"status": "sent"})
        orders_declined = db["orders"].count_documents({"status": "declined"})

        return {
            "active_products": active_products,
            "active_customers": active_customers,
            "active_sales_people": active_sales_people,
            "orders_draft": orders_draft,
            "orders_sent": orders_sent,
            "orders_accepted": orders_accepted,
            "orders_declined": orders_declined,
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
):
    """
    Retrieve products with optional search and brand filtering.
    Always applies pagination.
    """
    try:
        # Base query: only products with stock > 0 and not marked as deleted
        query = {
            "stock": {"$gt": 0},
            "is_deleted": {"$exists": False},
        }
        # If there's a search string, match name or cf_sku_code (case-insensitive)
        if search and search != "":
            regex = {"$regex": search, "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # If brand filter is applied, add it to the query
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{re.escape(brand)}$", "$options": "i"}

        # Always apply pagination
        skip = page * limit
        docs_cursor = products_collection.find(query).skip(skip).limit(limit)

        # Count how many total match the query
        total_count = products_collection.count_documents(query)

        products = [serialize_mongo_document(doc) for doc in docs_cursor]

        return JSONResponse({"products": products, "total_count": total_count})
    except Exception as e:
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.get("/customers")
def get_customers(
    name: Optional[str] = None,
    page: int = Query(1, ge=1, description="1-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sort: Optional[str] = None,
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

        # Count total matching documents for pagination
        total_count = customers_collection.count_documents(query)

        # Calculate skip based on 1-based indexing
        skip = (page - 1) * limit
        cursor = (
            customers_collection.find(query).sort(sort_order).skip(skip).limit(limit)
        )

        customers = [serialize_mongo_document(doc) for doc in cursor]

        return {"customers": customers, "total_count": total_count}
    except Exception as e:
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.get("/orders")
def read_all_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    """
    Retrieve all orders for admin, with pagination, converting created_at to IST in Mongo.
    page:  0-based page index
    limit: number of orders per page
    """
    # Basic query to match all orders
    match_stage = {"$match": {}}

    # Count total orders (for the frontend) without pagination
    total_count = orders_collection.count_documents({})

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
                # Or keep the entire object if you prefer:
                # "created_by_info": 1
                # but then you'd still need to convert _id to string, if you want
            }
        },
    ]

    # Execute the pipeline
    orders_cursor = orders_collection.aggregate(pipeline)

    # Convert each Mongo document to JSON-serializable Python dict
    orders_with_user_info = [serialize_mongo_document(doc) for doc in orders_cursor]

    return {"orders": orders_with_user_info, "total_count": total_count}


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


@router.get("/salespeople/{salesperson_id}")
def salesperson(salesperson_id: str):
    users_cursor = db.users.find_one({"_id": ObjectId(salesperson_id)})
    sales_person = serialize_mongo_document(dict(users_cursor))

    # Prepare the result
    sales_person_code = sales_person.get("code")

    if sales_person_code:
        # Fetch customers assigned to the salesperson
        customers_cursor = db.customers.find(
            {
                "$or": [
                    {
                        "cf_sales_person": {
                            "$regex": f"\\b{sales_person_code}\\b",
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
