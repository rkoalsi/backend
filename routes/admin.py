from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, parse_data, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .helpers import validate_file, process_upload, get_access_token
from typing import Optional
import re
from datetime import datetime
import pytz

router = APIRouter()

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
        orders_declined = db["orders"].count_documents({"status": "declined"})

        return {
            "active_products": active_products,
            "active_customers": active_customers,
            "active_sales_people": active_sales_people,
            "orders_draft": orders_draft,
            "orders_accepted": orders_accepted,
            "orders_declined": orders_declined,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products")
def get_products(
    page: int = Query(0, ge=0),  # 0-based page index
    limit: int = Query(10, ge=1),  # number of items per page
):
    """
    Returns a paginated list of products.

    :param page:   The page index (0-based).
    :param limit:  Number of documents to return per page.
    """
    query = {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}

    # Get the total count of matching documents
    total_count = products_collection.count_documents(query)

    # Fetch only the page of data we care about
    # skip = page * limit  (0-based page index)
    products_cursor = products_collection.find(query).skip(page * limit).limit(limit)

    products = [serialize_mongo_document(doc) for doc in products_cursor]

    return JSONResponse({"products": products, "total_count": total_count})


@router.get("/customers")
def get_customers(
    name: Optional[str] = None,
    # Remove salesperson logic
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sort: Optional[str] = None,
):
    """
    Returns a paginated list of customers, optionally filtered by name.
    Sorting is optional:
      - Default sort is ascending by 'status'
      - If ?sort=desc, it will sort descending by 'status'
    """
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

    # Use skip/limit for server-side pagination
    cursor = (
        customers_collection.find(query)
        .sort(sort_order)
        .skip(page * limit)
        .limit(limit)
    )

    customers = [serialize_mongo_document(doc) for doc in cursor]

    return {"customers": customers, "total_count": total_count}


IST = pytz.timezone("Asia/Kolkata")  # Define the IST timezone


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
    users_cursor = db.users.find({"role": "sales_person"})
    sales_people = serialize_mongo_document(list(users_cursor))

    # Prepare the result
    for sales_person in sales_people:
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
