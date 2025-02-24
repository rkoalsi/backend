from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo import ASCENDING, DESCENDING
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional
import os, json

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


@router.get("/counts")
def get_product_counts():
    """
    Returns the number of active products grouped by brand and category.
    """
    try:
        base_query = {
            "stock": {"$gt": 0},
            "is_deleted": {"$exists": False},
            "status": "active",
        }

        pipeline = [
            {"$match": base_query},
            {
                "$group": {
                    "_id": {"brand": "$brand", "category": "$category"},
                    "count": {"$sum": 1},
                }
            },
        ]

        counts = list(db.products.aggregate(pipeline))
        # Format counts into a nested dict: { brand: { category: count, ... }, ... }
        result = {}
        for item in counts:
            group_id = item["_id"]
            # Skip if either 'brand' or 'category' is missing or null
            if not group_id.get("brand") or not group_id.get("category"):
                continue
            brand = group_id["brand"]
            category = group_id["category"]
            if brand not in result:
                result[brand] = {}
            result[brand][category] = item["count"]

        return result

    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="Failed to get product counts")


@router.get("/brands")
def get_all_brands():
    """
    Retrieve a list of all distinct brands.
    """
    try:
        brands = products_collection.distinct(
            "brand", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        brands = [
            {
                "brand": brand,
                "url": f"""{os.getenv('S3_URL')}/product_images/{(brand.lower().replace(" ", "_") if len(brand.split()) == 2 else brand.lower())}.jpeg""",
            }
            for brand in brands
            if brand
        ]  # Remove empty or null brands

        return {"brands": brands}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.get("/categories", response_model=dict)
def get_categories_for_brand(brand: str):
    """
    Retrieve a list of all distinct categories for a given brand.

    - **brand**: The name of the brand to fetch categories for.
    """
    try:
        # Fetch distinct categories for the specified brand
        categories = products_collection.distinct(
            "category",
            {"brand": brand, "stock": {"$gt": 0}, "is_deleted": {"$exists": False}},
        )

        # Remove empty or null categories
        categories = [category for category in categories if category]

        return {"categories": categories}
    except Exception as e:
        # Log the exception details (ensure logging is set up in your application)
        print(f"Error fetching categories for brand '{brand}': {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch categories.")


@router.get("")
def get_products(
    role: str = "salesperson",
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(25, ge=1, le=100, description="Number of items per page"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search term for name or SKU code"),
    sort: Optional[str] = Query(
        "default", description="Sort order: default, price_asc, price_desc"
    ),
):
    """
    Retrieves paginated products with optional brand, category, and search filters.
    The default sort shows new products first within each brand.
    Additional sort orders by price (ascending or descending) are supported.
    """
    # Define base query
    query = {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}

    # Add brand filter
    if brand:
        query["brand"] = brand

    # Add category filter
    if category:
        query["category"] = category  # Adjust if 'category' is nested

    # Add search filter
    if search:
        regex = {"$regex": search, "$options": "i"}  # Case-insensitive search
        query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

    # Add additional condition for salespeople
    if role == "salesperson":
        query["status"] = "active"

    # Define the threshold date (three months ago)
    three_months_ago = datetime.now() - relativedelta(months=3)

    # Compute sort stage based on sort parameter
    if sort == "price_asc":
        # Sort by price ascending; you can add additional tie-breakers if needed
        sort_stage = {"rate": ASCENDING}
    elif sort == "price_desc":
        # Sort by price descending
        sort_stage = {"rate": DESCENDING}
    elif sort == "catalogue":
        # Only include documents that have a non-null catalogue_order.
        if not search:
            query["catalogue_order"] = {"$exists": True, "$ne": None}
        sort_stage = {"catalogue_order": ASCENDING}
    else:
        # Default sort: new products first (within each brand) and then by other fields
        sort_stage = {
            "brand": ASCENDING,
            "new": DESCENDING,  # New products first within each brand
            "category": ASCENDING,
            "sub_category": ASCENDING,
            "series": ASCENDING,
            "rate": ASCENDING,
            "name": ASCENDING,
        }
    # Aggregation Pipeline
    pipeline = [
        {"$match": query},
        {
            "$addFields": {
                "new": {
                    "$cond": [
                        {"$gte": ["$created_at", three_months_ago]},
                        True,
                        False,
                    ]
                }
            }
        },
        {"$sort": sort_stage},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]

    # Execute Aggregation Pipeline
    try:
        fetched_products = list(db.products.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    # Serialize products
    all_products = [serialize_mongo_document(doc) for doc in fetched_products]
    print(json.dumps(all_products, indent=4))

    # Calculate total products matching the query
    try:
        total_products = db.products.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Calculate total pages
    total_pages = (
        ((total_products + per_page - 1) // per_page) if total_products > 0 else 1
    )

    # Validate page number
    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "products": all_products,
        "total": total_products,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "brand": brand,
        "category": category,
        "search": search,
    }


@router.get("/all_categories", response_model=dict)
def get_all_product_categories():
    """
    Retrieve a sorted list of all distinct product categories (across all brands).
    Only categories for products with stock > 0 and not marked as deleted are returned.
    """
    try:
        # Use distinct to fetch unique categories across all products meeting the criteria
        categories = products_collection.distinct(
            "category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        # Filter out any empty or null values
        categories = [category for category in categories if category]
        # Sort the categories alphabetically
        return {"categories": sorted(categories)}
    except Exception as e:
        print(f"Error fetching all product categories: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch all product categories."
        )


@router.get("/{product_id}")
def get_product_by_id(product_id: str):
    """
    Retrieve an product by its ID.
    """
    product = get_product(product_id, products_collection)
    if not product:
        raise HTTPException(status_code=404, detail="Order not found")
    return product


@router.put("/{product_id}")
async def update_product(product_id: str, product: dict):
    # Ensure '_id' is not in the update data
    update_data = {k: v for k, v in product.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # Perform the update
    result = products_collection.update_one(
        {"_id": ObjectId(product_id)},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product updated"}


@router.delete("/{product_id}")
async def delete_product(product_id: str):
    result = products_collection.update_one(
        {"_id": ObjectId(product_id)}, {"$set": {"is_deleted": True}}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product deleted"}


# @router.get("/", response_class=HTMLResponse)
# def index():
#     return "<h1>Backend is running<h1>"
