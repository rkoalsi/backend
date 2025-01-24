from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, parse_data, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .helpers import validate_file, process_upload, get_access_token
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


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


@router.get("")
def get_products(
    role: str = "salesperson",
    page: int = Query(1, ge=1),  # Page number, default to 1
    per_page: int = Query(25, ge=1, le=100),  # Items per page, default to 25
    brand: str = Query(None),  # Optional brand filter
):
    """
    Retrieves paginated products for a specific brand or all brands.

    Args:
        role (str): Role of the requester. Defaults to "salesperson".
        page (int): Page number for pagination.
        per_page (int): Number of products per page.
        brand (str): Optional brand filter.

    Returns:
        dict: A dictionary containing paginated list of products.
    """
    # Define base query
    query = {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}

    # Add brand filter
    if brand:
        query["brand"] = brand

    # Add additional condition for salespeople
    if role == "salesperson":
        query["status"] = "active"

    # Fetch products based on the constructed query
    all_products = [serialize_mongo_document(doc) for doc in db.products.find(query)]

    # Define the threshold date (three months ago)
    three_months_ago = datetime.now() - relativedelta(months=3)

    # Mark new products
    for product in all_products:
        created_at_str = product.get("created_at")
        if created_at_str:
            try:
                created_at = parser.parse(created_at_str)
                product["new"] = created_at >= three_months_ago
            except Exception as e:
                print(
                    f"Error parsing created_at for item_id {product.get('item_id')}: {e}"
                )
                product["new"] = False

    # Sort the products: new products first
    sorted_products = sorted(
        all_products, key=lambda x: x.get("new", False), reverse=True
    )

    # Pagination logic
    total_products = len(sorted_products)
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    paginated_products = sorted_products[start_index:end_index]

    return {
        "products": paginated_products,
        "total": total_products,
        "page": page,
        "per_page": per_page,
        "total_pages": (total_products + per_page - 1) // per_page,
        "brand": brand,
    }


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
