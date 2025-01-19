from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, parse_data, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .helpers import validate_file, process_upload, get_access_token

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


@router.get("")
def get_products(role: str = "salesperson"):
    # Define base query
    query = {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}

    # Add additional condition for salespeople
    if role == "salesperson":
        query["status"] = "active"

    # Fetch products based on the constructed query
    products = [serialize_mongo_document({**doc}) for doc in db.products.find(query)]

    return {"products": products}


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
