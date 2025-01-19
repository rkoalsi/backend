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
def get_products():
    products = [
        serialize_mongo_document(
            {
                **doc,
            }
        )
        for doc in db.products.find({"status": "active", "stock": {"$gt": 0}})
    ]

    return {"products": products}
