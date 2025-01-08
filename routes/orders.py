from pymongo.collection import Collection
from datetime import datetime
from typing import List
from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo  # type: ignore
from bson.objectid import ObjectId


def serialize_mongo_document(document):
    """
    Recursively convert MongoDB ObjectId fields to strings in a document.
    """
    if isinstance(document, list):
        return [serialize_mongo_document(item) for item in document]
    elif isinstance(document, dict):
        return {key: serialize_mongo_document(value) for key, value in document.items()}
    elif isinstance(document, ObjectId):
        return str(document)
    else:
        return document


# Connect to MongoDB
client, db = connect_to_mongo()
orders_collection = db["orders"]
customers_collection = db["customers"]

router = APIRouter()


# Create a new order
def create_order(order: dict, collection: Collection) -> str:
    # Explicitly convert customer_id and product_ids to ObjectId
    customer_id = order.get("customer_id", "")
    products = order.get("products", [])
    if customer_id:
        order["customer_id"] = ObjectId(order.get("customer_id", ""))

    if len(products) > 0:
        order["products"] = [
            {"product_id": ObjectId(item["product_id"]), "quantity": item["quantity"]}
            for item in products
        ]
    order["created_by"] = ObjectId(order.get("created_by", ""))
    order["created_at"] = datetime.utcnow()
    order["updated_at"] = datetime.utcnow()

    # Insert the document into MongoDB
    result = collection.insert_one(order)
    return str(result.inserted_id)


def check_if_order_exists(
    created_by: str, orders_collection: Collection
) -> dict | bool:
    order = orders_collection.find_one(
        {"created_by": ObjectId(created_by), "status": "draft"}
    )
    if order:
        return order
    else:
        return False


# Get an order by ID and populate customer and product details
def get_order(
    order_id: str,
    orders_collection: Collection,
    customers_collection: Collection,
    products_collection: Collection,
):
    result = orders_collection.find_one({"_id": ObjectId(order_id)})
    if result:
        order = result
        order["status"] = str(order["status"]).capitalize()
        return serialize_mongo_document(order)
    return None


# Get all orders
def get_all_orders(created_by: str, collection: Collection):
    query = {}
    if created_by:
        query["created_by"] = ObjectId(created_by)
    print(query)
    return [
        serialize_mongo_document(
            {
                **doc,
            }
        )
        for doc in collection.find(query)
    ]


# Update an order
from bson.objectid import ObjectId
from pymongo.collection import Collection
from datetime import datetime


def update_order(
    order_id: str,
    order_update: dict,
    order_collection: Collection,
    customer_collection: Collection,
):
    order_update["updated_at"] = datetime.utcnow()

    # Handle customer updates
    if "customer_id" in order_update:
        customer_id = order_update.get("customer_id")
        customer = customer_collection.find_one({"_id": ObjectId(customer_id)})

        if customer:
            order_update["customer_id"] = ObjectId(customer_id)
            order_update["customer_name"] = customer.get("company_name")
            order_update["gst_type"] = customer.get("cf_in_ex")

    # Handle product updates
    if "products" in order_update:
        updated_products = []
        existing_order = order_collection.find_one({"_id": ObjectId(order_id)})
        existing_products = existing_order.get("products", []) if existing_order else []
        # Build a map of existing products for quick lookup
        existing_product_map = {
            str(product["product_id"]): product for product in existing_products
        }

        for product in order_update.get("products", []):
            product_id = (
                ObjectId(product["_id"]["$oid"])
                if isinstance(product["_id"], dict) and "$oid" in product["_id"]
                else ObjectId(product["product_id"])
            )

            # If product exists, update its quantity
            if str(product_id) in existing_product_map:
                existing_product_map[str(product_id)]["quantity"] = product.get(
                    "quantity", existing_product_map[str(product_id)].get("quantity", 1)
                )
            else:
                # Add new product
                updated_products.append(
                    {
                        "product_id": product_id,
                        "price": product.get("rate"),
                        "tax_percentage": (
                            product.get("item_tax_preferences", [{}])[0].get(
                                "tax_percentage", 0
                            )
                        ),
                        "brand": product.get("brand", ""),
                        "product_code": product.get("cf_sku_code", ""),
                        "quantity": product.get("quantity", 1),
                    }
                )

        # Combine updated quantities with new products
        final_products = list(existing_product_map.values()) + updated_products

        order_update["products"] = final_products

    # Perform the update in MongoDB
    order_collection.update_one({"_id": ObjectId(order_id)}, {"$set": order_update})


# Delete an order
def delete_order(order_id: str, collection: Collection):
    collection.delete_one({"_id": ObjectId(order_id)})


# API Endpoints


# Create a new order
@router.post("")
def create_new_order(order: dict):
    """
    Create a new order with raw dictionary data.
    """
    try:
        order_id = create_order(order, orders_collection)
        order["_id"] = order_id  # Add the generated ID back to the response
        return serialize_mongo_document(order)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/check")
def check_order_status(order: dict):
    """
    Create a new order with raw dictionary data.
    """
    try:
        created_by = order.get("created_by", "")
        if not created_by:
            raise HTTPException(status_code=400, detail="created_by is required")
        order = check_if_order_exists(created_by, orders_collection)
        if order:
            return {
                **serialize_mongo_document(order),
                "message": "Existing Draft Order Found",
                "can_create": False,
            }
        else:
            return {"message": "Existing Draft Order Not Found", "can_create": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Get an order by ID
@router.get("/{order_id}")
def read_order(order_id: str):
    """
    Retrieve an order by its ID.
    """
    order = get_order(order_id, orders_collection, db["customers"], db["products"])
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


# Get all orders
@router.get("")
def read_all_orders(created_by=""):
    """
    Retrieve all orders.
    """
    orders = get_all_orders(created_by, orders_collection)
    return orders


# Update an order
@router.put("/{order_id}")
def update_existing_order(order_id: str, order_update: dict):
    """
    Update an existing order with raw dictionary data.
    """
    update_order(order_id, order_update, orders_collection, customers_collection)
    updated_order = get_order(
        order_id, orders_collection, db["customers"], db["products"]
    )
    if not updated_order:
        raise HTTPException(status_code=404, detail="Order not found")
    return updated_order


# Delete an order
@router.delete("/{order_id}")
def delete_existing_order(order_id: str):
    """
    Delete an order by its ID.
    """
    delete_order(order_id, orders_collection)
    return {"detail": "Order deleted successfully"}
