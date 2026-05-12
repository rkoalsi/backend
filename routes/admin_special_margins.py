from fastapi import APIRouter, HTTPException, Body, Query
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from pymongo import UpdateOne
from datetime import datetime

db = get_database()

router = APIRouter()


def _archive_margins(docs: list, reason: str):
    """Copy margin docs to special_margin_history before overwriting/deleting."""
    if not docs:
        return
    history_docs = [
        {**{k: v for k, v in doc.items() if k != "_id"},
         "original_id": doc["_id"],
         "archived_at": datetime.now(),
         "reason": reason}
        for doc in docs
    ]
    db.special_margin_history.insert_many(history_docs)


def _get_product_brand(product_id: ObjectId) -> str:
    product = db.products.find_one({"_id": product_id}, {"brand": 1})
    return product.get("brand", "") if product else ""


@router.get("/{customer_id}")
def get_customer_special_margins(customer_id: str):
    """
    Retrieve all special margin products for the given customer.
    """
    pipeline = [
        {"$match": {"customer_id": ObjectId(customer_id)}},
        {
            "$lookup": {
                "from": "products",
                "localField": "product_id",
                "foreignField": "_id",
                "as": "product_info",
            }
        },
        {"$unwind": {"path": "$product_info", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"brand": "$product_info.brand"}},
        {
            "$project": {
                "brand": 1,
                "name": 1,
                "customer_id": 1,
                "margin": 1,
                "product_id": 1,
            }
        },
    ]
    special_margins = list(db.special_margins.aggregate(pipeline))
    return {"products": [serialize_mongo_document(doc) for doc in special_margins]}


@router.post("/bulk/{customer_id}")
def bulk_create_or_update_special_margins(customer_id: str, data: list = Body(...)):
    """
    Create or update multiple special margin entries in bulk for a given customer.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer_id")
        customer_obj_id = ObjectId(customer_id)

        for item in data:
            if not all(k in item for k in ("product_id", "margin")):
                raise HTTPException(
                    status_code=400,
                    detail="Each item must have 'product_id' and 'margin'.",
                )

            if not ObjectId.is_valid(item["product_id"]):
                raise HTTPException(
                    status_code=400, detail=f"Invalid product_id: {item['product_id']}"
                )

            product_obj_id = ObjectId(item["product_id"])
            existing_margin = db.special_margins.find_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id}
            )
            if existing_margin and existing_margin.get("margin") == item["margin"]:
                continue

            if existing_margin:
                _archive_margins([existing_margin], "bulk_update")

            brand = item.get("brand") or _get_product_brand(product_obj_id)
            db.special_margins.update_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id},
                {
                    "$set": {
                        "name": item["name"],
                        "margin": item["margin"],
                        "brand": brand,
                        "customer_id": customer_obj_id,
                        "product_id": product_obj_id,
                        "updated_at": datetime.now(),
                    }
                },
                upsert=True,
            )

        return {"message": "Bulk operation completed successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.put("/{customer_id}/product/{product_id}")
def update_customer_special_margin(
    customer_id: str, product_id: str, data: dict = Body(...)
):
    """
    Update the special margin for a single product for a given customer.
    """
    if not data.get("margin"):
        raise HTTPException(status_code=400, detail="Margin is required.")

    if not ObjectId.is_valid(customer_id) or not ObjectId.is_valid(product_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id or product_id")

    customer_obj_id = ObjectId(customer_id)
    product_obj_id = ObjectId(product_id)

    existing = db.special_margins.find_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id}
    )
    if existing:
        _archive_margins([existing], "manual_edit")

    brand = data.get("brand") or _get_product_brand(product_obj_id)
    update_data = {"margin": data["margin"], "brand": brand, "updated_at": datetime.now()}
    if data.get("name"):
        update_data["name"] = data["name"]

    db.special_margins.update_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id},
        {"$set": update_data},
        upsert=True,
    )
    return {"message": "Special margin updated successfully."}


@router.post("/brand/{customer_id}")
def create_brand_special_margins(customer_id: str, data: dict = Body(...)):
    if not data.get("brand") or not data.get("margin"):
        raise HTTPException(status_code=400, detail="brand and margin are required.")
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    customer_obj_id = ObjectId(customer_id)
    brand = data["brand"]
    margin = data["margin"]

    # Fetch all products for the brand (active and inactive) so orphaned margins are covered.
    products = list(db.products.find({"brand": brand}))
    if not products:
        raise HTTPException(
            status_code=404, detail="No products found for the specified brand."
        )

    product_ids = [p["_id"] for p in products]

    # Archive existing margins for this customer+brand before overwriting.
    existing_docs = list(db.special_margins.find(
        {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
    ))
    _archive_margins(existing_docs, "brand_overwrite")

    # Use upsert per product instead of delete+insert_many so per-product
    # customisations can be compared in history and no documents are lost atomically.
    now = datetime.now()
    for p in products:
        db.special_margins.update_one(
            {"customer_id": customer_obj_id, "product_id": p["_id"]},
            {
                "$set": {
                    "name": p.get("name", "Unnamed"),
                    "margin": margin,
                    "brand": brand,
                    "customer_id": customer_obj_id,
                    "product_id": p["_id"],
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    return {
        "message": f"Special margins updated for {len(products)} products for brand {brand}."
    }


@router.delete("/brand/{customer_id}")
def delete_brand_special_margins(
    customer_id: str,
    brand: str = Query(
        ..., description="The brand name for which to delete special margins"
    ),
):
    """
    Delete all special margin entries for a specific customer and brand.
    """
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    customer_obj_id = ObjectId(customer_id)

    # Query all products for the brand regardless of status so inactive-product
    # margins are also cleaned up.
    products = list(db.products.find({"brand": brand}, {"_id": 1}))
    if not products:
        raise HTTPException(
            status_code=404, detail="No products found for the specified brand."
        )

    product_ids = [p["_id"] for p in products]

    existing_docs = list(db.special_margins.find(
        {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
    ))
    if not existing_docs:
        raise HTTPException(
            status_code=404, detail="No special margins found for the specified brand."
        )

    _archive_margins(existing_docs, "brand_delete")
    result = db.special_margins.delete_many(
        {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
    )

    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s) for brand {brand}."
    }


@router.post("/{customer_id}")
def create_customer_special_margin(customer_id: str, data: dict = Body(...)):
    """
    Create a new special margin entry for a given customer.
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
        return "Product Margin Already Exists"

    product_obj_id = ObjectId(data["product_id"])
    brand = data.get("brand") or _get_product_brand(product_obj_id)

    new_margin = {
        "customer_id": ObjectId(customer_id),
        "product_id": product_obj_id,
        "name": data["name"],
        "margin": data["margin"],
        "brand": brand,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }

    result = db.special_margins.insert_one(new_margin)

    response_margin = {
        "_id": str(result.inserted_id),
        "customer_id": str(customer_id),
        "product_id": str(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
        "brand": brand,
    }
    return {
        "message": "Special margin created successfully.",
        "product": response_margin,
    }


@router.delete("/{customer_id}/bulk")
def delete_all_customer_special_margins(customer_id: str):
    """
    Delete all special margin entries for a specific customer.
    """
    existing_docs = list(db.special_margins.find({"customer_id": ObjectId(customer_id)}))
    if not existing_docs:
        raise HTTPException(
            status_code=404,
            detail="No special margins found for the specified customer or already deleted.",
        )
    _archive_margins(existing_docs, "bulk_delete")
    result = db.special_margins.delete_many({"customer_id": ObjectId(customer_id)})
    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s)."
    }


@router.delete("/{customer_id}/{special_margin_id}")
def delete_customer_special_margin(customer_id: str, special_margin_id: str):
    """
    Delete a specific special margin entry by _id (special_margin_id).
    """
    existing = db.special_margins.find_one(
        {"_id": ObjectId(special_margin_id), "customer_id": ObjectId(customer_id)}
    )
    if not existing:
        raise HTTPException(
            status_code=404, detail="Special margin not found or already deleted."
        )
    _archive_margins([existing], "single_delete")
    db.special_margins.delete_one({"_id": ObjectId(special_margin_id)})
    return {"message": "Special margin deleted successfully."}
