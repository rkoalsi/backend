from fastapi import APIRouter, HTTPException, Body, Query
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo import UpdateOne, ASCENDING
from datetime import datetime
from typing import List, Dict, Any, Optional

client, db = connect_to_mongo()
router = APIRouter()


def create_audit_log(
    action: str,
    customer_id: str,
    details: Dict[str, Any],
    user_id: Optional[str] = None,
):
    """Create an audit log entry for margin changes"""
    audit_entry = {
        "action": action,
        "customer_id": ObjectId(customer_id),
        "details": details,
        "user_id": user_id,
        "timestamp": datetime.now(),
    }
    db.margin_audit_log.insert_one(audit_entry)
    print(f"Margin change: {action} for customer {customer_id}: {details}")


def validate_margin_format(margin: str) -> bool:
    """Validate margin format (should be like '50%' or '50.5%')"""
    if not margin or not margin.endswith("%"):
        return False
    try:
        float(margin[:-1])
        return True
    except ValueError:
        return False


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
                "created_at": 1,
                "updated_at": 1,
                "margin_type": 1,  # Add field to track if it's individual or brand-level
            }
        },
    ]
    special_margins = list(db.special_margins.aggregate(pipeline))
    return {"products": [serialize_mongo_document(doc) for doc in special_margins]}


@router.post("/bulk/{customer_id}")
def bulk_create_or_update_special_margins(
    customer_id: str, data: List[Dict[str, Any]] = Body(...)
):
    """
    Create or update multiple special margin entries in bulk for a given customer.
    Uses proper bulk operations to prevent race conditions.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    customer_obj_id = ObjectId(customer_id)

    # Validate all items first
    bulk_operations = []
    audit_details = []

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

        if not validate_margin_format(item["margin"]):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid margin format: {item['margin']}. Use format like '50%'",
            )

        product_obj_id = ObjectId(item["product_id"])

        # Check existing margin for audit trail
        existing = db.special_margins.find_one(
            {"customer_id": customer_obj_id, "product_id": product_obj_id}
        )

        update_doc = {
            "name": item["name"],
            "margin": item["margin"],
            "customer_id": customer_obj_id,
            "product_id": product_obj_id,
            "updated_at": datetime.now(),
            "margin_type": "individual",  # Mark as individual margin
        }

        if not existing:
            update_doc["created_at"] = datetime.now()

        bulk_operations.append(
            UpdateOne(
                {"customer_id": customer_obj_id, "product_id": product_obj_id},
                {"$set": update_doc},
                upsert=True,
            )
        )

        # Prepare audit details
        audit_details.append(
            {
                "product_id": ObjectId(product_obj_id),
                "product_name": item["name"],
                "old_margin": existing["margin"] if existing else None,
                "new_margin": item["margin"],
                "action_type": "update" if existing else "create",
            }
        )

    try:
        # Execute bulk operation atomically
        if bulk_operations:
            result = db.special_margins.bulk_write(bulk_operations, ordered=False)

            # Create audit log
            create_audit_log(
                action="bulk_update",
                customer_id=customer_id,
                details={
                    "items_processed": len(bulk_operations),
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_count": result.upserted_count,
                    "changes": audit_details,
                },
            )

        return {
            "message": "Bulk operation completed successfully.",
            "processed": len(bulk_operations),
            "modified": result.modified_count if bulk_operations else 0,
            "created": result.upserted_count if bulk_operations else 0,
        }

    except Exception as e:
        print(f"Bulk update failed for customer {customer_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/brand/{customer_id}")
def create_brand_special_margins(
    customer_id: str,
    data: Dict[str, Any] = Body(...),
    preserve_individual: bool = Query(
        False, description="Preserve existing individual product margins"
    ),
):
    """
    Create brand-level special margins with option to preserve individual margins.
    """
    if not data.get("brand") or not data.get("margin"):
        raise HTTPException(status_code=400, detail="brand and margin are required.")
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    if not validate_margin_format(data["margin"]):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid margin format: {data['margin']}. Use format like '50%'",
        )

    customer_obj_id = ObjectId(customer_id)
    brand = data["brand"]
    margin = data["margin"]

    # Fetch all active products for the given brand
    products = list(db.products.find({"brand": brand, "status": "active"}))
    if not products:
        raise HTTPException(
            status_code=404, detail="No products found for the specified brand."
        )

    product_ids = [p["_id"] for p in products]

    # Get existing margins for audit trail
    existing_margins = list(
        db.special_margins.find(
            {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
        )
    )

    bulk_operations = []

    if preserve_individual:
        # Only update products that don't have individual margins
        existing_product_ids = {
            m["product_id"]
            for m in existing_margins
            if m.get("margin_type") == "individual"
        }
        products_to_update = [
            p for p in products if p["_id"] not in existing_product_ids
        ]
    else:
        # Update all products (original behavior but with audit trail)
        products_to_update = products
        # Remove existing margins
        db.special_margins.delete_many(
            {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
        )

    # Build bulk operations
    for product in products_to_update:
        bulk_operations.append(
            UpdateOne(
                {"customer_id": customer_obj_id, "product_id": product["_id"]},
                {
                    "$set": {
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                        "customer_id": customer_obj_id,
                        "product_id": product["_id"],
                        "name": product.get("name", "Unnamed"),
                        "margin": margin,
                        "margin_type": "brand",  # Mark as brand-level margin
                    }
                },
                upsert=True,
            )
        )

    # Execute bulk operation
    if bulk_operations:
        result = db.special_margins.bulk_write(bulk_operations, ordered=False)

    # Create audit log
    create_audit_log(
        action="brand_margin_update",
        customer_id=customer_id,
        details={
            "brand": brand,
            "margin": margin,
            "products_affected": len(products_to_update),
            "preserve_individual": preserve_individual,
            "existing_margins_count": len(existing_margins),
        },
    )

    return {
        "message": f"Special margins updated for {len(products_to_update)} products for brand {brand}.",
        "products_affected": len(products_to_update),
        "individual_margins_preserved": (
            len(products) - len(products_to_update) if preserve_individual else 0
        ),
    }


@router.put("/{customer_id}/product/{product_id}")
def update_customer_special_margin(
    customer_id: str, product_id: str, data: Dict[str, Any] = Body(...)
):
    """
    Update the special margin for a single product for a given customer.
    """
    if not data.get("margin"):
        raise HTTPException(status_code=400, detail="Margin is required.")

    if not ObjectId.is_valid(customer_id) or not ObjectId.is_valid(product_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id or product_id")

    if not validate_margin_format(data["margin"]):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid margin format: {data['margin']}. Use format like '50%'",
        )

    customer_obj_id = ObjectId(customer_id)
    product_obj_id = ObjectId(product_id)

    # Get existing margin for audit trail
    existing = db.special_margins.find_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id}
    )

    update_data = {
        "margin": data["margin"],
        "updated_at": datetime.now(),
        "margin_type": "individual",  # Mark as individual override
    }
    if data.get("name"):
        update_data["name"] = data["name"]
    if not existing:
        update_data["created_at"] = datetime.now()

    result = db.special_margins.update_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id},
        {"$set": update_data},
        upsert=True,
    )

    # Create audit log
    create_audit_log(
        action="individual_margin_update",
        customer_id=customer_id,
        details={
            "product_id": product_id,
            "product_name": data.get("name"),
            "old_margin": existing["margin"] if existing else None,
            "new_margin": data["margin"],
        },
    )

    return {"message": "Special margin updated successfully."}


@router.get("/{customer_id}/audit")
def get_margin_audit_trail(
    customer_id: str,
    limit: int = Query(50, description="Number of records to return"),
    skip: int = Query(0, description="Number of records to skip"),
):
    """
    Get audit trail for margin changes for a specific customer.
    """
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    # Create index for better performance
    db.margin_audit_log.create_index([("customer_id", ASCENDING), ("timestamp", -1)])

    audit_logs = list(
        db.margin_audit_log.find({"customer_id": ObjectId(customer_id)})
        .sort("timestamp", -1)
        .skip(skip)
        .limit(limit)
    )

    return {
        "audit_logs": [serialize_mongo_document(log) for log in audit_logs],
        "total_returned": len(audit_logs),
    }


# Keep your existing DELETE endpoints but add audit logging to them as well
@router.delete("/{customer_id}/bulk")
def delete_all_customer_special_margins(customer_id: str):
    """Delete all special margin entries for a specific customer."""
    # Get count before deletion for audit
    count_before = db.special_margins.count_documents(
        {"customer_id": ObjectId(customer_id)}
    )

    result = db.special_margins.delete_many({"customer_id": ObjectId(customer_id)})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404,
            detail="No special margins found for the specified customer or already deleted.",
        )

    # Create audit log
    create_audit_log(
        action="bulk_delete_all",
        customer_id=customer_id,
        details={"deleted_count": result.deleted_count},
    )

    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s)."
    }
