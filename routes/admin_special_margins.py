from fastapi import APIRouter, HTTPException, Body, Query, Request
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from pymongo import UpdateOne
from datetime import datetime
from typing import Optional, Dict, Any
import copy

client, db = connect_to_mongo()

router = APIRouter()


def create_audit_entry(
    action: str,
    document_id: ObjectId,
    customer_id: ObjectId,
    old_data: Optional[Dict[Any, Any]] = None,
    new_data: Optional[Dict[Any, Any]] = None,
    user_id: Optional[str] = None,
    additional_info: Optional[Dict[Any, Any]] = None,
):
    """
    Create an audit trail entry for special margins operations.

    Args:
        action: Type of action (CREATE, UPDATE, DELETE, BULK_CREATE, BULK_UPDATE, BULK_DELETE)
        document_id: ID of the special margin document
        customer_id: Customer ID
        old_data: Previous state of the document (for updates/deletes)
        new_data: New state of the document (for creates/updates)
        user_id: ID of the user performing the action (optional)
        additional_info: Any additional context (e.g., brand operations, bulk operation details)
    """
    audit_entry = {
        "timestamp": datetime.now(),
        "action": action,
        "collection": "special_margins",
        "document_id": document_id,
        "customer_id": customer_id,
        "old_data": old_data,
        "new_data": new_data,
        "user_id": user_id,
        "additional_info": additional_info or {},
    }

    # Insert audit entry
    db.special_margins_audit.insert_one(audit_entry)


def get_current_timestamp():
    """Get current timestamp for created_at and updated_at fields."""
    return datetime.now()


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
            }
        },
    ]
    special_margins = list(db.special_margins.aggregate(pipeline))
    return {"products": [serialize_mongo_document(doc) for doc in special_margins]}


@router.post("/bulk/{customer_id}")
def bulk_create_or_update_special_margins(
    customer_id: str, data: list = Body(...), request: Request = None
):
    """
    Create or update multiple special margin entries in bulk for a given customer.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer_id")
        customer_obj_id = ObjectId(customer_id)

        # Get user_id from request headers if available
        user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

        bulk_operations_info = []
        current_time = get_current_timestamp()

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

            # Check if document exists to determine if it's create or update
            existing_doc = db.special_margins.find_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id}
            )

            new_data = {
                "name": item["name"],
                "margin": item["margin"],
                "customer_id": customer_obj_id,
                "product_id": product_obj_id,
                "updated_at": current_time,
            }

            if not existing_doc:
                # Creating new document
                new_data["created_at"] = current_time
                action = "BULK_CREATE"
                old_data = None
            else:
                # Updating existing document
                action = "BULK_UPDATE"
                old_data = copy.deepcopy(existing_doc)
                # Remove MongoDB internal fields for cleaner audit
                old_data.pop("_id", None)

            # Perform the upsert
            result = db.special_margins.update_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id},
                {"$set": new_data},
                upsert=True,
            )

            # Get the document ID for audit trail
            if result.upserted_id:
                doc_id = result.upserted_id
            else:
                doc = db.special_margins.find_one(
                    {"customer_id": customer_obj_id, "product_id": product_obj_id}
                )
                doc_id = doc["_id"]

            # Create audit entry
            create_audit_entry(
                action=action,
                document_id=doc_id,
                customer_id=customer_obj_id,
                old_data=old_data,
                new_data=copy.deepcopy(new_data),
                user_id=user_id,
                additional_info={
                    "bulk_operation": True,
                    "item_index": data.index(item),
                },
            )

            bulk_operations_info.append(
                {
                    "product_id": str(product_obj_id),
                    "action": action,
                    "document_id": str(doc_id),
                }
            )

        return {
            "message": "Bulk operation completed successfully.",
            "operations": bulk_operations_info,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.put("/{customer_id}/product/{product_id}")
def update_customer_special_margin(
    customer_id: str, product_id: str, data: dict = Body(...), request: Request = None
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
    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

    # Get existing document for audit trail
    existing_doc = db.special_margins.find_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id}
    )

    current_time = get_current_timestamp()
    update_data = {"margin": data["margin"], "updated_at": current_time}

    if data.get("name"):
        update_data["name"] = data["name"]

    # If creating new document, add created_at
    if not existing_doc:
        update_data["created_at"] = current_time
        update_data["customer_id"] = customer_obj_id
        update_data["product_id"] = product_obj_id

    result = db.special_margins.update_one(
        {"customer_id": customer_obj_id, "product_id": product_obj_id},
        {"$set": update_data},
        upsert=True,
    )

    # Get document ID for audit
    if result.upserted_id:
        doc_id = result.upserted_id
        action = "CREATE"
        old_data = None
    else:
        doc = db.special_margins.find_one(
            {"customer_id": customer_obj_id, "product_id": product_obj_id}
        )
        doc_id = doc["_id"]
        action = "UPDATE"
        old_data = copy.deepcopy(existing_doc)
        old_data.pop("_id", None)

    # Create audit entry
    create_audit_entry(
        action=action,
        document_id=doc_id,
        customer_id=customer_obj_id,
        old_data=old_data,
        new_data=copy.deepcopy(update_data),
        user_id=user_id,
    )

    return {"message": "Special margin updated successfully."}


@router.post("/brand/{customer_id}")
def create_brand_special_margins(
    customer_id: str, data: dict = Body(...), request: Request = None
):
    """
    Create special margins for all products of a specific brand for a customer.
    """
    if not data.get("brand") or not data.get("margin"):
        raise HTTPException(status_code=400, detail="brand and margin are required.")
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    customer_obj_id = ObjectId(customer_id)
    brand = data["brand"]
    margin = data["margin"]
    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

    # Fetch all active products for the given brand
    products = list(db.products.find({"brand": brand, "status": "active"}))
    if not products:
        raise HTTPException(
            status_code=404, detail="No products found for the specified brand."
        )

    # Get existing special margins for audit trail
    product_ids = [p["_id"] for p in products]
    existing_margins = list(
        db.special_margins.find(
            {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
        )
    )

    # Create audit entries for deletions
    for existing in existing_margins:
        create_audit_entry(
            action="DELETE",
            document_id=existing["_id"],
            customer_id=customer_obj_id,
            old_data={k: v for k, v in existing.items() if k != "_id"},
            new_data=None,
            user_id=user_id,
            additional_info={
                "brand_operation": True,
                "brand": brand,
                "reason": "brand_margin_reset",
            },
        )

    # Remove existing special margins for this customer and brand
    db.special_margins.delete_many(
        {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
    )

    # Build new special margin documents
    current_time = get_current_timestamp()
    new_docs = [
        {
            "created_at": current_time,
            "updated_at": current_time,
            "customer_id": customer_obj_id,
            "product_id": p["_id"],
            "name": p.get("name", "Unnamed"),
            "margin": margin,
        }
        for p in products
    ]

    # Insert all new documents
    if new_docs:
        result = db.special_margins.insert_many(new_docs)

        # Create audit entries for new creations
        for i, doc in enumerate(new_docs):
            create_audit_entry(
                action="CREATE",
                document_id=result.inserted_ids[i],
                customer_id=customer_obj_id,
                old_data=None,
                new_data=copy.deepcopy(doc),
                user_id=user_id,
                additional_info={"brand_operation": True, "brand": brand},
            )

    return {
        "message": f"Special margins updated for {len(new_docs)} products for brand {brand}."
    }


@router.delete("/brand/{customer_id}")
def delete_brand_special_margins(
    customer_id: str,
    brand: str = Query(
        ..., description="The brand name for which to delete special margins"
    ),
    request: Request = None,
):
    """
    Delete all special margin entries for a specific customer and brand.
    """
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    customer_obj_id = ObjectId(customer_id)
    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

    # Fetch all active products for the given brand
    products = list(db.products.find({"brand": brand, "status": "active"}))
    if not products:
        raise HTTPException(
            status_code=404, detail="No products found for the specified brand."
        )

    product_ids = [p["_id"] for p in products]

    # Get existing documents for audit trail
    existing_margins = list(
        db.special_margins.find(
            {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
        )
    )

    if not existing_margins:
        raise HTTPException(
            status_code=404, detail="No special margins found for the specified brand."
        )

    # Create audit entries for deletions
    for margin in existing_margins:
        create_audit_entry(
            action="DELETE",
            document_id=margin["_id"],
            customer_id=customer_obj_id,
            old_data={k: v for k, v in margin.items() if k != "_id"},
            new_data=None,
            user_id=user_id,
            additional_info={"brand_operation": True, "brand": brand},
        )

    result = db.special_margins.delete_many(
        {"customer_id": customer_obj_id, "product_id": {"$in": product_ids}}
    )

    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s) for brand {brand}."
    }


@router.post("/{customer_id}")
def create_customer_special_margin(
    customer_id: str, data: dict = Body(...), request: Request = None
):
    """
    Create a new special margin entry for a given customer.
    """
    if not data.get("product_id") or not data.get("name") or not data.get("margin"):
        raise HTTPException(
            status_code=400, detail="product_id, name, and margin are required."
        )

    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

    existing = db.special_margins.find_one(
        {
            "customer_id": ObjectId(customer_id),
            "product_id": ObjectId(data["product_id"]),
        }
    )
    if existing:
        return "Product Margin Already Exists"

    current_time = get_current_timestamp()

    # Insert into DB
    new_margin = {
        "customer_id": ObjectId(customer_id),
        "product_id": ObjectId(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
        "created_at": current_time,
        "updated_at": current_time,
    }

    result = db.special_margins.insert_one(new_margin)

    # Create audit entry
    create_audit_entry(
        action="CREATE",
        document_id=result.inserted_id,
        customer_id=ObjectId(customer_id),
        old_data=None,
        new_data=copy.deepcopy(new_margin),
        user_id=user_id,
    )

    # Convert for the response
    response_margin = {
        "_id": str(result.inserted_id),
        "customer_id": str(customer_id),
        "product_id": str(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
        "created_at": current_time,
        "updated_at": current_time,
    }
    return {
        "message": "Special margin created successfully.",
        "product": response_margin,
    }


@router.delete("/{customer_id}/bulk")
def delete_all_customer_special_margins(customer_id: str, request: Request = None):
    """
    Delete all special margin entries for a specific customer.
    """
    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None
    customer_obj_id = ObjectId(customer_id)

    # Get existing documents for audit trail
    existing_margins = list(db.special_margins.find({"customer_id": customer_obj_id}))

    if not existing_margins:
        raise HTTPException(
            status_code=404,
            detail="No special margins found for the specified customer or already deleted.",
        )

    # Create audit entries for deletions
    for margin in existing_margins:
        create_audit_entry(
            action="BULK_DELETE",
            document_id=margin["_id"],
            customer_id=customer_obj_id,
            old_data={k: v for k, v in margin.items() if k != "_id"},
            new_data=None,
            user_id=user_id,
            additional_info={"bulk_customer_delete": True},
        )

    result = db.special_margins.delete_many({"customer_id": customer_obj_id})

    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s)."
    }


@router.delete("/{customer_id}/{special_margin_id}")
def delete_customer_special_margin(
    customer_id: str, special_margin_id: str, request: Request = None
):
    """
    Delete a specific special margin entry by _id (special_margin_id).
    """
    user_id = getattr(request, "headers", {}).get("x-user-id") if request else None

    # Get existing document for audit trail
    existing_doc = db.special_margins.find_one(
        {"_id": ObjectId(special_margin_id), "customer_id": ObjectId(customer_id)}
    )

    if not existing_doc:
        raise HTTPException(
            status_code=404, detail="Special margin not found or already deleted."
        )

    # Create audit entry
    create_audit_entry(
        action="DELETE",
        document_id=ObjectId(special_margin_id),
        customer_id=ObjectId(customer_id),
        old_data={k: v for k, v in existing_doc.items() if k != "_id"},
        new_data=None,
        user_id=user_id,
    )

    result = db.special_margins.delete_one(
        {"_id": ObjectId(special_margin_id), "customer_id": ObjectId(customer_id)}
    )

    return {"message": "Special margin deleted successfully."}


# Additional endpoint to retrieve audit trail
@router.get("/{customer_id}/audit")
def get_customer_special_margins_audit(
    customer_id: str,
    limit: int = Query(50, description="Number of audit entries to return"),
    skip: int = Query(0, description="Number of audit entries to skip"),
):
    """
    Retrieve audit trail for special margins of a specific customer.
    """
    if not ObjectId.is_valid(customer_id):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    pipeline = [
        {"$match": {"customer_id": ObjectId(customer_id)}},
        {"$sort": {"timestamp": -1}},
        {"$skip": skip},
        {"$limit": limit},
    ]

    audit_entries = list(db.special_margins_audit.aggregate(pipeline))
    return {
        "audit_trail": [serialize_mongo_document(entry) for entry in audit_entries],
        "total_returned": len(audit_entries),
    }
