from fastapi import APIRouter, HTTPException, Depends
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from datetime import datetime
router = APIRouter()
db = get_database()
collection = db["customer_address_details"]

try:
    collection.create_index([("customer_id", 1), ("address_id", 1)], unique=True)
except Exception:
    pass

VALID_STATUSES = {"open", "closed", "warehouse"}


@router.get("/{customer_id}")
def get_address_details(customer_id: str):
    """Return all address detail records for a given customer."""
    docs = list(collection.find({"customer_id": customer_id}))
    return {"address_details": [serialize_mongo_document(d) for d in docs]}


@router.put("/{customer_id}/{address_id}", dependencies=[Depends(JWTBearer())])
def upsert_address_detail(customer_id: str, address_id: str, payload: dict):
    """
    Create or update the extra metadata for a specific customer address.
    Accepted payload fields: status, notes (and any future fields).
    """
    status = payload.get("status")
    if status and status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{status}'. Must be one of: {', '.join(sorted(VALID_STATUSES))}",
        )

    update_fields: dict = {"updated_at": datetime.utcnow()}
    if status is not None:
        update_fields["status"] = status
    if "notes" in payload:
        update_fields["notes"] = payload["notes"]
    # Support arbitrary future fields passed in the payload
    reserved = {"status", "notes", "customer_id", "address_id"}
    for key, value in payload.items():
        if key not in reserved:
            update_fields[key] = value

    result = collection.find_one_and_update(
        {"customer_id": customer_id, "address_id": address_id},
        {
            "$set": update_fields,
            "$setOnInsert": {
                "customer_id": customer_id,
                "address_id": address_id,
                "created_at": datetime.utcnow(),
            },
        },
        upsert=True,
        return_document=True,
    )
    return {"address_detail": serialize_mongo_document(result)}


@router.delete("/{customer_id}/{address_id}", dependencies=[Depends(JWTBearer())])
def delete_address_detail(customer_id: str, address_id: str):
    """Remove the extra metadata for a specific customer address."""
    result = collection.delete_one(
        {"customer_id": customer_id, "address_id": address_id}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Address detail not found")
    return {"status": "deleted"}
