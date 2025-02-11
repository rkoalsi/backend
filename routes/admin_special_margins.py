from fastapi import APIRouter, HTTPException, Body
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId

client, db = connect_to_mongo()

router = APIRouter()


@router.get("/{customer_id}")
def get_customer_special_margins(customer_id: str):
    """
    Retrieve all special margin products for the given customer.
    """
    special_margins = [
        serialize_mongo_document(doc)
        for doc in db.special_margins.find({"customer_id": ObjectId(customer_id)})
    ]
    # Convert ObjectIds to strings for JSON serializability
    return {"products": special_margins}


@router.post("/bulk/{customer_id}")
def bulk_create_or_update_special_margins(customer_id: str, data: list = Body(...)):
    """
    Create or update multiple special margin entries in bulk for a given customer using update_many.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer_id")
        customer_obj_id = ObjectId(customer_id)

        for item in data:
            if not all(k in item for k in ("product_id", "name", "margin")):
                raise HTTPException(
                    status_code=400,
                    detail="Each item must have 'product_id', 'name', and 'margin'.",
                )

            if not ObjectId.is_valid(item["product_id"]):
                raise HTTPException(
                    status_code=400, detail=f"Invalid product_id: {item['product_id']}"
                )

            product_obj_id = ObjectId(item["product_id"])

            # Use update_many to update or insert
            db.special_margins.update_one(
                {"customer_id": customer_obj_id, "product_id": product_obj_id},
                {
                    "$set": {
                        "name": item["name"],
                        "margin": item["margin"],
                        "customer_id": customer_obj_id,
                        "product_id": product_obj_id,
                    }
                },
                upsert=True,
            )

        return {"message": "Bulk operation completed successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/{customer_id}")
def create_customer_special_margin(customer_id: str, data: dict = Body(...)):
    """
    Create a new special margin entry for a given customer.
    Expects data like:
      {
        "product_id": "XYZ123",
        "name": "Some Product",
        "margin": "50%"
      }
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
        # Already exists -> return 409 conflict
        return "Product Margin Already Exists"

    # Optionally validate that the passed customer_id & product_id are valid ObjectIds
    # if not ObjectId.is_valid(customer_id) or not ObjectId.is_valid(data["product_id"]):
    #     raise HTTPException(status_code=400, detail="Invalid ObjectId")

    # Insert into DB as actual ObjectIds
    new_margin = {
        "customer_id": ObjectId(customer_id),
        "product_id": ObjectId(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
    }

    result = db.special_margins.insert_one(new_margin)

    # Convert for the response
    response_margin = {
        "_id": str(result.inserted_id),
        "customer_id": str(customer_id),
        "product_id": str(data["product_id"]),
        "name": data["name"],
        "margin": data["margin"],
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
    result = db.special_margins.delete_many({"customer_id": ObjectId(customer_id)})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404,
            detail="No special margins found for the specified customer or already deleted.",
        )
    return {
        "message": f"Successfully deleted {result.deleted_count} special margin(s)."
    }


@router.delete("/{customer_id}/{special_margin_id}")
def delete_customer_special_margin(customer_id: str, special_margin_id: str):
    """
    Delete a specific special margin entry by _id (special_margin_id).
    """
    result = db.special_margins.delete_one(
        {"_id": ObjectId(special_margin_id), "customer_id": ObjectId(customer_id)}
    )
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404, detail="Special margin not found or already deleted."
        )
    return {"message": "Special margin deleted successfully."}
