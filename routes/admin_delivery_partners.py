from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Body,
)
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, datetime

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
delivery_partners_collection = db["delivery_partners"]


@router.get("")
def get_delivery_partners(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}

        pipeline = [
            {"$match": match_statement},
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        total_count = delivery_partners_collection.count_documents(match_statement)
        cursor = delivery_partners_collection.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")

        return {
            "delivery_partners": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_delivery_partner(
    data: dict = Body(..., description="Fields to update for the targeted customer"),
):
    try:
        data["created_at"] = datetime.datetime.now()
        delivery_partners_collection.insert_one({**data})
        return {"message": "Delivery Partner Created Successfully"}
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{delivery_partner_id}")
def update_delivery_partner(
    delivery_partner_id: str,
    update_data: dict = Body(
        ..., description="Fields to update for the targeted customer"
    ),
):
    try:
        if not ObjectId.is_valid(delivery_partner_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(delivery_partner_id)
        delivery_partner = delivery_partners_collection.find_one(
            {"_id": customer_obj_id}
        )
        if not delivery_partner:
            raise HTTPException(status_code=404, detail="Customer not found")

        update_data["updated_at"] = datetime.datetime.now()

        delivery_partners_collection.update_one(
            {"_id": customer_obj_id}, {"$set": update_data}
        )
        return {"message": "Delivery Partner updated successfully"}
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{delivery_partner_id}")
def delete_delivery_partner(delivery_partner_id: str):
    try:
        if not ObjectId.is_valid(delivery_partner_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        obj_id = ObjectId(delivery_partner_id)
        result = delivery_partners_collection.delete_one({"_id": obj_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Delivery Partner not found")

        return {"message": "Delivery Partner deleted successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
