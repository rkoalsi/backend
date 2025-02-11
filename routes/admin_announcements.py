from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople
from dotenv import load_dotenv
import os, datetime

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]


@router.get("")
def get_announcements(
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
        total_count = db.announcements.count_documents(match_statement)
        cursor = db.announcements.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "announcements": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{announcement_id}")
def delete_announcement(announcement_id: str):
    """
    Delete a announcement by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        doc = db.announcements.find_one({"_id": ObjectId(announcement_id)})
        result = db.announcements.update_one(
            {"_id": ObjectId(announcement_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Catalogue not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_announcement(announcements: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in announcements.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.announcements.insert_one(
            {**update_data, "created_at": datetime.datetime.now()}
        )

        if result:
            # Fetch and return the updated document.
            template = db.templates.find_one({"name": "update_notification_1"})
            notify_all_salespeople(db, template, {})
            return "Document Created"
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Announcement not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{announcement_id}")
def update_announcement(announcement_id: str, announcement: dict):
    """
    Update the announcement with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in announcement.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.announcements.update_one(
            {"_id": ObjectId(announcement_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.announcements.find_one(
                {"_id": ObjectId(announcement_id)}
            )
            return serialize_mongo_document(updated_catalogue)
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Announcement not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
