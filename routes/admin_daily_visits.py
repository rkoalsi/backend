from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople
from dotenv import load_dotenv
import math, datetime

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()

IST_OFFSET = 19800000


@router.get("")
async def get_daily_visits(page: int = Query(0, ge=0), limit: int = Query(25, ge=1)):
    skip = page * limit

    pipeline = [
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        # Unwind the created_by_info array to get a single object (if available)
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
        {"$skip": skip},
        {"$limit": limit},
        {
            "$addFields": {
                "created_at": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "date": {"$add": ["$created_at", IST_OFFSET]},
                    }
                },
                "updated_at": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "date": {"$add": ["$updated_at", IST_OFFSET]},
                    }
                },
                "updates": {
                    "$map": {
                        "input": {"$ifNull": ["$updates", []]},
                        "as": "update",
                        "in": {
                            "$mergeObjects": [
                                "$$update",
                                {
                                    "created_at": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d %H:%M:%S",
                                            "date": {
                                                "$add": [
                                                    "$$update.created_at",
                                                    IST_OFFSET,
                                                ]
                                            },
                                        }
                                    },
                                    "updated_at": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d %H:%M:%S",
                                            "date": {
                                                "$add": [
                                                    "$$update.updated_at",
                                                    IST_OFFSET,
                                                ]
                                            },
                                        }
                                    },
                                },
                            ]
                        },
                    }
                },
            }
        },
        {"$sort": {"created_at": -1}},
    ]

    try:
        daily_visits_cursor = db.daily_visits.aggregate(pipeline)
        daily_visits = list(daily_visits_cursor)
        total_count = db.daily_visits.count_documents({})
        total_pages = math.ceil(total_count / limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Optionally, merge the lookup field into the root document
    for visit in daily_visits:
        if "created_by_info" in visit and visit["created_by_info"]:
            # Assuming the user document has a "name" field.
            visit["created_by"] = visit["created_by_info"].get("name", "N/A")
        else:
            visit["created_by"] = "N/A"
        # Remove the lookup field.
        if "created_by_info" in visit:
            del visit["created_by_info"]
    return JSONResponse(
        status_code=200,
        content={
            "daily_visits": serialize_mongo_document(daily_visits),
            "total_count": total_count,
            "total_pages": total_pages,
        },
    )


@router.delete("/{daily_visit_id}")
def delete_daily_visit(daily_visit_id: str):
    """
    Delete a daily_visit by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        doc = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Catalogue not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_daily_visit(daily_visits: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in daily_visits.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.daily_visits.insert_one(
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


@router.put("/{daily_visit_id}")
def update_daily_visit(daily_visit_id: str, daily_visit: dict):
    """
    Update the daily_visit with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in daily_visit.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.daily_visits.find_one(
                {"_id": ObjectId(daily_visit_id)}
            )
            return serialize_mongo_document(updated_catalogue)
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Announcement not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
