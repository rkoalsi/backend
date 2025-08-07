from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from config.root import connect_to_mongo, serialize_mongo_document
from bson.objectid import ObjectId
from dotenv import load_dotenv
import math, datetime

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()


@router.get("")
async def get_hooks_category(page: int = Query(0, ge=0), limit: int = Query(25, ge=1)):
    skip = page * limit

    pipeline = [
        {"$skip": skip},
        {"$limit": limit},
    ]

    try:
        hooks_category_cursor = db.hooks_category.aggregate(pipeline)
        hooks_category = list(hooks_category_cursor)
        total_count = db.hooks_category.count_documents({})
        total_pages = math.ceil(total_count / limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse(
        status_code=200,
        content={
            "hooks_category": serialize_mongo_document(hooks_category),
            "total_count": total_count,
            "total_pages": total_pages,
        },
    )


@router.post("")
def create_hook_category(hooks_category: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in hooks_category.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        result = db.hooks_category.insert_one(
            {**update_data, "created_at": datetime.datetime.now()}
        )
        if result:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Document Created Successfully",
                },
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{hook_category_id}")
def update_hook_category(hook_category_id: str, hook_category: dict):
    """
    Update the daily_visit with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in hook_category.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.hooks_category.update_one(
            {"_id": ObjectId(hook_category_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.hook_category.find_one(
                {"_id": ObjectId(hook_category_id)}
            )
            return serialize_mongo_document(updated_catalogue)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{hook_category_id}")
def delete_daily_visit(hook_category_id: str):
    """
    Delete a daily_visit by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        result = db.hooks_category.delete_one({"_id": ObjectId(hook_category_id)})
        if result:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Hooks Category not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
