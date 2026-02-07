from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from datetime import datetime

router = APIRouter()
db = get_database()


@router.get("")
def get_careers(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}
        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        total_count = db.careers.count_documents(match_statement)
        cursor = db.careers.aggregate(pipeline)
        careers = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "careers": careers,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_career(career: dict):
    try:
        update_data = {k: v for k, v in career.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No data provided")

        update_data["is_active"] = True
        update_data["created_at"] = datetime.utcnow().isoformat()

        result = db.careers.insert_one(update_data)
        if result:
            return "Document Created"
        else:
            raise HTTPException(status_code=500, detail="Failed to create career")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{career_id}")
def update_career(career_id: str, career: dict):
    try:
        try:
            obj_id = ObjectId(career_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid career_id format")

        existing_doc = db.careers.find_one({"_id": obj_id})
        if not existing_doc:
            raise HTTPException(status_code=404, detail="Career not found")

        update_data = {k: v for k, v in career.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.careers.update_one({"_id": obj_id}, {"$set": update_data})

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Career not found")

        updated_career = db.careers.find_one({"_id": obj_id})
        return serialize_mongo_document(updated_career)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{career_id}")
def delete_career(career_id: str):
    try:
        doc = db.careers.find_one({"_id": ObjectId(career_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Career not found")

        result = db.careers.update_one(
            {"_id": ObjectId(career_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Career status toggled successfully"}
        else:
            raise HTTPException(status_code=404, detail="Career not found")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
