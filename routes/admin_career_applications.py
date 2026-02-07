from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from typing import Optional

router = APIRouter()
db = get_database()


@router.get("")
def get_career_applications(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    career_id: Optional[str] = Query(None, description="Filter by career ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    try:
        match_statement = {}
        if career_id:
            match_statement["career_id"] = career_id
        if status:
            match_statement["status"] = status

        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        total_count = db.career_applications.count_documents(match_statement)
        cursor = db.career_applications.aggregate(pipeline)
        applications = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "career_applications": applications,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{application_id}")
def get_career_application(application_id: str):
    try:
        try:
            obj_id = ObjectId(application_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid application_id format")

        doc = db.career_applications.find_one({"_id": obj_id})
        if not doc:
            raise HTTPException(status_code=404, detail="Application not found")

        return serialize_mongo_document(doc)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{application_id}")
def update_career_application(application_id: str, application: dict):
    try:
        try:
            obj_id = ObjectId(application_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid application_id format")

        existing_doc = db.career_applications.find_one({"_id": obj_id})
        if not existing_doc:
            raise HTTPException(status_code=404, detail="Application not found")

        update_data = {k: v for k, v in application.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.career_applications.update_one(
            {"_id": obj_id}, {"$set": update_data}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Application not found")

        updated_doc = db.career_applications.find_one({"_id": obj_id})
        return serialize_mongo_document(updated_doc)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{application_id}")
def delete_career_application(application_id: str):
    try:
        try:
            obj_id = ObjectId(application_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid application_id format")

        result = db.career_applications.delete_one({"_id": obj_id})
        if result.deleted_count == 1:
            return {"detail": "Application deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Application not found")
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
