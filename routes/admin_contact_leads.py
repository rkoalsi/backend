from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
db = get_database()
contact_submissions_collection = db["contact_submissions"]


class UpdateContactSubmissionRequest(BaseModel):
    status: Optional[str] = None  # "not_contacted" | "contacted"
    notes: Optional[str] = None


@router.get("")
def get_contact_submissions(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, or phone"),
):
    try:
        match_statement = {}
        if search and search.strip():
            match_statement["$or"] = [
                {"name": {"$regex": search.strip(), "$options": "i"}},
                {"email": {"$regex": search.strip(), "$options": "i"}},
                {"phone": {"$regex": search.strip(), "$options": "i"}},
            ]
        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        total_count = contact_submissions_collection.count_documents(match_statement)
        cursor = contact_submissions_collection.aggregate(pipeline)
        leads = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "contact_submissions": leads,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.patch("/{submission_id}")
def update_contact_submission(
    submission_id: str,
    body: UpdateContactSubmissionRequest,
):
    try:
        update_fields = {}
        if body.status is not None:
            if body.status not in ("not_contacted", "contacted"):
                raise HTTPException(status_code=400, detail="Invalid status value")
            update_fields["status"] = body.status
        if body.notes is not None:
            update_fields["notes"] = body.notes

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = contact_submissions_collection.update_one(
            {"_id": ObjectId(submission_id)},
            {"$set": update_fields},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Submission not found")

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
