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
distributor_registrations_collection = db["distributor_registrations"]

STATUSES = ("not_contacted", "contacted", "onboarded", "declined")


class UpdateDistributorRegistrationRequest(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None


@router.get("")
def get_distributor_registrations(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    search: Optional[str] = Query(
        None, description="Search by company, brand, contact person, email or phone"
    ),
    status: Optional[str] = Query(None, description="Filter by follow-up status"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    state: Optional[str] = Query(None, description="Filter by distribution state"),
):
    try:
        match_statement: dict = {}
        if search and search.strip():
            term = search.strip()
            match_statement["$or"] = [
                {"company_name": {"$regex": term, "$options": "i"}},
                {"brand_name": {"$regex": term, "$options": "i"}},
                {"contact_person_name": {"$regex": term, "$options": "i"}},
                {"email": {"$regex": term, "$options": "i"}},
                {"phone": {"$regex": term, "$options": "i"}},
            ]
        if status:
            match_statement["status"] = status
        if category:
            match_statement["categories"] = category
        if state:
            match_statement["distribution_states"] = state

        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        total_count = distributor_registrations_collection.count_documents(
            match_statement
        )
        cursor = distributor_registrations_collection.aggregate(pipeline)
        registrations = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "distributor_registrations": registrations,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.patch("/{registration_id}")
def update_distributor_registration(
    registration_id: str,
    body: UpdateDistributorRegistrationRequest,
):
    try:
        update_fields = {}
        if body.status is not None:
            if body.status not in STATUSES:
                raise HTTPException(status_code=400, detail="Invalid status value")
            update_fields["status"] = body.status
        if body.notes is not None:
            update_fields["notes"] = body.notes

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = distributor_registrations_collection.update_one(
            {"_id": ObjectId(registration_id)},
            {"$set": update_fields},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Registration not found")

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
