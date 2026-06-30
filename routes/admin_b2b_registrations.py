from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
db = get_database()
b2b_leads_collection = db["b2b_leads"]


class UpdateB2BLeadRequest(BaseModel):
    notes: Optional[str] = None


@router.patch("/{lead_id}")
def update_b2b_lead(lead_id: str, body: UpdateB2BLeadRequest):
    """Edit admin-facing fields (currently just notes) on a B2B self-registration lead."""
    try:
        update_fields = {}
        if body.notes is not None:
            update_fields["notes"] = body.notes
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = b2b_leads_collection.update_one(
            {"_id": ObjectId(lead_id)}, {"$set": update_fields}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Lead not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("")
def get_b2b_registrations(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    status: Optional[str] = Query(None, description="Filter: lead | verified | account"),
):
    """List B2B self-registration leads (newest first) from the funnel.

    Lifecycle: a lead row is created the instant a number requests an OTP, then
    flipped `verified` once the OTP is confirmed (which also creates the user
    account). Joins the `users` collection by phone to surface account state
    (status, whether their profile/details are complete, Zoho customer_id).

    `status` filter:
      • lead     → requested OTP but never verified (drop-off)
      • verified → completed OTP verification (account exists)
      • account  → verified AND has a linked customer account
    """
    try:
        match_statement: dict = {}
        if status == "lead":
            match_statement["verified"] = {"$ne": True}
        elif status in ("verified", "account"):
            match_statement["verified"] = True

        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "phone",
                    "foreignField": "phone",
                    "as": "account",
                }
            },
            {"$unwind": {"path": "$account", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "account_name": "$account.name",
                    "account_status": "$account.status",
                    "profile_completed": {"$ifNull": ["$account.profile_completed", False]},
                    "customer_id": "$account.customer_id",
                    "shop_name": "$account.customer_name",
                }
            },
            {"$project": {"account": 0}},
        ]

        total_count = b2b_leads_collection.count_documents(match_statement)
        cursor = b2b_leads_collection.aggregate(pipeline)
        registrations = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")

        return {
            "b2b_registrations": registrations,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
