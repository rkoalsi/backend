from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from ..config.root import get_database

router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))


class ContactSubmissionRequest(BaseModel):
    name: str
    email: str
    phone: str = ""
    companyName: str = ""
    businessType: List[str] = []
    city: str = ""
    message: str = ""


def now_ist():
    return datetime.now(IST)


@router.post("")
async def create_contact_submission(request: ContactSubmissionRequest):
    try:
        db = get_database()

        submission = {
            "name": request.name,
            "email": request.email,
            "phone": request.phone,
            "company_name": request.companyName,
            "business_type": request.businessType,
            "city": request.city,
            "message": request.message,
            "status": "not_contacted",
            "notes": "",
            "created_at": now_ist(),
        }

        result = db.contact_submissions.insert_one(submission)

        return {
            "success": True,
            "message": "Contact submission saved successfully",
            "id": str(result.inserted_id),
        }

    except Exception as e:
        print(f"Error saving contact submission: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to save contact submission: {str(e)}"
        )
