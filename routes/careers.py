import os
import time
import boto3
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from datetime import datetime
from typing import Optional

router = APIRouter()
db = get_database()

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

ALLOWED_RESUME_TYPES = [
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]
MAX_RESUME_SIZE_MB = 5


@router.get("")
def get_active_careers():
    try:
        careers = list(
            db.careers.find({"is_active": True}).sort("created_at", -1)
        )
        return serialize_mongo_document(careers)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{career_id}")
def get_career(career_id: str):
    try:
        try:
            obj_id = ObjectId(career_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid career_id format")

        doc = db.careers.find_one({"_id": obj_id, "is_active": True})
        if not doc:
            raise HTTPException(status_code=404, detail="Career not found")

        return serialize_mongo_document(doc)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/apply")
async def apply_for_career(
    career_id: str = Form(...),
    full_name: str = Form(...),
    email: str = Form(...),
    mobile: str = Form(...),
    current_location: str = Form(...),
    total_experience: str = Form(...),
    relevant_experience: str = Form(...),
    current_company: str = Form(...),
    current_designation: str = Form(...),
    current_ctc: str = Form(...),
    expected_ctc: str = Form(...),
    notice_period: str = Form(...),
    preferred_location: str = Form(...),
    declaration: str = Form(...),
    resume: UploadFile = File(...),
    linkedin_url: Optional[str] = Form(None),
    available_for_interview: Optional[str] = Form(None),
    applied_before: Optional[str] = Form(None),
    custom_answers: Optional[str] = Form(None),
):
    try:
        # Validate career exists and is active
        try:
            career_obj_id = ObjectId(career_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid career_id format")

        career = db.careers.find_one({"_id": career_obj_id, "is_active": True})
        if not career:
            raise HTTPException(status_code=404, detail="Career listing not found or inactive")

        # Validate required fields are not empty
        required = {
            "full_name": full_name,
            "email": email,
            "mobile": mobile,
            "current_location": current_location,
            "total_experience": total_experience,
            "relevant_experience": relevant_experience,
            "current_company": current_company,
            "current_designation": current_designation,
            "current_ctc": current_ctc,
            "expected_ctc": expected_ctc,
            "notice_period": notice_period,
            "preferred_location": preferred_location,
            "declaration": declaration,
        }
        for field_name, value in required.items():
            if not value or not value.strip():
                raise HTTPException(
                    status_code=400,
                    detail=f"{field_name} is required",
                )

        if declaration.strip().lower() != "true":
            raise HTTPException(status_code=400, detail="Declaration must be accepted")

        # Validate email format
        import re
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise HTTPException(status_code=400, detail="Invalid email format")

        # Validate resume file type
        if resume.content_type not in ALLOWED_RESUME_TYPES:
            raise HTTPException(
                status_code=400,
                detail="Resume must be a PDF or Word document",
            )

        # Validate resume file size
        contents = await resume.read()
        if len(contents) > MAX_RESUME_SIZE_MB * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail=f"Resume file size must be less than {MAX_RESUME_SIZE_MB}MB",
            )

        # Upload resume to S3
        file_extension = os.path.splitext(resume.filename)[1]
        timestamp = int(time.time() * 1000)
        unique_filename = f"career_resumes/{career_id}_{timestamp}{file_extension}"

        from io import BytesIO
        s3_client.upload_fileobj(
            BytesIO(contents),
            AWS_S3_BUCKET_NAME,
            unique_filename,
            ExtraArgs={"ACL": "public-read", "ContentType": resume.content_type},
        )
        resume_url = f"{AWS_S3_URL}/{unique_filename}"

        # Parse custom_answers JSON if provided
        parsed_custom_answers = None
        if custom_answers:
            import json
            try:
                parsed_custom_answers = json.loads(custom_answers)
            except json.JSONDecodeError:
                parsed_custom_answers = None

        # Build application document
        application = {
            "career_id": career_id,
            "applicant_name": full_name.strip(),
            "applicant_email": email.strip(),
            "applicant_phone": mobile.strip(),
            "current_location": current_location.strip(),
            "total_experience": total_experience.strip(),
            "relevant_experience": relevant_experience.strip(),
            "current_company": current_company.strip(),
            "current_designation": current_designation.strip(),
            "current_ctc": current_ctc.strip(),
            "expected_ctc": expected_ctc.strip(),
            "notice_period": notice_period.strip(),
            "preferred_location": preferred_location.strip(),
            "linkedin_url": linkedin_url.strip() if linkedin_url else None,
            "available_for_interview": available_for_interview.strip() if available_for_interview else None,
            "applied_before": applied_before.strip() if applied_before else None,
            "custom_answers": parsed_custom_answers,
            "resume_url": resume_url,
            "declaration": True,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
        }

        result = db.career_applications.insert_one(application)
        if result.inserted_id:
            return {"message": "Application submitted successfully", "id": str(result.inserted_id)}
        else:
            raise HTTPException(status_code=500, detail="Failed to submit application")

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
