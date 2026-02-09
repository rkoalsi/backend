import os
import re
import time
import random
import boto3
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from ..config.root import get_database, serialize_mongo_document
from ..config.whatsapp import send_whatsapp
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from typing import Optional

router = APIRouter()
db = get_database()


class CareerSendOTPRequest(BaseModel):
    phone: str

    @validator('phone')
    def validate_phone(cls, v):
        v = v.strip()
        if not re.match(r'^\+\d+$', v):
            raise ValueError('Phone number must be in international format (e.g., +919876543210)')
        if len(v) < 10 or len(v) > 16:
            raise ValueError('Invalid phone number length')
        return v


class CareerVerifyOTPRequest(BaseModel):
    phone: str
    otp: str

    @validator('phone')
    def validate_phone(cls, v):
        v = v.strip()
        if not re.match(r'^\+\d+$', v):
            raise ValueError('Phone number must be in international format (e.g., +919876543210)')
        if len(v) < 10 or len(v) > 16:
            raise ValueError('Invalid phone number length')
        return v

    @validator('otp')
    def validate_otp(cls, v):
        v = v.strip()
        if not v.isdigit() or len(v) != 6:
            raise ValueError('OTP must be a 6-digit number')
        return v


def generate_career_otp():
    return str(random.randint(100000, 999999))

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


@router.post("/send-otp")
async def career_send_otp(request: CareerSendOTPRequest):
    try:
        phone = request.phone
        otp = generate_career_otp()

        existing = db.career_otp.find_one({"phone": phone})
        if existing:
            db.career_otp.update_one(
                {"phone": phone},
                {
                    "$set": {
                        "otp": otp,
                        "otp_created_at": datetime.now(),
                        "otp_attempts": 0,
                        "updated_at": datetime.now(),
                    }
                },
            )
        else:
            db.career_otp.insert_one(
                {
                    "phone": phone,
                    "otp": otp,
                    "otp_created_at": datetime.now(),
                    "otp_attempts": 0,
                    "verified": False,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            )

        template = db.templates.find_one({"name": "otp_verification"})
        if not template:
            raise HTTPException(status_code=500, detail="OTP template not found")

        send_whatsapp(to=phone, template_doc=template, params={"otp": otp})

        return {"success": True, "message": "OTP sent successfully via WhatsApp", "phone": phone}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send OTP: {str(e)}")


@router.post("/verify-otp")
async def career_verify_otp(request: CareerVerifyOTPRequest):
    try:
        phone = request.phone
        otp = request.otp

        record = db.career_otp.find_one({"phone": phone})
        if not record:
            raise HTTPException(status_code=404, detail="No OTP request found for this phone number")

        if record.get("verified", False):
            return {"success": True, "message": "Phone number already verified", "verified": True}

        otp_created_at = record.get("otp_created_at")
        if not otp_created_at:
            raise HTTPException(status_code=400, detail="No OTP found. Please request a new OTP")

        if datetime.now() - otp_created_at > timedelta(minutes=10):
            raise HTTPException(status_code=400, detail="OTP expired. Please request a new OTP")

        otp_attempts = record.get("otp_attempts", 0)
        if otp_attempts >= 3:
            raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP")

        stored_otp = record.get("otp")
        if stored_otp != otp:
            db.career_otp.update_one({"phone": phone}, {"$inc": {"otp_attempts": 1}})
            remaining = 3 - (otp_attempts + 1)
            raise HTTPException(status_code=400, detail=f"Invalid OTP. {remaining} attempts remaining")

        db.career_otp.update_one(
            {"phone": phone},
            {
                "$set": {"verified": True, "verified_at": datetime.now(), "updated_at": datetime.now()},
                "$unset": {"otp": "", "otp_created_at": "", "otp_attempts": ""},
            },
        )

        return {"success": True, "message": "Phone number verified successfully", "verified": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to verify OTP: {str(e)}")


@router.post("/apply")
async def apply_for_career(
    career_id: str = Form(...),
    full_name: str = Form(...),
    email: str = Form(...),
    mobile: str = Form(...),
    current_city: str = Form(...),
    current_state: str = Form(...),
    total_experience_years: str = Form(...),
    total_experience_months: str = Form(...),
    relevant_experience_years: str = Form(...),
    relevant_experience_months: str = Form(...),
    current_company: str = Form(...),
    current_designation: str = Form(...),
    current_ctc_amount: str = Form(...),
    expected_ctc_amount: str = Form(...),
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
            "current_city": current_city,
            "current_state": current_state,
            "current_company": current_company,
            "current_designation": current_designation,
            "current_ctc_amount": current_ctc_amount,
            "expected_ctc_amount": expected_ctc_amount,
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
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise HTTPException(status_code=400, detail="Invalid email format")

        # Verify OTP was completed for this phone number
        otp_record = db.career_otp.find_one({"phone": mobile.strip()})
        if not otp_record or not otp_record.get("verified", False):
            raise HTTPException(status_code=400, detail="Mobile number not verified. Please complete OTP verification.")

        # Check for duplicate applications (same career + same email or phone)
        existing_app = db.career_applications.find_one({
            "career_id": career_id,
            "$or": [
                {"applicant_email": email.strip().lower()},
                {"applicant_phone": mobile.strip()},
            ],
        })
        if existing_app:
            raise HTTPException(
                status_code=409,
                detail="You have already applied for this position with this email or phone number.",
            )

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

        # Validate CTC amounts are numeric
        try:
            float(current_ctc_amount)
            float(expected_ctc_amount)
        except ValueError:
            raise HTTPException(status_code=400, detail="CTC amount must be a valid number")

        # Build application document
        total_exp = f"{total_experience_years} Years {total_experience_months} Months"
        relevant_exp = f"{relevant_experience_years} Years {relevant_experience_months} Months"

        application = {
            "career_id": career_id,
            "applicant_name": full_name.strip(),
            "applicant_email": email.strip().lower(),
            "applicant_phone": mobile.strip(),
            "current_city": current_city.strip(),
            "current_state": current_state.strip(),
            "current_location": f"{current_city.strip()}, {current_state.strip()}",
            "total_experience": total_exp,
            "total_experience_years": int(total_experience_years),
            "total_experience_months": int(total_experience_months),
            "relevant_experience": relevant_exp,
            "relevant_experience_years": int(relevant_experience_years),
            "relevant_experience_months": int(relevant_experience_months),
            "current_company": current_company.strip(),
            "current_designation": current_designation.strip(),
            "current_ctc": f"{current_ctc_amount} LPA",
            "current_ctc_amount": float(current_ctc_amount),
            "expected_ctc": f"{expected_ctc_amount} LPA",
            "expected_ctc_amount": float(expected_ctc_amount),
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
