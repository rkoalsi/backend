from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import random
from ..config.root import get_database
from ..config.whatsapp import send_whatsapp

router = APIRouter()


class SendOTPRequest(BaseModel):
    phone: str


class VerifyOTPRequest(BaseModel):
    phone: str
    otp: str


def generate_otp():
    return str(random.randint(100000, 999999))


@router.post("/send-otp")
async def send_otp(request: SendOTPRequest):
    try:
        db = get_database()
        phone = request.phone.strip()

        if not phone:
            raise HTTPException(status_code=400, detail="Phone number is required")

        cleaned_phone = ''.join(char for char in phone if char.isdigit())

        if len(cleaned_phone) != 10:
            raise HTTPException(status_code=400, detail="Invalid phone number. Must be 10 digits")

        otp = generate_otp()

        existing_lead = db.catalogue_leads.find_one({"phone": cleaned_phone})

        if existing_lead:
            db.catalogue_leads.update_one(
                {"phone": cleaned_phone},
                {
                    "$set": {
                        "otp": otp,
                        "otp_created_at": datetime.now(),
                        "otp_attempts": 0,
                        "updated_at": datetime.now()
                    }
                }
            )
        else:
            db.catalogue_leads.insert_one({
                "phone": cleaned_phone,
                "otp": otp,
                "otp_created_at": datetime.now(),
                "otp_attempts": 0,
                "verified": False,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })

        template = db.templates.find_one({"name": "otp_verification"})

        if not template:
            raise HTTPException(status_code=500, detail="OTP template not found")

        send_whatsapp(
            to=cleaned_phone,
            template_doc=template,
            params={"otp": otp}
        )

        return {
            "success": True,
            "message": "OTP sent successfully via WhatsApp",
            "phone": cleaned_phone
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error sending OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send OTP: {str(e)}")


@router.post("/verify-otp")
async def verify_otp(request: VerifyOTPRequest):
    try:
        db = get_database()
        phone = request.phone.strip()
        otp = request.otp.strip()

        if not phone or not otp:
            raise HTTPException(status_code=400, detail="Phone number and OTP are required")

        cleaned_phone = ''.join(char for char in phone if char.isdigit())

        lead = db.catalogue_leads.find_one({"phone": cleaned_phone})

        if not lead:
            raise HTTPException(status_code=404, detail="No OTP request found for this phone number")

        if lead.get("verified", False):
            return {
                "success": True,
                "message": "Phone number already verified",
                "verified": True
            }

        otp_created_at = lead.get("otp_created_at")
        if not otp_created_at:
            raise HTTPException(status_code=400, detail="No OTP found. Please request a new OTP")

        time_diff = datetime.now() - otp_created_at
        if time_diff > timedelta(minutes=10):
            raise HTTPException(status_code=400, detail="OTP expired. Please request a new OTP")

        otp_attempts = lead.get("otp_attempts", 0)
        if otp_attempts >= 3:
            raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP")

        stored_otp = lead.get("otp")

        if stored_otp != otp:
            db.catalogue_leads.update_one(
                {"phone": cleaned_phone},
                {"$inc": {"otp_attempts": 1}}
            )
            remaining_attempts = 3 - (otp_attempts + 1)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid OTP. {remaining_attempts} attempts remaining"
            )

        db.catalogue_leads.update_one(
            {"phone": cleaned_phone},
            {
                "$set": {
                    "verified": True,
                    "verified_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                "$unset": {"otp": "", "otp_created_at": "", "otp_attempts": ""}
            }
        )

        return {
            "success": True,
            "message": "Phone number verified successfully",
            "verified": True
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error verifying OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to verify OTP: {str(e)}")
