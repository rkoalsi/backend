from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, validator
from datetime import datetime, timedelta, timezone
import random
import re
from typing import Optional
from ..config.root import get_database
from ..config.whatsapp import send_whatsapp

router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))


class SendOTPRequest(BaseModel):
    phone: str

    @validator('phone')
    def validate_phone(cls, v):
        v = v.strip()
        if not re.match(r'^\+\d+$', v):
            raise ValueError('Phone number must be in international format (e.g., +919876543210)')
        if len(v) < 10 or len(v) > 16:
            raise ValueError('Invalid phone number length')
        return v


class VerifyOTPRequest(BaseModel):
    phone: str
    otp: str
    brand_name: str

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


def generate_otp():
    return str(random.randint(100000, 999999))


def now_ist():
    return datetime.now(IST)


@router.post("/send-otp")
async def send_otp(request: SendOTPRequest):
    """
    Send OTP to phone number via WhatsApp for brand lead capture
    """
    try:
        db = get_database()
        phone = request.phone

        otp = generate_otp()

        existing_lead = db.brand_leads.find_one({"phone": phone})

        if existing_lead:
            db.brand_leads.update_one(
                {"phone": phone},
                {
                    "$set": {
                        "otp": otp,
                        "otp_created_at": now_ist(),
                        "otp_attempts": 0,
                        "updated_at": now_ist()
                    }
                }
            )
        else:
            db.brand_leads.insert_one({
                "phone": phone,
                "otp": otp,
                "otp_created_at": now_ist(),
                "otp_attempts": 0,
                "verified": False,
                "verified_at": None,
                "brand_name": None,
                "created_at": now_ist(),
                "updated_at": now_ist()
            })

        template = db.templates.find_one({"name": "otp_verification"})

        if not template:
            raise HTTPException(status_code=500, detail="OTP template not found")

        send_whatsapp(
            to=phone,
            template_doc=template,
            params={"otp": otp}
        )

        return {
            "success": True,
            "message": "OTP sent successfully via WhatsApp",
            "phone": phone
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error sending OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send OTP: {str(e)}")


@router.post("/verify-otp")
async def verify_otp(request: VerifyOTPRequest):
    """
    Verify OTP and store brand lead with brand name
    """
    try:
        db = get_database()
        phone = request.phone
        otp = request.otp
        brand_name = request.brand_name

        lead = db.brand_leads.find_one({"phone": phone})

        if not lead:
            raise HTTPException(status_code=404, detail="No OTP request found for this phone number")

        if lead.get("verified", False):
            # Already verified — update the brand_name for the new interest
            db.brand_leads.update_one(
                {"phone": phone},
                {"$set": {"brand_name": brand_name, "updated_at": now_ist()}}
            )
            return {
                "success": True,
                "message": "Phone number already verified",
                "verified": True
            }

        otp_created_at = lead.get("otp_created_at")
        if not otp_created_at:
            raise HTTPException(status_code=400, detail="No OTP found. Please request a new OTP")

        # Check OTP expiry (10 minutes)
        if otp_created_at.tzinfo is None:
            otp_created_at = otp_created_at.replace(tzinfo=timezone.utc)
        time_diff = now_ist() - otp_created_at
        if time_diff > timedelta(minutes=10):
            raise HTTPException(status_code=400, detail="OTP expired. Please request a new OTP")

        otp_attempts = lead.get("otp_attempts", 0)
        if otp_attempts >= 3:
            raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP")

        stored_otp = lead.get("otp")

        if stored_otp != otp:
            db.brand_leads.update_one(
                {"phone": phone},
                {"$inc": {"otp_attempts": 1}}
            )
            remaining_attempts = 3 - (otp_attempts + 1)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid OTP. {remaining_attempts} attempts remaining"
            )

        db.brand_leads.update_one(
            {"phone": phone},
            {
                "$set": {
                    "verified": True,
                    "verified_at": now_ist(),
                    "brand_name": brand_name,
                    "updated_at": now_ist()
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
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error verifying OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to verify OTP: {str(e)}")
