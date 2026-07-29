"""Public distributor / brand onboarding form (`/distributors` on both the
marketplace and pupscribe.in).

The phone number is verified over WhatsApp before the application is accepted:

  1. POST /otp/request  — the number is checked for WhatsApp reachability, then a
                          6-digit code is sent to it.
  2. GET  /otp/status   — polled while the code is in flight; reports back when
                          Meta tells us the number is not on WhatsApp.
  3. POST /otp/verify   — exchanges a correct code for a short-lived signed token.
  4. POST ""            — the application itself, which only saves when it carries
                          a valid token for the phone number on the form.

There is no way to check WhatsApp registration synchronously through Plivo, so
reachability is established in two stages: a cheap local gate that rejects
landlines and malformed numbers up front, and the asynchronous delivery report
for the OTP message, which is where a genuinely non-WhatsApp number shows up as
`failed`/`undelivered`.
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, validator
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import base64
import hashlib
import hmac
import os
import re
import secrets
import time

from ..config.root import get_database
from ..config.phone import normalize_indian_mobile
from ..config.whatsapp import send_template_message

router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))
SECRET_KEY = os.getenv("SECRET_KEY")
OTP_TEMPLATE_NAME = os.getenv("OTP_TEMPLATE_NAME", "otp_verification")

# How long a verified-phone token stays usable — long enough to fill in a ten
# field B2B form without rushing, short enough that a leaked token is useless.
VERIFIED_TOKEN_EXPIRE_SECONDS = 45 * 60

OTP_PURPOSE = "distributor"

# Kept in sync with the CATEGORIES list on the two /distributors pages
# (order_form_frontend and the pupscribe.in site). A brand whose category is not
# on the list can type their own — see the categories validator, which keeps a
# known value verbatim (so the admin filters keep working) and otherwise accepts
# the free text as-is.
CATEGORIES = [
    "Pet Food (Dog & Cat)",
    "Dog Food",
    "Cat Food",
    "Cat Litter",
    "Grooming (Shampoo & Conditioner)",
    "Treats & Chews",
    "Toys",
    "Collars, Leashes & Harnesses",
    "Health & Supplements",
    "Accessories & Others",
]

INDIAN_STATES = [
    "Andaman and Nicobar Islands",
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chandigarh",
    "Chhattisgarh",
    "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jammu and Kashmir",
    "Jharkhand",
    "Karnataka",
    "Kerala",
    "Ladakh",
    "Lakshadweep",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "Nagaland",
    "Odisha",
    "Puducherry",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
]

_STATE_LOOKUP = {s.lower(): s for s in INDIAN_STATES}
_CATEGORY_LOOKUP = {c.lower(): c for c in CATEGORIES}

# Icon keys the two /distributors pages know how to render. Admins pick one per
# card rather than uploading artwork.
CARD_ICONS = [
    "pets",
    "food",
    "litter",
    "grooming",
    "treats",
    "toys",
    "health",
    "accessories",
    "shipping",
    "store",
]
CARD_ACCENTS = ["indigo", "magenta", "green"]

# Shown until an admin configures their own set in /admin/leads.
DEFAULT_CARDS = [
    {
        "title": "Pet food distributors",
        "text": "Dog food and cat food distributors — dry, wet, kitten and therapeutic diets.",
        "icon": "food",
        "accent": "indigo",
    },
    {
        "title": "Cat litter distributors",
        "text": "Clumping, silica, tofu and natural litter brands looking for national reach.",
        "icon": "litter",
        "accent": "magenta",
    },
    {
        "title": "Grooming distributors",
        "text": "Dog and cat shampoo, conditioners, coat care and grooming tools.",
        "icon": "grooming",
        "accent": "green",
    },
    {
        "title": "Treats, toys & accessories",
        "text": "Treats and chews, toys, collars, leashes, harnesses and everyday goods.",
        "icon": "treats",
        "accent": "indigo",
    },
]

# Delivery states that mean the number is not on WhatsApp (Meta error 131026,
# "message undeliverable") or the send was refused outright.
_UNREACHABLE_STATUSES = {"failed", "undelivered", "rejected"}


def now_ist():
    return datetime.now(IST)


def _db():
    return get_database()


# ── Verified-phone token ──────────────────────────────────────────────────────
# Same construction as the login-link token in users.py: an opaque, signed,
# expiring stand-in for a phone number. Holding one proves only that whoever has
# it answered an OTP on that number a few minutes ago.


def _token_signature(payload: str) -> str:
    digest = hmac.new(
        (SECRET_KEY or "").encode(), payload.encode(), hashlib.sha256
    ).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")[:22]


def make_verified_phone_token(phone10: str) -> str:
    payload = f"{phone10}.{int(time.time()) + VERIFIED_TOKEN_EXPIRE_SECONDS}"
    encoded = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    return f"{encoded}.{_token_signature(payload)}"


def resolve_verified_phone_token(token: str) -> Optional[str]:
    """Return the 10-digit phone for a valid, unexpired token, else None."""
    try:
        encoded, signature = str(token).split(".")
        payload = base64.urlsafe_b64decode(
            encoded + "=" * (-len(encoded) % 4)
        ).decode()
        phone10, expires_at = payload.rsplit(".", 1)
        if not hmac.compare_digest(signature, _token_signature(payload)):
            return None
        if int(expires_at) < int(time.time()):
            return None
        return phone10
    except Exception:
        return None


# ── OTP storage ───────────────────────────────────────────────────────────────
# Reuses the shared `otp_codes` collection (hashed codes, TTL index, unique on
# phone+purpose) under its own purpose so a distributor code can never be
# replayed against the customer login flow.

OTP_EXPIRE_SECONDS = int(os.getenv("OTP_EXPIRE_SECONDS", 300))
OTP_MAX_VERIFY_ATTEMPTS = 5
OTP_RESEND_COOLDOWN_SECONDS = int(os.getenv("OTP_RESEND_COOLDOWN_SECONDS", 30))


def _hash_otp(code: str) -> str:
    return hmac.new(
        (SECRET_KEY or "").encode(), code.encode(), hashlib.sha256
    ).hexdigest()


# ── Reachability cache ────────────────────────────────────────────────────────


def _record_send(phone10: str, message_uuid: Optional[str], status: str, error=None):
    _db().whatsapp_reachability.update_one(
        {"phone": phone10},
        {
            "$set": {
                "phone": phone10,
                "message_uuid": message_uuid,
                "status": status,
                "error": error,
                "checked_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )


def _resolve_reachability(phone10: str) -> dict:
    """Latest known delivery state of the OTP we sent to `phone10`.

    Returns {"state": "pending"|"reachable"|"unreachable", "detail": str}.
    """
    db = _db()
    entry = db.whatsapp_reachability.find_one({"phone": phone10})
    if not entry:
        return {"state": "pending", "detail": ""}

    status = entry.get("status")
    if status in _UNREACHABLE_STATUSES:
        return {"state": "unreachable", "detail": entry.get("error") or ""}

    message_uuid = entry.get("message_uuid")
    if not message_uuid:
        # The send itself threw before Plivo accepted it.
        return {"state": "unreachable", "detail": entry.get("error") or ""}

    # The delivery report lands on the `chats` row keyed by message_uuid
    # (routes/chats.py::plivo_callback), so that is the source of truth.
    chat = db.chats.find_one(
        {"type": "outgoing", "message_uuid": message_uuid},
        {"status": 1, "error_code": 1},
    )
    chat_status = (chat or {}).get("status")
    if chat_status in _UNREACHABLE_STATUSES:
        detail = f"WhatsApp error {chat.get('error_code')}" if chat.get("error_code") else ""
        _record_send(phone10, message_uuid, chat_status, detail)
        return {"state": "unreachable", "detail": detail}
    if chat_status in ("delivered", "read"):
        return {"state": "reachable", "detail": ""}

    # "queued"/"sent" — accepted by Plivo but no terminal report yet. The caller
    # polls; the frontend lets the user proceed once it stops looking wrong.
    return {"state": "pending", "detail": ""}


# ── Request models ────────────────────────────────────────────────────────────


class OtpRequestBody(BaseModel):
    phone: str


class OtpVerifyBody(BaseModel):
    phone: str
    code: str

    @validator("code")
    def validate_code(cls, v):
        v = (v or "").strip()
        if not v.isdigit() or len(v) != 6:
            raise ValueError("Enter the 6-digit code")
        return v


class AddressModel(BaseModel):
    """Same shape as the billing/shipping addresses on the customer onboarding
    modal (CustomerCreationRequestForm), so a distributor that later becomes a
    customer needs no re-keying."""

    address: str = ""
    street2: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    phone: str = ""
    attention: str = ""
    country: str = "India"

    @validator("address", "city", "zip")
    def required_part(cls, v):
        v = (v or "").strip()
        if not v:
            raise ValueError("Street address, city and pincode are required")
        return v

    @validator("street2", "phone", "attention", "country")
    def trim(cls, v):
        return (v or "").strip()

    @validator("state")
    def validate_state(cls, v):
        match = _STATE_LOOKUP.get((v or "").strip().lower())
        if not match:
            raise ValueError("Select a valid state")
        return match


class DistributorRegistrationRequest(BaseModel):
    # Company / contact block
    companyName: str
    gstNumber: str = ""
    panNumber: str = ""
    billingAddress: AddressModel
    shipFromAddress: AddressModel
    phone: str
    email: str
    contactPersonName: str
    # Brand block
    brandName: str
    categories: List[str] = []
    distributionStates: List[str] = []
    margin: str = ""
    # Proof that `phone` answered an OTP (from /otp/verify)
    verificationToken: str

    @validator("companyName", "contactPersonName", "brandName")
    def required_text(cls, v):
        v = (v or "").strip()
        if not v:
            raise ValueError("This field is required")
        return v

    @validator("email")
    def validate_email(cls, v):
        v = (v or "").strip()
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v):
            raise ValueError("Enter a valid email address")
        return v

    @validator("gstNumber", "panNumber")
    def upper_tax_id(cls, v):
        return (v or "").strip().upper()

    @validator("categories")
    def validate_categories(cls, v):
        """Known categories are canonicalised so the admin filters group them;
        anything else is kept as the applicant typed it. Plenty of brands sit
        outside our list (pharma, feeding bowls, pet furniture) and rejecting
        them would lose the lead."""
        cleaned = []
        for item in v or []:
            text = (item or "").strip()
            if not text:
                continue
            if len(text) > 60:
                raise ValueError("Category names must be 60 characters or fewer")
            # Collapse whitespace so "Cat  Litter " and "Cat Litter" are one value.
            text = re.sub(r"\s+", " ", text)
            match = _CATEGORY_LOOKUP.get(text.lower(), text)
            if match not in cleaned:
                cleaned.append(match)
        if not cleaned:
            raise ValueError("Select at least one category")
        if len(cleaned) > 15:
            raise ValueError("Select up to 15 categories")
        return cleaned

    @validator("distributionStates")
    def validate_states(cls, v):
        cleaned = []
        for item in v or []:
            match = _STATE_LOOKUP.get((item or "").strip().lower())
            if not match:
                raise ValueError(f"Unknown state: {item}")
            if match not in cleaned:
                cleaned.append(match)
        if not cleaned:
            raise ValueError("Select at least one distribution state")
        return cleaned


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("/options")
async def get_options():
    """Dropdown options for the public /distributors form, so the two frontends
    (marketplace + pupscribe.in) never drift from the server-side validator."""
    return {"categories": CATEGORIES, "states": INDIAN_STATES}


@router.get("/cards")
async def get_cards():
    """The "who we are looking for" cards on /distributors, managed from
    /admin/leads. Falls back to the defaults below when nothing has been
    configured, so the page is never blank on a fresh database."""
    try:
        cursor = (
            _db()
            .distributor_page_cards.find({"active": {"$ne": False}})
            .sort("order", 1)
        )
        cards = [
            {
                "title": c.get("title", ""),
                "text": c.get("text", ""),
                "icon": c.get("icon", "pets"),
                "accent": c.get("accent", "indigo"),
            }
            for c in cursor
        ]
    except Exception as e:
        print(f"Failed to load distributor page cards: {e}")
        cards = []

    return {"cards": cards or DEFAULT_CARDS}


@router.post("/otp/request")
async def request_otp(body: OtpRequestBody):
    """Send a WhatsApp OTP to the applicant's mobile number.

    The number is gated locally first: `normalize_indian_mobile` rejects
    landlines, short codes and fields holding two numbers, none of which can
    receive a WhatsApp message. Numbers that pass but are simply not registered
    on WhatsApp surface through /otp/status once Meta reports back.
    """
    resolved = normalize_indian_mobile(body.phone)
    if not resolved["valid"]:
        raise HTTPException(
            status_code=400,
            detail=resolved["reason"] or "Enter a valid Indian mobile number",
        )
    phone10 = resolved["phone"]

    db = _db()

    # Anti-spam: one code per number per cooldown window.
    existing = db.otp_codes.find_one(
        {"phone": phone10, "purpose": OTP_PURPOSE}, {"created_at": 1}
    )
    if existing and existing.get("created_at"):
        age = (datetime.utcnow() - existing["created_at"]).total_seconds()
        if age < OTP_RESEND_COOLDOWN_SECONDS:
            raise HTTPException(
                status_code=429,
                detail=f"Please wait {int(OTP_RESEND_COOLDOWN_SECONDS - age)}s before requesting another code",
            )

    template = db.templates.find_one({"name": OTP_TEMPLATE_NAME})
    if not template:
        raise HTTPException(
            status_code=503,
            detail="OTP delivery is temporarily unavailable. Please try again later.",
        )

    code = f"{secrets.randbelow(1_000_000):06d}"
    db.otp_codes.update_one(
        {"phone": phone10, "purpose": OTP_PURPOSE},
        {
            "$set": {
                "phone": phone10,
                "purpose": OTP_PURPOSE,
                "code_hash": _hash_otp(code),
                "expires_at": datetime.utcnow() + timedelta(seconds=OTP_EXPIRE_SECONDS),
                "attempts": 0,
                "consumed": False,
                "created_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )

    result = send_template_message(phone10, template, {"otp": code})
    _record_send(
        phone10, result.get("message_uuid"), result.get("status"), result.get("error")
    )

    if result.get("status") in _UNREACHABLE_STATUSES:
        # Plivo refused it outright — no point making them wait on a poll.
        raise HTTPException(
            status_code=400,
            detail="We could not reach this number on WhatsApp. Please use a WhatsApp-enabled mobile number.",
        )

    return {
        "success": True,
        "message": "Code sent on WhatsApp",
        "phone": phone10,
        "expires_in": OTP_EXPIRE_SECONDS,
    }


@router.get("/otp/status")
async def otp_status(phone: str = Query(...)):
    """Polled while the code is in flight — reports a number that turned out not
    to be on WhatsApp, so the form can ask for a different one."""
    resolved = normalize_indian_mobile(phone)
    if not resolved["valid"]:
        raise HTTPException(status_code=400, detail="Enter a valid Indian mobile number")
    return _resolve_reachability(resolved["phone"])


@router.post("/otp/verify")
async def verify_otp(body: OtpVerifyBody):
    """Exchange a correct code for a short-lived token proving phone ownership."""
    resolved = normalize_indian_mobile(body.phone)
    if not resolved["valid"]:
        raise HTTPException(status_code=400, detail="Enter a valid Indian mobile number")
    phone10 = resolved["phone"]

    db = _db()
    entry = db.otp_codes.find_one({"phone": phone10, "purpose": OTP_PURPOSE})

    if not entry or entry.get("consumed"):
        raise HTTPException(status_code=400, detail="Request a new code")
    if entry["expires_at"] < datetime.utcnow():
        db.otp_codes.delete_one({"_id": entry["_id"]})
        raise HTTPException(status_code=400, detail="Code expired. Request a new one")
    if entry.get("attempts", 0) >= OTP_MAX_VERIFY_ATTEMPTS:
        db.otp_codes.delete_one({"_id": entry["_id"]})
        raise HTTPException(
            status_code=429, detail="Too many attempts. Request a new code"
        )
    if not hmac.compare_digest(entry["code_hash"], _hash_otp(body.code)):
        db.otp_codes.update_one({"_id": entry["_id"]}, {"$inc": {"attempts": 1}})
        remaining = OTP_MAX_VERIFY_ATTEMPTS - (entry.get("attempts", 0) + 1)
        raise HTTPException(
            status_code=400, detail=f"Incorrect code. {max(remaining, 0)} attempts left"
        )

    # Single-use: consumed on success so the code can never be replayed.
    db.otp_codes.delete_one({"_id": entry["_id"]})
    # A code that was actually read proves the number is on WhatsApp, whatever
    # the delivery report said.
    _record_send(phone10, (db.whatsapp_reachability.find_one({"phone": phone10}) or {}).get("message_uuid"), "delivered")

    return {
        "success": True,
        "verified": True,
        "phone": phone10,
        "verificationToken": make_verified_phone_token(phone10),
    }


@router.post("")
async def create_distributor_registration(request: DistributorRegistrationRequest):
    try:
        db = _db()

        verified_phone = resolve_verified_phone_token(request.verificationToken)
        if not verified_phone:
            raise HTTPException(
                status_code=401,
                detail="Your phone verification expired. Please verify your number again.",
            )

        resolved = normalize_indian_mobile(request.phone)
        if not resolved["valid"] or resolved["phone"] != verified_phone:
            raise HTTPException(
                status_code=400,
                detail="The mobile number does not match the verified number.",
            )

        if not request.gstNumber and not request.panNumber:
            raise HTTPException(
                status_code=400, detail="Provide either a GST number or a PAN"
            )

        registration = {
            "company_name": request.companyName,
            "gst_number": request.gstNumber,
            "pan_number": request.panNumber,
            "billing_address": request.billingAddress.dict(),
            "ship_from_address": request.shipFromAddress.dict(),
            "phone": verified_phone,
            "phone_verified": True,
            "email": request.email,
            "contact_person_name": request.contactPersonName,
            "brand_name": request.brandName,
            "categories": request.categories,
            "distribution_states": request.distributionStates,
            "margin": request.margin.strip(),
            "status": "not_contacted",
            "notes": "",
            "created_at": now_ist(),
        }

        result = db.distributor_registrations.insert_one(registration)

        # Notify the leads admin of the new distributor application
        try:
            from .notifications import (
                create_notifications_for_emails,
                LEAD_NOTIFICATION_EMAILS,
            )

            create_notifications_for_emails(
                db,
                LEAD_NOTIFICATION_EMAILS,
                "new_lead",
                f"New distributor application: {request.brandName}",
                f"{request.companyName} applied to distribute {request.brandName} "
                f"({', '.join(request.categories)}) across "
                f"{len(request.distributionStates)} state(s).",
                "/admin/leads?tab=distributors",
            )
        except Exception as e:
            print(f"Failed to notify of distributor registration: {e}")

        return {
            "success": True,
            "message": "Distributor registration saved successfully",
            "id": str(result.inserted_id),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error saving distributor registration: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to save registration: {str(e)}"
        )
