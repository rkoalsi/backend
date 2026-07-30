"""Invite-only distributor / brand onboarding form.

There is no public distributor sign-up any more. An admin creates an invite in
/admin/distributor_invites, which mints a single unguessable token, and sends the
resulting link to the brand. Every endpoint below is scoped to that token — the
link *is* the authorisation, so nothing here is reachable without one.

The phone number on the form is still verified over WhatsApp before the
application is accepted:

  1. POST /{token}/otp/request  — the number is checked for WhatsApp
                                  reachability, then a 6-digit code is sent.
  2. GET  /{token}/otp/status   — polled while the code is in flight; reports
                                  back when Meta tells us the number is not on
                                  WhatsApp.
  3. POST /{token}/otp/verify   — exchanges a correct code for a short-lived
                                  signed token.
  4. POST /{token}              — the application itself, which only saves when
                                  it carries a valid token for the phone number
                                  on the form.

There is no way to check WhatsApp registration synchronously through Plivo, so
reachability is established in two stages: a cheap local gate that rejects
landlines and malformed numbers up front, and the asynchronous delivery report
for the OTP message, which is where a genuinely non-WhatsApp number shows up as
`failed`/`undelivered`.

An invite stays usable until an admin revokes it, so a brand can come back to
the same link and correct what they submitted — the resubmission updates the
existing application rather than creating a second one.
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

# Kept in sync with the CATEGORIES list on the invite form. A brand whose
# category is not on the list can type their own — see the categories validator,
# which keeps a known value verbatim (so the admin filters keep working) and
# otherwise accepts the free text as-is.
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

# Mirrors STEPS on pages/distributor_registration/[token].tsx. Kept here so the
# admin table and the form agree on what "step 3" means.
STEP_LABELS = ["Verify mobile", "Company", "Addresses", "Brand"]

# Delivery states that mean the number is not on WhatsApp (Meta error 131026,
# "message undeliverable") or the send was refused outright.
_UNREACHABLE_STATUSES = {"failed", "undelivered", "rejected"}


def now_ist():
    return datetime.now(IST)


def _db():
    return get_database()


# ── Invite lookup ─────────────────────────────────────────────────────────────


def resolve_invite(token: str) -> dict:
    """Return the invite for `token`, or raise the reason it is unusable.

    Every endpoint on this router funnels through here, so a revoked link stops
    working mid-form rather than only at submit time.
    """
    token = (token or "").strip()
    if not token:
        raise HTTPException(status_code=404, detail="This registration link is not valid")

    invite = _db().distributor_invites.find_one({"token": token})
    if not invite:
        raise HTTPException(
            status_code=404,
            detail="This registration link is not valid. Please ask your Pupscribe contact for a new one.",
        )
    if invite.get("status") == "revoked":
        raise HTTPException(
            status_code=410,
            detail="This registration link has been closed. Please ask your Pupscribe contact for a new one.",
        )
    return invite


# ── Draft progress ────────────────────────────────────────────────────────────
# The form saves what has been typed after every step, so /admin/distributor_invites
# can show how far a brand got and the brand can close the tab and come back.
#
# A draft is partial by definition, so it deliberately does NOT go through
# DistributorRegistrationRequest — half a form would never pass those
# validators. It is instead whitelisted and length-capped here, and only becomes
# a real application through POST /{token}, which does validate in full.

_DRAFT_TEXT_FIELDS = (
    "companyName",
    "gstNumber",
    "panNumber",
    "email",
    "contactPersonName",
    "brandName",
    "margin",
    "phone",
)
_DRAFT_ADDRESS_FIELDS = ("billingAddress", "shipFromAddress")
_DRAFT_LIST_FIELDS = ("categories", "distributionStates")
_ADDRESS_KEYS = (
    "address",
    "street2",
    "city",
    "state",
    "zip",
    "phone",
    "attention",
    "country",
)

_DRAFT_MAX_TEXT = 200
_DRAFT_MAX_LIST = 40


def _draft_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()[:_DRAFT_MAX_TEXT]


def sanitize_draft(data) -> dict:
    """Whitelist and cap an in-progress form payload.

    Anything unrecognised is dropped rather than rejected — a draft save must
    never fail loudly and cost the brand what they have typed.
    """
    if not isinstance(data, dict):
        return {}

    cleaned: dict = {}
    for key in _DRAFT_TEXT_FIELDS:
        if key in data:
            cleaned[key] = _draft_text(data.get(key))
    for key in _DRAFT_ADDRESS_FIELDS:
        raw = data.get(key)
        if isinstance(raw, dict):
            cleaned[key] = {k: _draft_text(raw.get(k)) for k in _ADDRESS_KEYS}
    for key in _DRAFT_LIST_FIELDS:
        raw = data.get(key)
        if isinstance(raw, list):
            cleaned[key] = [
                _draft_text(item) for item in raw[:_DRAFT_MAX_LIST] if _draft_text(item)
            ]
    return cleaned


def invite_progress(invite: dict) -> dict:
    """How far this brand has got, as one object both the admin table and the
    form read.

    `step` is the furthest step saved, and survives a submission — a brand that
    comes back to correct something is "filled in, currently editing step 2",
    not back to square one.
    """
    draft_step = invite.get("draft_step")
    step = draft_step if isinstance(draft_step, int) else None

    if invite.get("registration_id"):
        stage = "submitted"
    elif step is not None:
        stage = "in_progress"
    elif invite.get("phone_verified_at"):
        stage = "verified"
    elif invite.get("opened_at"):
        stage = "opened"
    else:
        stage = "not_opened"

    return {
        "stage": stage,
        "step": step,
        "step_label": STEP_LABELS[step] if step is not None and 0 <= step < len(STEP_LABELS) else "",
        "total_steps": len(STEP_LABELS),
        "opened_at": invite.get("opened_at"),
        "last_opened_at": invite.get("last_opened_at"),
        "phone_verified_at": invite.get("phone_verified_at"),
        "draft_updated_at": invite.get("draft_updated_at"),
    }


# ── Verified-phone token ──────────────────────────────────────────────────────
# Same construction as the login-link token in users.py: an opaque, signed,
# expiring stand-in for a phone number. Holding one proves only that whoever has
# it answered an OTP on that number a few minutes ago. The invite token is baked
# into the signature so a code verified on one invite cannot submit another.


def _token_signature(payload: str) -> str:
    digest = hmac.new(
        (SECRET_KEY or "").encode(), payload.encode(), hashlib.sha256
    ).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")[:22]


def make_verified_phone_token(phone10: str, invite_token: str) -> str:
    payload = (
        f"{phone10}.{invite_token}.{int(time.time()) + VERIFIED_TOKEN_EXPIRE_SECONDS}"
    )
    encoded = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    return f"{encoded}.{_token_signature(payload)}"


def resolve_verified_phone_token(token: str, invite_token: str) -> Optional[str]:
    """Return the 10-digit phone for a valid, unexpired token that was issued
    against `invite_token`, else None."""
    try:
        encoded, signature = str(token).split(".")
        payload = base64.urlsafe_b64decode(
            encoded + "=" * (-len(encoded) % 4)
        ).decode()
        phone10, token_invite, expires_at = payload.split(".")
        if not hmac.compare_digest(signature, _token_signature(payload)):
            return None
        if not hmac.compare_digest(token_invite, invite_token):
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


class DraftBody(BaseModel):
    """An in-progress form: the step the brand is on, plus whatever they have
    typed so far. Both are best-effort — see sanitize_draft."""

    step: int = 0
    data: dict = {}

    @validator("step")
    def validate_step(cls, v):
        try:
            v = int(v)
        except (TypeError, ValueError):
            raise ValueError("Invalid step")
        if not 0 <= v < len(STEP_LABELS):
            raise ValueError("Invalid step")
        return v


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
    # Proof that `phone` answered an OTP (from /{token}/otp/verify)
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


@router.get("/{token}")
async def get_invite(token: str):
    """Everything the invite form needs in one request: whether the link is
    live, what the admin pre-filled, the dropdown options (so the frontend can
    never drift from the server-side validator) and — on a return visit — what
    was submitted last time, so the brand edits rather than retypes.
    """
    invite = resolve_invite(token)
    db = _db()

    # First open is what tells the admin the link actually reached someone;
    # `opened_at` is never overwritten, so it stays the first-contact timestamp.
    # `None` matches a missing field in Mongo, so this sets it exactly once.
    db.distributor_invites.update_one(
        {"_id": invite["_id"], "opened_at": None},
        {"$set": {"opened_at": now_ist()}},
    )
    db.distributor_invites.update_one(
        {"_id": invite["_id"]}, {"$set": {"last_opened_at": now_ist()}}
    )

    submission = None
    registration_id = invite.get("registration_id")
    if registration_id:
        doc = _db().distributor_registrations.find_one({"_id": registration_id})
        if doc:
            submission = {
                "companyName": doc.get("company_name", ""),
                "gstNumber": doc.get("gst_number", ""),
                "panNumber": doc.get("pan_number", ""),
                "billingAddress": doc.get("billing_address") or {},
                "shipFromAddress": doc.get("ship_from_address") or {},
                "phone": doc.get("phone", ""),
                "email": doc.get("email", ""),
                "contactPersonName": doc.get("contact_person_name", ""),
                "brandName": doc.get("brand_name", ""),
                "categories": doc.get("categories") or [],
                "distributionStates": doc.get("distribution_states") or [],
                "margin": doc.get("margin", ""),
            }

    return {
        "valid": True,
        "note": invite.get("note", "") or "",
        "prefill": {
            "companyName": invite.get("company_name") or "",
            "brandName": invite.get("brand_name") or "",
            "contactPersonName": invite.get("contact_person_name") or "",
            "email": invite.get("email") or "",
            "phone": invite.get("phone") or "",
        },
        "submission": submission,
        # What they had typed when they last closed the tab, and where they were
        # — the form restores both once the phone is verified again.
        "draft": invite.get("draft") or None,
        "draftStep": invite.get("draft_step"),
        # Populated once the agreement upload lands; the form shows a download
        # link when it is set and simply omits the section while it is empty.
        "agreementUrl": invite.get("agreement_url") or "",
        "categories": CATEGORIES,
        "states": INDIAN_STATES,
    }


@router.put("/{token}/draft")
async def save_draft(token: str, body: DraftBody):
    """Autosaved by the form as the brand moves through it.

    Losing a draft save is not worth an error the brand has to act on, so this
    stays deliberately permissive: unknown fields are dropped, nothing is
    validated for completeness, and the reply carries no state the form needs.
    """
    invite = resolve_invite(token)

    _db().distributor_invites.update_one(
        {"_id": invite["_id"]},
        {
            "$set": {
                "draft": sanitize_draft(body.data),
                "draft_step": body.step,
                "draft_updated_at": now_ist(),
            }
        },
    )
    return {"success": True}


@router.post("/{token}/otp/request")
async def request_otp(token: str, body: OtpRequestBody):
    """Send a WhatsApp OTP to the applicant's mobile number.

    The number is gated locally first: `normalize_indian_mobile` rejects
    landlines, short codes and fields holding two numbers, none of which can
    receive a WhatsApp message. Numbers that pass but are simply not registered
    on WhatsApp surface through /otp/status once Meta reports back.
    """
    resolve_invite(token)

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


@router.get("/{token}/otp/status")
async def otp_status(token: str, phone: str = Query(...)):
    """Polled while the code is in flight — reports a number that turned out not
    to be on WhatsApp, so the form can ask for a different one."""
    resolve_invite(token)

    resolved = normalize_indian_mobile(phone)
    if not resolved["valid"]:
        raise HTTPException(status_code=400, detail="Enter a valid Indian mobile number")
    return _resolve_reachability(resolved["phone"])


@router.post("/{token}/otp/verify")
async def verify_otp(token: str, body: OtpVerifyBody):
    """Exchange a correct code for a short-lived token proving phone ownership."""
    invite = resolve_invite(token)

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

    # Reaching this point is the first hard signal that a real person is behind
    # the link, so it shows in /admin/distributor_invites even if they go no
    # further.
    db.distributor_invites.update_one(
        {"_id": invite["_id"]},
        {"$set": {"phone_verified_at": now_ist(), "verified_phone": phone10}},
    )

    return {
        "success": True,
        "verified": True,
        "phone": phone10,
        "verificationToken": make_verified_phone_token(phone10, invite["token"]),
    }


@router.post("/{token}")
async def create_distributor_registration(
    token: str, request: DistributorRegistrationRequest
):
    try:
        db = _db()
        invite = resolve_invite(token)

        verified_phone = resolve_verified_phone_token(
            request.verificationToken, invite["token"]
        )
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
            "invite_id": invite["_id"],
            "updated_at": now_ist(),
        }

        # The link stays live until an admin revokes it, so a brand can come back
        # and fix a typo. Keying the upsert on the invite makes a return visit
        # update their application instead of filing a duplicate for the sales
        # team to reconcile.
        existing = db.distributor_registrations.find_one(
            {"invite_id": invite["_id"]}, {"_id": 1}
        )
        result = db.distributor_registrations.update_one(
            {"invite_id": invite["_id"]},
            {
                "$set": registration,
                "$setOnInsert": {
                    "status": "not_contacted",
                    "notes": "",
                    "created_at": now_ist(),
                },
            },
            upsert=True,
        )
        is_update = existing is not None
        registration_id = existing["_id"] if is_update else result.upserted_id

        # The draft is scaffolding for an unfinished form; once it has been
        # submitted the application itself is the record, and a leftover
        # draft_step would read as "still on step 3" in the admin table.
        db.distributor_invites.update_one(
            {"_id": invite["_id"]},
            {
                "$set": {
                    "registration_id": registration_id,
                    "last_submitted_at": now_ist(),
                },
                "$inc": {"submission_count": 1},
                "$unset": {"draft": "", "draft_step": "", "draft_updated_at": ""},
            },
        )

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
                f"Distributor application {'updated' if is_update else 'received'}: {request.brandName}",
                f"{request.companyName} "
                f"{'updated their application to distribute' if is_update else 'applied to distribute'} "
                f"{request.brandName} ({', '.join(request.categories)}) across "
                f"{len(request.distributionStates)} state(s).",
                "/admin/leads?tab=distributors",
            )
        except Exception as e:
            print(f"Failed to notify of distributor registration: {e}")

        return {
            "success": True,
            "message": "Distributor registration saved successfully",
            "id": str(registration_id),
            "updated": is_update,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error saving distributor registration: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to save registration: {str(e)}"
        )
