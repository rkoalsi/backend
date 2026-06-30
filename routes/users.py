from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks, Request, Response
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.hash import bcrypt
from bson.objectid import ObjectId
import os, time, secrets, hashlib, hmac
from typing import Optional
from collections import defaultdict

router = APIRouter()

db = get_database()

# ── Constants ─────────────────────────────────────────────────────────────────
# Issue 7: reduced from 7 days → 24 hours
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 1440))
PASSWORD_RESET_TOKEN_EXPIRE_MINUTES = 60
# (registration token removed — OTP verification now creates the account directly)

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
FRONTEND_RESET_URL = os.getenv("FRONTEND_RESET_URL")

users_collection = db["users"]
password_resets_collection = db["password_resets"]
otp_collection = db["otp_codes"]
# Funnel capture for B2B self-registration: one doc per number the moment an OTP
# is requested (before any account exists), so drop-offs are still recorded.
b2b_leads_collection = db["b2b_leads"]

# ── OTP / mobile-auth constants ────────────────────────────────────────────────
OTP_EXPIRE_SECONDS = int(os.getenv("OTP_EXPIRE_SECONDS", 300))          # 5 minutes
OTP_MAX_VERIFY_ATTEMPTS = 5
# Plivo/WhatsApp template (must be approved in Plivo + present in db.templates)
# carrying a single body parameter for the 6-digit code.
OTP_TEMPLATE_NAME = os.getenv("OTP_TEMPLATE_NAME", "otp_verification")
# Minimum seconds between OTP sends to the same number (anti-spam / anti-enumeration).
OTP_RESEND_COOLDOWN_SECONDS = int(os.getenv("OTP_RESEND_COOLDOWN_SECONDS", 30))

# Harden the OTP store:
#  • TTL index → Mongo auto-deletes each code the moment it expires (no stale codes
#    linger, even after a crash that skips the in-code cleanup).
#  • unique (phone, purpose) → at most one live code per number+purpose, so a new
#    request always supersedes the old one (matches the upsert in issue_otp).
try:
    otp_collection.create_index("expires_at", expireAfterSeconds=0)
    otp_collection.create_index([("phone", 1), ("purpose", 1)], unique=True)
except Exception:
    pass


# ── Issue 9: simple in-memory rate limiter (no extra dependencies) ─────────────
class _RateLimiter:
    """Per-key sliding-window rate limiter backed by in-process memory.
    Fine for single-process deployments; swap for Redis if you scale out."""

    def __init__(self, max_attempts: int, window_seconds: int):
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self._log: dict[str, list[float]] = defaultdict(list)

    def is_limited(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        log = self._log[key]
        # evict stale entries
        self._log[key] = [t for t in log if t > cutoff]
        if len(self._log[key]) >= self.max_attempts:
            return True
        self._log[key].append(now)
        return False


_login_limiter = _RateLimiter(max_attempts=5, window_seconds=300)       # 5 / 5 min
_reset_limiter = _RateLimiter(max_attempts=3, window_seconds=3600)       # 3 / 1 hr
_otp_limiter = _RateLimiter(max_attempts=5, window_seconds=600)          # 5 / 10 min per phone


# ── Helpers ───────────────────────────────────────────────────────────────────
def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.verify(plain, hashed)


def hash_password(password: str) -> str:
    return bcrypt.hash(password)


def find_user_by_email(email: str) -> bool:
    return db.users.find_one({"email": email}) is not None


def authenticate_user(email: str, password: str):
    user = db.users.find_one({"email": email, "status": "active"})
    if not user or not verify_password(password, user["password"]):
        return False
    return serialize_mongo_document(user)


def _minimal_payload(user: dict) -> dict:
    """Issue 6: store only what downstream code actually needs.
    Keeps the existing {"data": {...}} envelope so permissions.py stays compatible."""
    return {
        "_id": str(user["_id"]),
        "email": user.get("email", ""),
        "role": user.get("role", ""),
        "customer_id": user.get("customer_id"),
        "name": user.get("name", ""),
        "code": user.get("code", ""),
    }


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def _set_auth_cookie(response: Response, token: str) -> None:
    """Issue 8: write the token into an HttpOnly cookie so JS cannot read it."""
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,       # no JS access
        secure=os.getenv("ENVIRONMENT", "development") == "production",
        samesite="lax",      # protects against CSRF while allowing same-site nav
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )


def send_reset_email(to_email: str, reset_link: str) -> bool:
    import requests as req_lib

    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {os.getenv('RESEND_API_KEY')}",
        "Content-Type": "application/json",
    }
    data = {
        "from": "no-reply@no-reply.pupscribe.in",
        "to": [to_email],
        "subject": "Password Reset Request",
        "html": f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #333;">Password Reset Request</h2>
                <p>You requested a password reset for your Order Form account.</p>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{reset_link}"
                       style="background-color: #007bff; color: white; padding: 12px 30px;
                              text-decoration: none; border-radius: 5px; display: inline-block;">
                        Reset Your Password
                    </a>
                </div>
                <p style="color: #666; font-size: 14px;">
                    If you did not request this reset, please ignore this email.
                    This link will expire in 1 hour.
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                <p style="color: #999; font-size: 12px;">Thanks,<br>The Pupscribe Team</p>
            </div>
        """,
    }
    try:
        r = req_lib.post(url, headers=headers, json=data, timeout=10)
        r.raise_for_status()
        print(f"✅ Reset email sent: {r.json().get('id')}")
        return True
    except req_lib.exceptions.RequestException as e:
        print(f"❌ Reset email failed: {e}")
        return False


# ── OTP / mobile-auth helpers ──────────────────────────────────────────────────
def normalize_phone(raw) -> str:
    """Reduce any phone input to its last 10 digits (Indian mobile).
    Mirrors the last-10-digit matching used elsewhere (chatbot is_b2b match)."""
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


def find_user_by_phone(phone10: str) -> Optional[dict]:
    """Find an active account whose stored phone matches these last-10 digits.
    Works for any role (customer / sales_person / admin). `phone` is stored as an
    int by existing flows, so match both int and string representations."""
    if not phone10 or len(phone10) != 10:
        return None
    candidates = [phone10]
    try:
        candidates.append(int(phone10))
    except ValueError:
        pass
    # Require an active account — mirrors email/password login (authenticate_user),
    # and keeps pending self-registered users blocked until an admin approves them.
    return db.users.find_one({"phone": {"$in": candidates}, "status": "active"})


def capture_b2b_lead(phone10: str) -> None:
    """Record a B2B self-registration lead in the `b2b_leads` collection the moment
    a number is entered for OTP — BEFORE any user account exists. This captures
    drop-offs (people who request an OTP but never verify). Idempotent per number."""
    now = datetime.utcnow()
    b2b_leads_collection.update_one(
        {"phone": int(phone10)},
        {
            "$setOnInsert": {"phone": int(phone10), "created_at": now, "verified": False},
            "$set": {"updated_at": now},
            "$inc": {"otp_requests": 1},
        },
        upsert=True,
    )


def _hash_otp(code: str) -> str:
    # Keyed (HMAC) hash so a leaked DB row can't be brute-forced without SECRET_KEY,
    # and codes are never stored in plaintext.
    return hmac.new(
        (SECRET_KEY or "").encode(), code.encode(), hashlib.sha256
    ).hexdigest()


def otp_on_cooldown(phone10: str, purpose: str) -> bool:
    """True if a code was issued to this number+purpose within the resend window."""
    entry = otp_collection.find_one(
        {"phone": phone10, "purpose": purpose}, {"created_at": 1}
    )
    if not entry or not entry.get("created_at"):
        return False
    age = (datetime.utcnow() - entry["created_at"]).total_seconds()
    return age < OTP_RESEND_COOLDOWN_SECONDS


def issue_otp(phone10: str, purpose: str) -> str:
    """Generate, store (hashed) and return a 6-digit OTP for this phone+purpose."""
    code = f"{secrets.randbelow(1_000_000):06d}"
    otp_collection.update_one(
        {"phone": phone10, "purpose": purpose},
        {
            "$set": {
                "phone": phone10,
                "purpose": purpose,
                "code_hash": _hash_otp(code),
                "expires_at": datetime.utcnow() + timedelta(seconds=OTP_EXPIRE_SECONDS),
                "attempts": 0,
                "consumed": False,
                "created_at": datetime.utcnow(),
            }
        },
        upsert=True,
    )
    return code


def verify_otp(phone10: str, code: str, purpose: str) -> bool:
    """Validate an OTP; consumes it on success, counts attempts, enforces expiry."""
    entry = otp_collection.find_one({"phone": phone10, "purpose": purpose})
    if not entry or entry.get("consumed"):
        return False
    if entry["expires_at"] < datetime.utcnow():
        otp_collection.delete_one({"_id": entry["_id"]})
        return False
    if entry.get("attempts", 0) >= OTP_MAX_VERIFY_ATTEMPTS:
        otp_collection.delete_one({"_id": entry["_id"]})
        return False
    # Constant-time comparison to avoid leaking the code via timing.
    if not hmac.compare_digest(entry["code_hash"], _hash_otp(code)):
        otp_collection.update_one({"_id": entry["_id"]}, {"$inc": {"attempts": 1}})
        return False
    # Single-use: delete on success so the code can never be replayed.
    otp_collection.delete_one({"_id": entry["_id"]})
    return True


def send_otp_whatsapp(phone10: str, code: str) -> bool:
    """Deliver the OTP over WhatsApp via Plivo. Falls back to a log line if the
    template is not configured (so dev environments still work)."""
    try:
        template = db.templates.find_one({"name": OTP_TEMPLATE_NAME})
        if not template:
            print(f"⚠️  OTP template '{OTP_TEMPLATE_NAME}' not found; OTP for {phone10} = {code}")
            return False
        from ..config.whatsapp import send_whatsapp

        send_whatsapp(phone10, template, {"otp": code})
        return True
    except Exception as e:
        print(f"❌ Failed to send OTP WhatsApp to {phone10}: {e}")
        return False


# ── Pydantic models ───────────────────────────────────────────────────────────
class UserCreate(BaseModel):
    email: EmailStr
    password: str


class OtpRequest(BaseModel):
    phone: str
    # "login"  → must already map to an account
    # "register" → must NOT map to an account (new B2B onboarding)
    purpose: str = "login"


class OtpLogin(BaseModel):
    phone: str
    code: str


class OtpVerify(BaseModel):
    phone: str
    code: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/register")
async def register_user(user: UserCreate, response: Response):
    if find_user_by_email(user.email):
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed = hash_password(user.password)
    result = users_collection.insert_one(
        {"email": user.email, "password": hashed, "role": "customer"}
    )
    if not result:
        raise HTTPException(status_code=400, detail="Error inserting user in database")

    # Issue 6: minimal payload
    token_payload = {"_id": str(result.inserted_id), "email": user.email, "role": "customer", "customer_id": None, "name": None}
    access_token = create_access_token(data={"data": token_payload})
    _set_auth_cookie(response, access_token)
    return {
        "message": "User registered successfully",
        "user_id": str(result.inserted_id),
        "access_token": access_token,
    }


@router.post("/login")
async def login_user(
    user: UserLogin,
    response: Response,
    request: Request,
    background_tasks: BackgroundTasks,
):
    # Issue 9: rate-limit by IP
    client_ip = request.client.host if request.client else "unknown"
    if _login_limiter.is_limited(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Please wait 5 minutes before trying again.",
        )

    authenticated = authenticate_user(user.email, user.password)
    if not authenticated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Issue 6: minimal JWT payload
    access_token = create_access_token(data={"data": _minimal_payload(authenticated)})

    # Issue 8: write HttpOnly cookie
    _set_auth_cookie(response, access_token)

    # Log login activity for customer accounts
    if authenticated.get("customer_id"):
        from .customer_activity import log_activity, extract_client_info

        ip, ua = extract_client_info(request)
        customer_name = (
            authenticated.get("contact_name")
            or f"{authenticated.get('first_name', '')} {authenticated.get('last_name', '')}".strip()
        )
        background_tasks.add_task(
            log_activity,
            action="login",
            category="auth",
            user_id=authenticated.get("_id"),
            customer_id=authenticated.get("customer_id"),
            customer_name=customer_name,
            email=authenticated.get("email"),
            metadata={},
            ip_address=ip,
            user_agent=ua,
        )

    # Return the token in the body as well for clients that prefer header-based auth
    return {
        "message": "Login successful",
        "user_id": authenticated["_id"],
        "user": authenticated,
        "access_token": access_token,
    }


@router.post("/logout")
async def logout(response: Response):
    """Issue 8: clear the auth cookie server-side."""
    response.delete_cookie(key="access_token", path="/")
    return {"message": "Logged out successfully"}


@router.get("/me")
async def read_users_me(token: str = Depends(JWTBearer())):
    """Return fresh user data from the database for the authenticated caller."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user_data = payload.get("data", {})

    # Support both new minimal payload (dict with _id) and legacy payload (email string)
    if isinstance(user_data, dict):
        user_id = user_data.get("_id")
    else:
        # Old token: data == email string — look up by email
        user = db.users.find_one({"email": user_data})
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication")
        user = serialize_mongo_document(user)
        user.pop("password", None)
        return {"user": user}

    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication")

    # Guard against stale/malformed _id values (e.g. "undefined" from old sessions)
    try:
        obj_id = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication")

    user = db.users.find_one({"_id": obj_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user = serialize_mongo_document(user)
    user.pop("password", None)
    return {"user": user}


@router.post("/forgot_password")
async def forgot_password(
    request: Request,
    body: PasswordResetRequest,
    background_tasks: BackgroundTasks,
):
    # Issue 9: rate-limit password-reset requests by IP
    client_ip = request.client.host if request.client else "unknown"
    if _reset_limiter.is_limited(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many reset requests. Please wait before trying again.",
        )

    user = db.users.find_one({"email": body.email})
    # Generic message prevents email enumeration
    if not user:
        return {"message": "If the email exists, a reset link has been sent."}

    reset_token = create_access_token(
        data={"email": body.email},
        expires_delta=timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES),
    )
    password_resets_collection.insert_one(
        {
            "email": body.email,
            "token": reset_token,
            "expires_at": datetime.utcnow() + timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES),
        }
    )
    reset_link = f"{FRONTEND_RESET_URL}?token={reset_token}"
    background_tasks.add_task(send_reset_email, body.email, reset_link)
    return {"message": "If the email exists, a reset link has been sent."}


class TourSeenUpdate(BaseModel):
    tour_key: str  # 'home' | 'orders' | 'dashboard'


@router.patch("/tour-seen")
async def mark_tour_seen(body: TourSeenUpdate, token: str = Depends(JWTBearer())):
    """Mark a specific onboarding tour as seen for the authenticated user."""
    if body.tour_key not in {"home", "orders", "dashboard"}:
        raise HTTPException(status_code=400, detail="Invalid tour_key")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user_data = payload.get("data", {})
    user_id = user_data.get("_id") if isinstance(user_data, dict) else None
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication")

    try:
        obj_id = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication")

    users_collection.update_one(
        {"_id": obj_id},
        {"$set": {f"tour_seen.{body.tour_key}": True}},
    )
    return {"message": "Tour marked as seen"}


@router.post("/reset_password")
async def reset_password(confirm: PasswordResetConfirm):
    try:
        payload = jwt.decode(confirm.token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid token")

    reset_entry = password_resets_collection.find_one(
        {"token": confirm.token, "email": email}
    )
    if not reset_entry:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    if reset_entry["expires_at"] < datetime.utcnow():
        password_resets_collection.delete_one({"_id": reset_entry["_id"]})
        raise HTTPException(status_code=400, detail="Token has expired")

    db.users.update_one(
        {"email": email}, {"$set": {"password": hash_password(confirm.new_password)}}
    )
    password_resets_collection.delete_one({"_id": reset_entry["_id"]})
    return {"message": "Password has been reset successfully"}


# ── Mobile / OTP authentication ────────────────────────────────────────────────
# Works for ANY existing account (customer / sales_person / admin) as an
# alternative to email+password, and as the verification step for new B2B
# self-registration. The number must be reachable on WhatsApp (that is how the
# code is delivered) and — for login — must already belong to an account.

@router.post("/otp/request")
async def request_otp(body: OtpRequest):
    phone10 = normalize_phone(body.phone)
    if len(phone10) != 10:
        raise HTTPException(status_code=400, detail="Enter a valid 10-digit mobile number")

    if _otp_limiter.is_limited(phone10):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many OTP requests. Please wait a few minutes and try again.",
        )

    purpose = body.purpose if body.purpose in ("login", "register") else "login"

    if otp_on_cooldown(phone10, purpose):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Please wait {OTP_RESEND_COOLDOWN_SECONDS}s before requesting another OTP.",
        )

    if purpose == "login":
        # Must belong to an existing account, otherwise there is nothing to log in to.
        if not find_user_by_phone(phone10):
            raise HTTPException(
                status_code=404,
                detail="No account is registered with this mobile number.",
            )
    else:  # register
        if find_user_by_phone(phone10):
            raise HTTPException(
                status_code=409,
                detail="This mobile number already has an account. Please log in instead.",
            )
        # Capture the lead immediately so drop-offs (before completing the form)
        # are still recorded under /admin/leads.
        capture_b2b_lead(phone10)

    code = issue_otp(phone10, purpose)
    send_otp_whatsapp(phone10, code)
    # Never leak the code in the response.
    return {"message": "OTP sent to your WhatsApp number", "phone": phone10}


@router.post("/otp/login")
async def login_with_otp(
    body: OtpLogin,
    response: Response,
    request: Request,
    background_tasks: BackgroundTasks,
):
    phone10 = normalize_phone(body.phone)
    if not verify_otp(phone10, body.code, "login"):
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    user = find_user_by_phone(phone10)
    if not user:
        raise HTTPException(status_code=404, detail="No account is registered with this mobile number.")

    authenticated = serialize_mongo_document(user)
    access_token = create_access_token(data={"data": _minimal_payload(authenticated)})
    _set_auth_cookie(response, access_token)

    # Log login activity for customer accounts (mirrors email/password login)
    if authenticated.get("customer_id"):
        from .customer_activity import log_activity, extract_client_info

        ip, ua = extract_client_info(request)
        customer_name = (
            authenticated.get("contact_name")
            or authenticated.get("customer_name")
            or f"{authenticated.get('first_name', '')} {authenticated.get('last_name', '')}".strip()
        )
        background_tasks.add_task(
            log_activity,
            action="login",
            category="auth",
            user_id=authenticated.get("_id"),
            customer_id=authenticated.get("customer_id"),
            customer_name=customer_name,
            email=authenticated.get("email"),
            metadata={"method": "otp"},
            ip_address=ip,
            user_agent=ua,
        )

    authenticated.pop("password", None)
    return {
        "message": "Login successful",
        "user_id": authenticated["_id"],
        "user": authenticated,
        "access_token": access_token,
    }


@router.post("/otp/verify")
async def verify_registration_otp(body: OtpVerify, response: Response):
    """Verify a registration OTP, create the B2B account (no business details yet)
    and log the user straight in.

    The customer enters nothing but their mobile number here — all business
    details (shop name, GST/PAN, addresses) are filled in later from the
    Account Settings page (mirrors the salesperson customer-creation-request form).
    Until those details are completed and an admin approves, the user has no
    `customer_id` so they can browse but not place orders."""
    phone10 = normalize_phone(body.phone)
    if not verify_otp(phone10, body.code, "register"):
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    now = datetime.utcnow()

    # Mark the lead verified (funnel tracking in b2b_leads).
    b2b_leads_collection.update_one(
        {"phone": int(phone10)},
        {"$set": {"verified": True, "verified_at": now, "updated_at": now}},
        upsert=True,
    )

    # Create the user account on first verification (idempotent on re-verify).
    # No business details captured here — only the verified mobile number.
    existing = find_user_by_phone(phone10)
    if not existing:
        users_collection.update_one(
            {"phone": int(phone10)},
            {
                "$setOnInsert": {
                    "phone": int(phone10),
                    "role": "customer",
                    "status": "active",          # can log in immediately
                    "self_registered": True,     # ← flag for B2B self-onboarded clients
                    "auth_method": "otp",
                    "phone_verified": True,
                    "profile_completed": False,  # details still pending (Account Settings)
                    "customer_id": None,         # no Zoho contact until approved → no ordering
                    "created_at": now,
                },
                "$set": {"updated_at": now},
            },
            upsert=True,
        )

    user = users_collection.find_one({"phone": int(phone10)})
    authenticated = serialize_mongo_document(user)

    # Notify that a new B2B user verified on the purchase portal.
    # TESTING: scoped to a single recipient instead of all admins.
    if not existing:
        try:
            from .notifications import create_notification

            target = db.users.find_one({"email": "rkoalsi2000@gmail.com"}, {"_id": 1})
            if target:
                create_notification(
                    db,
                    str(target["_id"]),
                    "b2b_user_verified",
                    f"New B2B signup verified: +91 {phone10}",
                    "A new customer verified their mobile number on the purchase portal. "
                    "They’ll appear under Leads once they submit their business details.",
                    "/admin/leads?tab=b2b",
                )
        except Exception as e:
            print(f"Failed to notify of B2B verification: {e}")

    # Log the user in (same session/cookie as password & OTP login).
    access_token = create_access_token(data={"data": _minimal_payload(authenticated)})
    _set_auth_cookie(response, access_token)

    authenticated.pop("password", None)
    return {
        "message": "Mobile number verified",
        "user_id": authenticated["_id"],
        "user": authenticated,
        "access_token": access_token,
    }
