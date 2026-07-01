from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks, Request, Response
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import JWTBearer
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.hash import bcrypt
from bson.objectid import ObjectId
import os, time
from collections import defaultdict

router = APIRouter()

db = get_database()

# ── Constants ─────────────────────────────────────────────────────────────────
# Issue 7: reduced from 7 days → 24 hours
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 1440))
PASSWORD_RESET_TOKEN_EXPIRE_MINUTES = 60

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
FRONTEND_RESET_URL = os.getenv("FRONTEND_RESET_URL")

users_collection = db["users"]
password_resets_collection = db["password_resets"]


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


# ── Pydantic models ───────────────────────────────────────────────────────────
class UserCreate(BaseModel):
    email: EmailStr
    password: str


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
