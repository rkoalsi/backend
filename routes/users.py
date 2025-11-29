from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from ..config.root import get_database, serialize_mongo_document
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.hash import bcrypt
import os

router = APIRouter()

db = get_database()


# Define constants for password reset
PASSWORD_RESET_TOKEN_EXPIRE_MINUTES = 60  # Token valid for 1 hour

RESET_EMAIL_SENDER = os.getenv("RESET_EMAIL_SENDER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FRONTEND_RESET_URL = os.getenv("FRONTEND_RESET_URL")
# Password hashing setup

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = 1440 * 7

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

db = get_database()
users_collection = db["users"]
password_resets_collection = db["password_resets"]


def verify_password(plain_password, hashed_password):
    return bcrypt.verify(plain_password, hashed_password)


def hash_password(password):
    return bcrypt.hash(password)


def find_user_by_email(email: str):
    user = db.users.find_one({"email": email})
    return True if user else False


def authenticate_user(email: str, password: str):
    user = db.users.find_one({"email": email, "status": "active"})
    if not user or not verify_password(password, user["password"]):
        return False
    return serialize_mongo_document(user)


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def send_reset_email(to_email: str, reset_link: str):
    """Simple HTTP-based email sending."""
    import requests
    
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {os.getenv('RESEND_API_KEY')}",
        "Content-Type": "application/json"
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
                    This link will expire in 24 hours.
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                <p style="color: #999; font-size: 12px;">
                    Thanks,<br>The Pupscribe Team
                </p>
            </div>
            """
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Email sent! ID: {result.get('id')}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ HTTP request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return False
    
# Pydantic models
class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


# Pydantic models for password reset
class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str


@router.post("/register")
async def register_user(user: dict):
    # Check if user already exists
    existing_user = find_user_by_email(user.get("email", ""))
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash the password
    hashed_password = hash_password(user.get("password"))

    # Insert user into the database
    user_data = {"email": user.get("email", ""), "password": hashed_password}
    result = users_collection.insert_one(user_data)
    if not result:
        raise HTTPException(status_code=400, detail="Error inserting user in database")

    access_token = create_access_token(data={"data": user["email"]})
    return {
        "message": "User registered successfully",
        "user_id": str(result.inserted_id),
        "access_token": access_token,
    }


@router.post("/login")
async def login_user(user: UserLogin):
    # Find user by email
    user = authenticate_user(user.email, user.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"data": user})
    return {
        "message": "Login successful",
        "user_id": user["_id"],
        "user": user,
        "access_token": access_token,
    }


@router.get("/me")
async def read_users_me(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("data")
        if email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication",
            )
        return {"email": email}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


# Endpoint to request password reset
@router.post("/forgot_password")
async def forgot_password(
    request: PasswordResetRequest, background_tasks: BackgroundTasks
):
    user = db.users.find_one({"email": request.email})
    if not user:
        # To prevent email enumeration, respond with a generic message
        return {"message": "If the email exists, a reset link has been sent."}

    # Generate a reset token
    reset_token = create_access_token(
        data={"email": request.email},
        expires_delta=timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES),
    )

    # Store the reset token in the database
    reset_entry = {
        "email": request.email,
        "token": reset_token,
        "expires_at": datetime.utcnow()
        + timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES),
    }
    password_resets_collection.insert_one(reset_entry)

    # Create the reset link
    reset_link = f"{FRONTEND_RESET_URL}?token={reset_token}"

    # Send the reset email in the background
    background_tasks.add_task(send_reset_email, request.email, reset_link)

    return {"message": "If the email exists, a reset link has been sent."}


# Endpoint to reset the password
@router.post("/reset_password")
async def reset_password(confirm: PasswordResetConfirm):
    try:
        # Decode the token to get the email
        payload = jwt.decode(confirm.token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("email")
        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid token")

    # Retrieve the reset entry from the database
    reset_entry = password_resets_collection.find_one(
        {"token": confirm.token, "email": email}
    )
    if not reset_entry:
        raise HTTPException(status_code=400, detail="Invalid or expired token")

    if reset_entry["expires_at"] < datetime.utcnow():
        # Token has expired
        password_resets_collection.delete_one({"_id": reset_entry["_id"]})
        raise HTTPException(status_code=400, detail="Token has expired")

    # Hash the new password
    hashed_password = hash_password(confirm.new_password)

    # Update the user's password in the database
    db.users.update_one({"email": email}, {"$set": {"password": hashed_password}})

    # Remove the used reset token
    password_resets_collection.delete_one({"_id": reset_entry["_id"]})

    return {"message": "Password has been reset successfully"}
