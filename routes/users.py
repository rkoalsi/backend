from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document, parse_data  # type: ignore
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.hash import bcrypt
from bson import ObjectId
import smtplib, os
from email.mime.text import MIMEText

router = APIRouter()

client, db = connect_to_mongo()


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
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

client, db = connect_to_mongo()
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
    return parse_data(user)


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# Helper function to send reset email
def send_reset_email(to_email: str, reset_link: str):
    subject = "Password Reset Request"
    body = f"""
    Hi,

    You requested a password reset. Click the link below to reset your password:

    {reset_link}

    If you did not request this, please ignore this email.

    Thanks,
    Pupscribe Team
    """
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = RESET_EMAIL_SENDER
    msg["To"] = to_email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(RESET_EMAIL_SENDER, [to_email], msg.as_string())
    except Exception as e:
        print(f"Error sending email: {e}")
        raise HTTPException(status_code=500, detail="Failed to send reset email")


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
        "user_id": user["_id"]["$oid"],
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
