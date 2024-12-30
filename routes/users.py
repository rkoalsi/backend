from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, disconnect_on_exit, parse_data  # type: ignore
from pydantic import BaseModel, EmailStr
from bson.objectid import ObjectId
from passlib.context import CryptContext

router = APIRouter()

client, db = connect_to_mongo()

# Password hashing setup
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
client, db = connect_to_mongo()
users_collection = db["users"]


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


# Pydantic models
class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


@router.post("/register")
async def register_user(user: UserCreate):
    # Check if user already exists
    existing_user = users_collection.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash the password
    hashed_password = hash_password(user.password)

    # Insert user into the database
    user_data = {"email": user.email, "password": hashed_password}
    result = users_collection.insert_one(user_data)

    return {
        "message": "User registered successfully",
        "user_id": str(result.inserted_id),
    }


@router.post("/login")
async def login_user(user: UserLogin):
    # Find user by email
    db_user = users_collection.find_one({"email": user.email})
    if not db_user:
        raise HTTPException(status_code=400, detail="No User Found with Email")

    # Verify password
    if not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=400, detail="Invalid email or password")

    return {"message": "Login successful", "user_id": str(db_user["_id"])}


@router.get("/", response_class=HTMLResponse)
def index():
    return "<h1>Backend is running<h1>"
