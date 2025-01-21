from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document, parse_data  # type: ignore
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.hash import bcrypt
from bson import ObjectId

router = APIRouter()

client, db = connect_to_mongo()

# Password hashing setup

SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

client, db = connect_to_mongo()
users_collection = db["users"]


def verify_password(plain_password, hashed_password):
    return bcrypt.verify(plain_password, hashed_password)


def hash_password(password):
    return bcrypt.hash(password)


def find_user_by_email(email: str):
    user = db.users.find_one({"email": email})
    return True if user else False


def authenticate_user(email: str, password: str):
    user = db.users.find_one({"email": email})
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
    existing_user = find_user_by_email(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash the password
    hashed_password = hash_password(user.password)

    # Insert user into the database
    user_data = {"email": user.email, "password": hashed_password}
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


@router.get("/salespeople")
def salespeople():
    users_cursor = db.users.find({"role": "sales_person"})
    sales_people = list(users_cursor)

    # Prepare the result
    for sales_person in sales_people:
        sales_person_code = sales_person.get("code")

        if sales_person_code:
            # Fetch customers assigned to the salesperson
            customers_cursor = db.customers.find(
                {
                    "$or": [
                        {
                            "cf_sales_person": {
                                "$regex": f"\\b{sales_person_code}\\b",
                                "$options": "i",
                            }
                        },
                        {"cf_sales_person": "Defaulter"},
                        {"cf_sales_person": "Company customers"},
                    ],
                    "status": "active",
                }
            )
            sales_person["customers"] = serialize_mongo_document(list(customers_cursor))
        else:
            # Assign customers with "Defaulter" or "Company customers" to all salespeople
            customers_cursor = db.customers.find(
                {
                    "$or": [
                        {"cf_sales_person": "Defaulter"},
                        {"cf_sales_person": "Company customers"},
                    ],
                    "status": "active",
                }
            )
            sales_person["customers"] = serialize_mongo_document(list(customers_cursor))

    return {"users": serialize_mongo_document(sales_people)}


@router.get("/salespeoples/customers")
def salespeople():
    users_cursor = db.users.find({"role": "sales_person"})
    users = serialize_mongo_document(list(users_cursor))
    return {"users": users}


@router.put("/salespeople/{salesperson_id}")
def salespeople(salesperson_id: str, salesperson: dict):
    update_data = {k: v for k, v in salesperson.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # Perform the update
    result = db.users.update_one(
        {"_id": ObjectId(salesperson_id)},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sales Person not found")
    return {"message": "Sales Person Updated"}
