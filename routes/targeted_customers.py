from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Body,
)
from fastapi.responses import JSONResponse, StreamingResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, pytz

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
targeted_customers_collection = db["targeted_customers"]


@router.get("")
async def get_all_potential_customers(user: str):
    try:
        print(user)
        pcs = list(
            db.targeted_customers.find(
                {"sales_people": {"$in": [ObjectId(user)]}}
            ).sort({"created_at": -1})
        )
        for pc in pcs:
            # Convert created_at from UTC to IST if it exists
            if "created_at" in pc:
                utc_dt = pc["created_at"]
                # Ensure the datetime is timezone aware; assume it's UTC if not
                if utc_dt.tzinfo is None:
                    utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
                # Convert to IST (UTC+5:30)
                ist_timezone = pytz.timezone("Asia/Kolkata")
                ist_dt = utc_dt.astimezone(ist_timezone)
                # Format the datetime as a string, e.g., "YYYY-MM-DD HH:MM:SS"
                pc["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")
        return serialize_mongo_document(pcs)
    except Exception as e:
        return e
