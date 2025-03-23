from fastapi import APIRouter, HTTPException, Body, status
from bson import ObjectId
from .helpers import validate_file, process_upload
import pytz, logging
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from datetime import datetime

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


@router.post("")
async def create_potential_customer(data: dict):
    expected_reorders_collection = db["expected_reorders"]
    # # Remove the id field if present, so MongoDB can generate it.
    data["customer_id"] = ObjectId(data["customer_id"])
    data["created_by"] = ObjectId(data["created_by"])
    data["created_at"] = datetime.now()
    result = expected_reorders_collection.insert_one(data)
    if not result:
        raise HTTPException(status_code=404, detail="Potential Customer not created")
    return "Document Created"


@router.get("")
async def get_all_expected_reorders(created_by: str):
    try:
        pcs = list(
            db.expected_reorders.find({"created_by": ObjectId(created_by)}).sort(
                {"created_at": -1}
            )
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


@router.get("/{potential_customer_id}")
def get_hook_by_id(potential_customer_id: str):
    try:
        potential_customer = dict(
            db.expected_reorders.find_one({"_id": ObjectId(potential_customer_id)})
        )
        # Convert created_at from UTC to IST if it exists
        if "created_at" in potential_customer:
            utc_dt = potential_customer["created_at"]
            # Ensure the datetime is timezone aware; assume it's UTC if not
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
            # Convert to IST (UTC+5:30)
            ist_timezone = pytz.timezone("Asia/Kolkata")
            ist_dt = utc_dt.astimezone(ist_timezone)
            # Format the datetime as a string, e.g., "YYYY-MM-DD HH:MM:SS"
            potential_customer["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")
        return serialize_mongo_document(potential_customer)
    except Exception as e:
        return e


@router.put("/{potential_customer_id}")
async def update_potential_customer(potential_customer_id: str, data: dict = Body(...)):
    expected_reorders_collection = db["expected_reorders"]
    # Convert customer and created_by fields to ObjectId.
    # Set an updated timestamp (or update created_at if desired).
    data["updated_at"] = datetime.now()
    # Convert each hook's category_id to category_id as ObjectId.
    data.pop("_id")
    result = expected_reorders_collection.update_one(
        {"_id": ObjectId(potential_customer_id)}, {"$set": data}
    )
    if result.modified_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Potential Customer not updated",
        )
    return "Document Updated"
