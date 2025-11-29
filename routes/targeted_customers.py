from fastapi import (
    APIRouter,
)
from ..config.root import get_database, serialize_mongo_document  
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, pytz, datetime

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
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


@router.post("/save_note")
async def save_note(data: dict):
    try:
        note_text = data.get("notes")  # User's note text
        _id = data.get("_id")  # Document ID
        created_by = data.get("created_by")  # User who created the note

        if not note_text or not _id or not created_by:
            return {"error": "Missing required fields"}

        _id = ObjectId(_id)  # Convert _id to ObjectId
        created_by = ObjectId(created_by)  # Convert created_by to ObjectId

        # Find the document first
        document = db.targeted_customers.find_one({"_id": _id})

        if not document:
            return {"error": "Document not found"}

        # Ensure `notes` field exists before using $push
        if "notes" not in document:
            db.targeted_customers.update_one({"_id": _id}, {"$set": {"notes": []}})

        # Get existing notes
        existing_notes = document.get("notes", [])

        # Check if a note from this user already exists
        user_note_index = next(
            (
                i
                for i, note in enumerate(existing_notes)
                if note["created_by"] == created_by
            ),
            None,
        )

        if user_note_index is not None:
            # User already has a note, update it
            update_result = db.targeted_customers.update_one(
                {"_id": _id, f"notes.{user_note_index}.created_by": created_by},
                {
                    "$set": {
                        f"notes.{user_note_index}.note": note_text,
                        f"notes.{user_note_index}.updated_at": datetime.datetime.now(),
                    }
                },
            )
            if update_result.modified_count == 1:
                return {"message": "Note updated successfully"}
            else:
                return {"error": "Note update failed"}

        else:
            # User has not added a note before, so add a new one
            new_note = {
                "note": note_text,
                "created_by": created_by,
                "created_at": datetime.datetime.now(),
            }
            db.targeted_customers.update_one(
                {"_id": _id}, {"$push": {"notes": new_note}}
            )
            return {"message": "Note added successfully"}

    except Exception as e:
        print(e)
        return {"error": str(e)}
