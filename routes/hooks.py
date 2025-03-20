from fastapi import APIRouter, HTTPException, Body, status, Query
from bson import ObjectId
import pytz, logging
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from datetime import datetime

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


@router.get("/categories")
def get_categories():
    try:
        hooks_category = list(db.hooks_category.find({"is_active": True}))
        return serialize_mongo_document(hooks_category)
    except Exception as e:
        return e


@router.post("")
async def create_shop_hook(hook: dict):
    shop_hooks_collection = db["shop_hooks"]

    # Convert fields to ObjectId
    hook["customer_id"] = ObjectId(hook["customer_id"])
    hook["created_by"] = ObjectId(hook["created_by"])
    hook["created_at"] = datetime.now()
    hook["is_active"] = True

    for h in hook["hooks"]:
        h["category_id"] = ObjectId(h.pop("category_id"))

    # Check if an active entry exists
    existing_hook = shop_hooks_collection.find_one(
        {
            "customer_id": hook["customer_id"],
            "created_by": hook["created_by"],
            "is_active": True,
        }
    )

    if existing_hook:
        # Identify only the changed hooks
        changed_hooks = [
            new_hook
            for new_hook in hook["hooks"]
            if any(
                old_hook["entryId"] == new_hook["entryId"]
                and (
                    old_hook["hooksAvailable"] != new_hook["hooksAvailable"]
                    or old_hook["totalHooks"] != new_hook["totalHooks"]
                )
                for old_hook in existing_hook["hooks"]
            )
        ]

        if changed_hooks:
            hook["history"] = existing_hook.get("history", [])
            hook["history"].append(
                {
                    "previous_hooks": changed_hooks,
                    "updated_at": datetime.now(),
                }
            )

        # Mark old document inactive
        shop_hooks_collection.update_one(
            {"_id": existing_hook["_id"]}, {"$set": {"is_active": False}}
        )

    # Insert new document
    result = shop_hooks_collection.insert_one(hook)

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create hook entry")

    return {"message": "New hook entry created", "hook_id": str(result.inserted_id)}


# Convert UTC datetime to IST
def convert_utc_to_ist(utc_dt):
    if utc_dt:
        try:
            # If datetime is naive, assume it's in UTC
            if utc_dt.tzinfo is None:
                utc_dt = pytz.utc.localize(utc_dt)
            # Convert to IST (UTC+5:30)
            ist_timezone = pytz.timezone("Asia/Kolkata")
            return utc_dt.astimezone(ist_timezone).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Error converting timezone: {e}")
            return None
    return None


# 🟢 Get all hooks created by a user (sorted by latest)
@router.get("")
async def get_all_hooks(created_by: str, show_history: bool = False):
    try:
        query_filter = {"created_by": ObjectId(created_by)}
        if not show_history:
            query_filter["is_active"] = (
                True  # Only show active entries unless history is requested
            )

        hooks = list(db.shop_hooks.find(query_filter).sort("created_at", -1))
        for hook in hooks:
            # Convert created_at from UTC to IST
            if "created_at" in hook:
                utc_dt = hook["created_at"]
                if utc_dt.tzinfo is None:
                    utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
                ist_timezone = pytz.timezone("Asia/Kolkata")
                ist_dt = utc_dt.astimezone(ist_timezone)
                hook["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

        return serialize_mongo_document(hooks)
    except Exception as e:
        return {"error": str(e)}


# 🟢 Get a single hook by ID
@router.get("/{hook_id}")
def get_hook_by_id(hook_id: str):
    try:
        shop_hook = db["shop_hooks"].find_one({"_id": ObjectId(hook_id)})
        if not shop_hook:
            raise HTTPException(status_code=404, detail="Hook entry not found")

        # Convert timestamp
        shop_hook["created_at"] = convert_utc_to_ist(shop_hook.get("created_at"))

        return serialize_mongo_document(shop_hook)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching hook by ID: {str(e)}"
        )


@router.put("/{hook_id}")
async def update_shop_hook(hook_id: str, hook: dict = Body(...)):
    shop_hooks_collection = db["shop_hooks"]

    # Get existing hook entry for history tracking
    existing_hook = shop_hooks_collection.find_one({"_id": ObjectId(hook_id)})
    if not existing_hook:
        raise HTTPException(status_code=404, detail="Hook not found")

    hook["created_at"] = existing_hook["created_at"]
    hook["customer_id"] = ObjectId(hook["customer_id"])
    hook["created_by"] = ObjectId(hook["created_by"])
    hook["updated_at"] = datetime.now()
    hook["is_active"] = True

    for h in hook["hooks"]:
        h["category_id"] = ObjectId(h.pop("category_id"))

    # Identify only the changed hooks
    changed_hooks = [
        new_hook
        for new_hook in hook["hooks"]
        if any(
            old_hook["entryId"] == new_hook["entryId"]
            and (
                old_hook["hooksAvailable"] != new_hook["hooksAvailable"]
                or old_hook["totalHooks"] != new_hook["totalHooks"]
            )
            for old_hook in existing_hook["hooks"]
        )
    ]

    if changed_hooks:
        hook["history"] = existing_hook.get("history", [])
        hook["history"].append(
            {
                "previous_hooks": changed_hooks,
                "updated_at": datetime.now(),
            }
        )

    # Mark older versions inactive
    shop_hooks_collection.update_many(
        {"customer_id": hook["customer_id"], "is_active": True},
        {"$set": {"is_active": False}},
    )

    # Insert as a new document
    result = shop_hooks_collection.insert_one(hook)

    if not result:
        raise HTTPException(status_code=500, detail="Failed to update hook entry")

    return {"message": "Hook updated", "hook_id": str(result.inserted_id)}
