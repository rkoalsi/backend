from fastapi import APIRouter, HTTPException, Body, status
from bson import ObjectId
from .helpers import validate_file, process_upload
import threading, logging
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
    # Remove the id field if present, so MongoDB can generate it.
    hook["customer_id"] = ObjectId(hook["customer_id"])
    hook["created_by"] = ObjectId(hook["created_by"])
    hook["created_at"] = datetime.now()
    for h in hook["hooks"]:
        hook_category_id = h.pop("category_id")
        h["category_id"] = ObjectId(hook_category_id)
    result = shop_hooks_collection.insert_one(hook)
    if not result:
        raise HTTPException(status_code=404, detail="Hook not created")
    return "Document Created"


@router.get("")
async def get_all_hooks(created_by: str):
    try:
        hooks = list(
            db.shop_hooks.find({"created_by": ObjectId(created_by)}).sort(
                {"created_at": -1}
            )
        )
        return serialize_mongo_document(hooks)
    except Exception as e:
        return e


@router.get("/{hook_id}")
def get_hook_by_id(hook_id: str):
    try:
        shop_hook = dict(db.shop_hooks.find_one({"_id": ObjectId(hook_id)}))
        return serialize_mongo_document(shop_hook)
    except Exception as e:
        return e


@router.put("/{hook_id}")
async def update_shop_hook(hook_id: str, hook: dict = Body(...)):
    shop_hooks_collection = db["shop_hooks"]
    # Convert customer and created_by fields to ObjectId.
    hook["customer_id"] = ObjectId(hook["customer_id"])
    hook["created_by"] = ObjectId(hook["created_by"])
    # Set an updated timestamp (or update created_at if desired).
    hook["updated_at"] = datetime.now()
    # Convert each hook's category_id to category_id as ObjectId.
    for h in hook["hooks"]:
        hook_category_id = h.pop("category_id")
        h["category_id"] = ObjectId(hook_category_id)
    result = shop_hooks_collection.update_one(
        {"_id": ObjectId(hook_id)}, {"$set": hook}
    )
    if result.modified_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Hook not updated"
        )
    return "Document Updated"
