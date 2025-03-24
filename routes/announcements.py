from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .helpers import validate_file, process_upload
import threading, logging
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


@router.get("")
def get_announcements():
    try:
        announcements = list(
            db.announcements.find({"is_active": True}).sort("created_at", -1)
        )
        return serialize_mongo_document(announcements)
    except Exception as e:
        return e
