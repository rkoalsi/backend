from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .helpers import validate_file, process_upload
import threading, logging
from ..config.root import get_database, serialize_mongo_document  

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

db = get_database()


@router.get("")
def get_trainings():
    try:
        trainings = list(db.trainings.find({"is_active": True}))
        return serialize_mongo_document(trainings)
    except Exception as e:
        return e
