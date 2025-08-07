from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .helpers import validate_file, process_upload
import threading, logging
from config.root import connect_to_mongo, serialize_mongo_document  

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


@router.get("")
def get_catalogues():
    try:
        catalogues = list(db.catalogues.find({"is_active": True}))
        return serialize_mongo_document(catalogues)
    except Exception as e:
        return e
