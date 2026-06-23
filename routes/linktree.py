from fastapi import APIRouter
from fastapi.responses import JSONResponse
import logging

from ..config.root import get_database, serialize_mongo_document

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

db = get_database()


def _empty_config() -> dict:
    """A safe default so the public page never 500s when nothing is configured yet."""
    return {
        "is_active": True,
        "accent_color": "#6366F1",
        "links": [],
        "whatsapp": {"enabled": False, "number": "", "message": "", "label": "Chat with us"},
        "spin_wheel": {
            "enabled": False,
            "title": "",
            "description": "",
            "cta_text": "Spin",
            "terms": "",
            "start_date": None,
            "end_date": None,
            "segments": [],
        },
    }


@router.get("")
def get_linktree():
    """Public endpoint: returns the active link-tree config with only active links."""
    try:
        doc = db.linktree.find_one({"is_active": True})
        if not doc:
            return _empty_config()

        config = serialize_mongo_document(doc)

        # Only expose active links, sorted by order.
        links = [l for l in config.get("links", []) if l.get("is_active", True)]
        links.sort(key=lambda l: l.get("order", 0))
        config["links"] = links

        return config
    except Exception as e:
        logger.error(f"Error fetching linktree: {e}")
        return JSONResponse(content=_empty_config(), status_code=200)
