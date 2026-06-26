from fastapi import APIRouter
from fastapi.responses import JSONResponse
import logging

from ..config.root import get_database, serialize_mongo_document

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

db = get_database()


def _default_header() -> dict:
    return {
        "title": "HOUSE OF BRANDS FOR PETS",
        "description": (
            "We filter pet products, so you don't have to. "
            "Bringing the world's best pet brands to retailers across India."
        ),
    }


def _default_footer() -> dict:
    return {
        "tagline": "BarkButler – House of Brands for Pets",
        "stat": "700+ Retail Stores Across India",
        "copyright": "© 2026 BarkButler",
        "links": [
            {"id": "email", "icon": "email", "label": "Email Us", "url": "mailto:info@barkbutler.in"},
            {"id": "partner", "icon": "store", "label": "Become a Retail Partner", "url": "https://www.pupscribe.in/#contact"},
        ],
    }


def _empty_config() -> dict:
    """A safe default so the public page never 500s when nothing is configured yet."""
    return {
        "is_active": True,
        "accent_color": "#29ABE2",
        "header": _default_header(),
        "footer": _default_footer(),
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


def _get_logo_url() -> str:
    """The header logo is the BarkButler brand image stored in the brands collection."""
    try:
        brand = db.brands.find_one({"name": {"$regex": "^barkbutler$", "$options": "i"}})
        if brand and brand.get("image_url"):
            return brand["image_url"]
    except Exception as e:
        logger.error(f"Error fetching barkbutler logo: {e}")
    return ""


@router.get("")
def get_linktree():
    """Public endpoint: returns the active link-tree config with only active links."""
    try:
        doc = db.linktree.find_one({"is_active": True})
        if not doc:
            config = _empty_config()
            config["logo_url"] = _get_logo_url()
            return config

        config = serialize_mongo_document(doc)

        # Backfill header/footer for configs saved before these fields existed.
        if not config.get("header"):
            config["header"] = _default_header()
        if not config.get("footer"):
            config["footer"] = _default_footer()

        # Only expose active links, sorted by order.
        links = [l for l in config.get("links", []) if l.get("is_active", True)]
        links.sort(key=lambda l: l.get("order", 0))
        config["links"] = links

        # Header logo always comes from the brands collection.
        config["logo_url"] = _get_logo_url()

        return config
    except Exception as e:
        logger.error(f"Error fetching linktree: {e}")
        return JSONResponse(content=_empty_config(), status_code=200)
