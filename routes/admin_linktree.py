from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import JSONResponse
from datetime import datetime, timezone
from dotenv import load_dotenv
import boto3, uuid, os

from ..config.root import get_database, serialize_mongo_document

load_dotenv()
router = APIRouter()
db = get_database()

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


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
        "copyright": "© 2026 Pupscribe Enterprises Private Limited",
        "links": [
            {"id": "email", "icon": "email", "label": "Email Us", "url": "mailto:info@barkbutler.in"},
            {"id": "partner", "icon": "store", "label": "Become a Retail Partner", "url": "https://www.pupscribe.in/#contact"},
        ],
        # --- blog footer (barkbutler.in) ---
        "shop": {
            "label": "Shop",
            "url": "https://www.amazon.in/stores/page/39059D39-A60C-4518-B0DF-23C77F797F79",
        },
        "nav_links": [
            {"id": "blog", "label": "Blog", "url": "/"},
            {"id": "website", "label": "Website", "url": "https://pupscribe.in"},
        ],
        "social": {
            "instagram": "https://www.instagram.com/barkbutler/",
            "facebook": "https://www.facebook.com/BarkButler",
            "linkedin": "https://www.linkedin.com/company/barkbutler/mycompany/",
            "youtube": "https://www.youtube.com/@barkbutler",
            "website": "https://pupscribe.in",
            "email": "info@barkbutler.in",
            "retail": True,
        },
        "legal": {
            "label": "Privacy Policy | Terms and Conditions",
            "url": "https://d31mkmby5gvlu4.cloudfront.net/public/Pupscribe-Terms-Conditions-Privacy-Policy.pdf",
        },
    }


def _get_logo_url() -> str:
    """The header logo is the BarkButler brand image stored in the brands collection."""
    try:
        brand = db.brands.find_one({"name": {"$regex": "^barkbutler$", "$options": "i"}})
        if brand and brand.get("image_url"):
            return brand["image_url"]
    except Exception:
        pass
    return ""


def _default_config() -> dict:
    return {
        "is_active": True,
        "accent_color": "#29ABE2",
        "header": _default_header(),
        "footer": _default_footer(),
        "links": [],
        "pixel_code": "",
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
def get_linktree_config():
    """Return the single link-tree config document, creating a default if missing."""
    try:
        doc = db.linktree.find_one({})
        if not doc:
            default = _default_config()
            default["updated_at"] = datetime.now(timezone.utc)
            db.linktree.insert_one(default)
            doc = db.linktree.find_one({})
        config = serialize_mongo_document(doc)
        # Backfill header/footer for configs saved before these fields existed.
        if not config.get("header"):
            config["header"] = _default_header()
        if not config.get("footer"):
            config["footer"] = _default_footer()
        config["logo_url"] = _get_logo_url()
        return config
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("")
def update_linktree_config(config: dict):
    """Upsert the whole link-tree config in a single call."""
    try:
        # Never let the client overwrite Mongo's _id.
        config.pop("_id", None)
        # logo_url is derived from the brands collection, not stored here.
        config.pop("logo_url", None)
        config["updated_at"] = datetime.now(timezone.utc)

        db.linktree.update_one({}, {"$set": config}, upsert=True)
        doc = db.linktree.find_one({})
        return serialize_mongo_document(doc)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/upload")
async def upload_linktree_image(file: UploadFile = File(...)):
    """Upload an image to S3 under the linktree/ prefix and return its public URL."""
    try:
        ext = (file.filename or "").rsplit(".", 1)[-1].lower()
        allowed = {"jpg", "jpeg", "png", "webp"}
        if ext not in allowed:
            raise HTTPException(
                status_code=400,
                detail="Only jpg/jpeg/png/webp files are allowed.",
            )
        file_key = f"linktree/{uuid.uuid4()}.{ext}"
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            file_key,
            ExtraArgs={
                "ACL": "public-read",  # public link-tree page must be able to load it
                "ContentType": file.content_type or "application/octet-stream",
            },
        )
        return {"url": f"{AWS_S3_URL}/{file_key}", "s3_key": file_key}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
