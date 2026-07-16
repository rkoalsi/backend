from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import JSONResponse
from datetime import datetime, timezone
from dotenv import load_dotenv
from bson.objectid import ObjectId
import boto3, uuid, os, re

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


def _slugify(text: str) -> str:
    """Turn a name into a URL-safe slug (lowercase, hyphenated)."""
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return slug or "card"


def _unique_slug(base: str, exclude_id: ObjectId = None) -> str:
    """Ensure the slug is unique across the collection, appending -2, -3, ... if needed."""
    slug = base
    n = 1
    while True:
        query = {"slug": slug}
        if exclude_id is not None:
            query["_id"] = {"$ne": exclude_id}
        if not db.business_cards.find_one(query):
            return slug
        n += 1
        slug = f"{base}-{n}"


@router.get("")
def list_cards():
    """Return all business cards, newest first, each with its QR scan count."""
    try:
        docs = [serialize_mongo_document(d) for d in db.business_cards.find().sort("created_at", -1)]
        # One aggregation for all cards instead of a count query per card.
        counts = {
            str(row["_id"]): row["count"]
            for row in db.business_card_scans.aggregate(
                [{"$group": {"_id": "$card_id", "count": {"$sum": 1}}}]
            )
        }
        for d in docs:
            d["scan_count"] = counts.get(d["_id"], 0)
        return docs
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{card_id}/scans")
def list_card_scans(card_id: str, limit: int = 100):
    """Return recent QR scans for one card, newest first, plus the total count."""
    try:
        oid = ObjectId(card_id)
        total = db.business_card_scans.count_documents({"card_id": oid})
        scans = [
            serialize_mongo_document(s)
            for s in db.business_card_scans.find({"card_id": oid})
            .sort("ts", -1)
            .limit(max(1, min(limit, 500)))
        ]
        return {"total": total, "scans": scans}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{card_id}")
def get_card(card_id: str):
    """Return a single business card by id."""
    try:
        doc = db.business_cards.find_one({"_id": ObjectId(card_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Card not found")
        return serialize_mongo_document(doc)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_card(card: dict):
    """Create a new business card. Slug is auto-generated from the name if not provided."""
    try:
        card.pop("_id", None)
        base = _slugify(card.get("slug") or card.get("name") or "card")
        card["slug"] = _unique_slug(base)
        card.setdefault("is_active", True)
        # Sensible defaults (all still editable from the admin form).
        if not card.get("company"):
            card["company"] = "Pupscribe Enterprises Pvt. Ltd."
        if not card.get("city"):
            card["city"] = "Mumbai"
        if not card.get("country"):
            card["country"] = "India"
        now = datetime.now(timezone.utc)
        card["created_at"] = now
        card["updated_at"] = now
        result = db.business_cards.insert_one(card)
        doc = db.business_cards.find_one({"_id": result.inserted_id})
        return serialize_mongo_document(doc)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{card_id}")
def update_card(card_id: str, card: dict):
    """Update an existing business card."""
    try:
        oid = ObjectId(card_id)
        existing = db.business_cards.find_one({"_id": oid})
        if not existing:
            raise HTTPException(status_code=404, detail="Card not found")

        card.pop("_id", None)
        card.pop("created_at", None)

        # Re-derive/validate slug uniqueness if the slug or name changed.
        if card.get("slug"):
            base = _slugify(card["slug"])
            card["slug"] = _unique_slug(base, exclude_id=oid)

        card["updated_at"] = datetime.now(timezone.utc)
        db.business_cards.update_one({"_id": oid}, {"$set": card})
        doc = db.business_cards.find_one({"_id": oid})
        return serialize_mongo_document(doc)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{card_id}")
def delete_card(card_id: str):
    """Delete a business card."""
    try:
        result = db.business_cards.delete_one({"_id": ObjectId(card_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Card not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/upload")
async def upload_card_image(file: UploadFile = File(...)):
    """Upload an image to S3 under the cards/ prefix and return its public URL."""
    try:
        ext = (file.filename or "").rsplit(".", 1)[-1].lower()
        allowed = {"jpg", "jpeg", "png", "webp"}
        if ext not in allowed:
            raise HTTPException(
                status_code=400,
                detail="Only jpg/jpeg/png/webp files are allowed.",
            )
        file_key = f"cards/{uuid.uuid4()}.{ext}"
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            file_key,
            ExtraArgs={
                "ACL": "public-read",  # public card page must be able to load it
                "ContentType": file.content_type or "application/octet-stream",
            },
        )
        return {"url": f"{AWS_S3_URL}/{file_key}", "s3_key": file_key}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
