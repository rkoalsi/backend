"""
Admin CRUD for the order-form merchandising placements.

See routes/promotions.py for why nothing here is named "ad" or "banner" at the
identifier level — including the S3 key prefix, which network-level blockers
match on just as readily as class names.
"""
import io
import json
import os
import uuid
from datetime import datetime
from typing import List, Optional

import boto3
from bson.objectid import ObjectId
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

from ..config.root import get_database, serialize_mongo_document

load_dotenv()
router = APIRouter()
db = get_database()
promotions_collection = db["promotions"]
events_collection = db["promotion_events"]

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

PLACEMENTS = ("brand_banner", "in_scroll")
TARGET_TYPES = ("none", "brand", "category", "url")
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg"}
MAX_IMAGE_BYTES = 5 * 1024 * 1024


async def _upload(file: UploadFile) -> str:
    """Put an image on S3 under the neutral `promotions/` prefix, return its URL."""
    if not AWS_S3_BUCKET_NAME or not AWS_S3_URL:
        raise HTTPException(status_code=500, detail="S3 is not configured.")

    ext = os.path.splitext(file.filename or "")[1].lower() or ".jpg"
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported image type '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty image file.")
    if len(content) > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail="Image too large (max 5MB).")

    key = f"promotions/{uuid.uuid4()}{ext}"
    try:
        s3_client.upload_fileobj(
            io.BytesIO(content),
            AWS_S3_BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": file.content_type or "image/jpeg", "ACL": "public-read"},
        )
    except ClientError as e:
        print(f"[admin_promotions] S3 upload failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload image.")
    return f"{AWS_S3_URL}/{key}"


def _delete_from_s3(url: Optional[str]) -> None:
    if not url or not AWS_S3_URL or not url.startswith(AWS_S3_URL):
        return
    try:
        s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=url.replace(f"{AWS_S3_URL}/", ""))
    except ClientError as e:
        print(f"[admin_promotions] could not delete old image: {e}")


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    """Accept an ISO date/datetime string, or empty for 'no bound'."""
    if not value or not value.strip():
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Could not read the date '{value}'.")


def _parse_brands(value: Optional[str]) -> List[str]:
    """Brands arrive as a JSON array from the form. Empty means every brand."""
    if not value or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Brands must be a JSON array.")
    if not isinstance(parsed, list):
        raise HTTPException(status_code=400, detail="Brands must be a JSON array.")
    return [str(b) for b in parsed if str(b).strip()]


def _validate(placement: str, target_type: str, target_value: str) -> None:
    if placement not in PLACEMENTS:
        raise HTTPException(
            status_code=400, detail=f"Placement must be one of: {', '.join(PLACEMENTS)}"
        )
    if target_type not in TARGET_TYPES:
        raise HTTPException(
            status_code=400, detail=f"Link type must be one of: {', '.join(TARGET_TYPES)}"
        )
    if target_type != "none" and not (target_value or "").strip():
        raise HTTPException(
            status_code=400, detail="This link type needs a destination."
        )
    if target_type == "url":
        v = target_value.strip()
        if not v.startswith(("http://", "https://", "/")):
            raise HTTPException(
                status_code=400,
                detail="A URL destination must start with http://, https:// or /",
            )


@router.get("")
def list_promotions(
    placement: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
):
    """All placements, newest first, with their lifetime counters."""
    try:
        query: dict = {}
        if placement:
            query["placement"] = placement
        if is_active is not None:
            query["is_active"] = is_active
        docs = list(
            promotions_collection.find(query).sort([("priority", -1), ("created_at", -1)])
        )
        return {"promotions": serialize_mongo_document(docs)}
    except Exception as e:
        print(f"[admin_promotions] list failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to load placements.")


@router.post("")
async def create_promotion(
    name: str = Form(...),
    placement: str = Form(...),
    alt_text: Optional[str] = Form(None),
    brands: Optional[str] = Form(None),
    after_n_products: int = Form(8),
    target_type: str = Form("none"),
    target_value: Optional[str] = Form(None),
    is_active: bool = Form(True),
    starts_at: Optional[str] = Form(None),
    ends_at: Optional[str] = Form(None),
    priority: int = Form(0),
    image_file: UploadFile = File(...),
    mobile_image_file: Optional[UploadFile] = File(None),
):
    if not name.strip():
        raise HTTPException(status_code=400, detail="Give the placement a name.")
    _validate(placement, target_type, target_value or "")

    starts = _parse_date(starts_at)
    ends = _parse_date(ends_at)
    if starts and ends and ends < starts:
        raise HTTPException(status_code=400, detail="The end date is before the start date.")

    doc = {
        "name": name.strip(),
        "placement": placement,
        "image_url": await _upload(image_file),
        "mobile_image_url": await _upload(mobile_image_file) if mobile_image_file else None,
        "alt_text": (alt_text or "").strip(),
        "brands": _parse_brands(brands),
        # Only meaningful for in_scroll; clamped so a typo can't put a banner
        # between every single product.
        "after_n_products": max(2, min(50, int(after_n_products))),
        "target_type": target_type,
        "target_value": (target_value or "").strip(),
        "is_active": is_active,
        "starts_at": starts,
        "ends_at": ends,
        "priority": int(priority),
        "impressions": 0,
        "clicks": 0,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    result = promotions_collection.insert_one(doc)
    return {"message": "Placement created.", "id": str(result.inserted_id)}


@router.put("/{promotion_id}")
async def update_promotion(
    promotion_id: str,
    name: str = Form(...),
    placement: str = Form(...),
    alt_text: Optional[str] = Form(None),
    brands: Optional[str] = Form(None),
    after_n_products: int = Form(8),
    target_type: str = Form("none"),
    target_value: Optional[str] = Form(None),
    is_active: bool = Form(True),
    starts_at: Optional[str] = Form(None),
    ends_at: Optional[str] = Form(None),
    priority: int = Form(0),
    image_file: Optional[UploadFile] = File(None),
    mobile_image_file: Optional[UploadFile] = File(None),
    clear_mobile_image: bool = Form(False),
):
    if not ObjectId.is_valid(promotion_id):
        raise HTTPException(status_code=400, detail="Invalid placement id")
    existing = promotions_collection.find_one({"_id": ObjectId(promotion_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Placement not found")

    _validate(placement, target_type, target_value or "")
    starts = _parse_date(starts_at)
    ends = _parse_date(ends_at)
    if starts and ends and ends < starts:
        raise HTTPException(status_code=400, detail="The end date is before the start date.")

    update = {
        "name": name.strip(),
        "placement": placement,
        "alt_text": (alt_text or "").strip(),
        "brands": _parse_brands(brands),
        "after_n_products": max(2, min(50, int(after_n_products))),
        "target_type": target_type,
        "target_value": (target_value or "").strip(),
        "is_active": is_active,
        "starts_at": starts,
        "ends_at": ends,
        "priority": int(priority),
        "updated_at": datetime.utcnow(),
    }

    if image_file:
        update["image_url"] = await _upload(image_file)
        _delete_from_s3(existing.get("image_url"))
    if mobile_image_file:
        update["mobile_image_url"] = await _upload(mobile_image_file)
        _delete_from_s3(existing.get("mobile_image_url"))
    elif clear_mobile_image:
        update["mobile_image_url"] = None
        _delete_from_s3(existing.get("mobile_image_url"))

    promotions_collection.update_one({"_id": ObjectId(promotion_id)}, {"$set": update})
    return {"message": "Placement updated."}


@router.delete("/{promotion_id}")
def delete_promotion(promotion_id: str):
    if not ObjectId.is_valid(promotion_id):
        raise HTTPException(status_code=400, detail="Invalid placement id")
    existing = promotions_collection.find_one({"_id": ObjectId(promotion_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Placement not found")

    _delete_from_s3(existing.get("image_url"))
    _delete_from_s3(existing.get("mobile_image_url"))
    promotions_collection.delete_one({"_id": ObjectId(promotion_id)})
    # Events are kept deliberately — deleting a placement shouldn't erase the
    # record of who engaged with it.
    return {"message": "Placement deleted."}


@router.get("/{promotion_id}/events")
def promotion_events(
    promotion_id: str,
    event: Optional[str] = Query(None, description="'view' or 'click'"),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Who engaged, most recent first. This is the point of the whole feature on a
    B2B order form: the useful answer is *which customers* clicked, not a rate.
    """
    if not ObjectId.is_valid(promotion_id):
        raise HTTPException(status_code=400, detail="Invalid placement id")
    query: dict = {"promotion_id": ObjectId(promotion_id)}
    if event:
        query["event"] = event
    docs = list(events_collection.find(query).sort("created_at", -1).limit(limit))
    return {"events": serialize_mongo_document(docs)}
