from fastapi import APIRouter, HTTPException, UploadFile, File
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from datetime import datetime, timezone
from typing import Optional
import boto3, os, uuid, re
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, NoCredentialsError

load_dotenv()

router = APIRouter()


def get_collection():
    db = get_database()
    return db.get_collection("blog_posts")


def generate_slug(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


@router.get("/posts")
def get_all_posts(
    page: int = 0,
    limit: int = 25,
    search: Optional[str] = None,
):
    collection = get_collection()
    query = {}
    if search:
        query["$or"] = [
            {"title": {"$regex": re.escape(search), "$options": "i"}},
            {"description": {"$regex": re.escape(search), "$options": "i"}},
        ]

    skip = page * limit
    total = collection.count_documents(query)
    posts = list(
        collection.find(query, {"content": 0})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    return {
        "posts": serialize_mongo_document(posts),
        "total_count": total,
        "total_pages": max(1, (total + limit - 1) // limit),
    }


@router.get("/posts/{post_id}")
def get_post(post_id: str):
    collection = get_collection()
    post = collection.find_one({"_id": ObjectId(post_id)})
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return serialize_mongo_document(post)


@router.post("/posts")
def create_post(data: dict):
    collection = get_collection()
    title = data.get("title", "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title is required")

    slug = data.get("slug", "").strip() or generate_slug(title)

    # Ensure slug is unique
    if collection.find_one({"slug": slug}):
        slug = f"{slug}-{uuid.uuid4().hex[:6]}"

    now = datetime.now()
    post = {
        **data,
        "slug": slug,
        "created_at": now,
        "updated_at": now,
    }
    post.pop("_id", None)
    result = collection.insert_one(post)

    return {
        "message": "Post created successfully",
        "id": str(result.inserted_id),
        "slug": slug,
    }


@router.put("/posts/{post_id}")
def update_post(post_id: str, data: dict):
    collection = get_collection()
    data["updated_at"] = datetime.now()
    data.pop("_id", None)

    result = collection.update_one({"_id": ObjectId(post_id)}, {"$set": data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"message": "Post updated successfully"}


@router.delete("/posts/{post_id}")
def delete_post(post_id: str):
    collection = get_collection()
    result = collection.delete_one({"_id": ObjectId(post_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"message": "Post deleted successfully"}


@router.post("/posts/upload-image")
async def upload_post_image(file: UploadFile = File(...)):
    """Upload a blog post image to S3 and return the URL."""
    allowed_types = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="Invalid file type. Allowed: JPEG, PNG, WebP, GIF")

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            region_name=os.getenv("S3_REGION"),
        )
        bucket = os.getenv("S3_BUCKET_NAME")
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "jpg"
        key = f"blog/{uuid.uuid4().hex}.{ext}"
        content = await file.read()

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=file.content_type,
        )
        s3_base_url = os.getenv("S3_URL", f"https://{bucket}.s3.{os.getenv('S3_REGION', 'ap-south-1')}.amazonaws.com")
        url = f"{s3_base_url}/{key}"
        return {"url": url}

    except (BotoCoreError, NoCredentialsError) as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")
