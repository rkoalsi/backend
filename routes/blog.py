from fastapi import APIRouter, Query, HTTPException
from ..config.root import get_database, serialize_mongo_document
from typing import Optional
import re

router = APIRouter()


def get_collection():
    db = get_database()
    return db.get_collection("blog_posts")


@router.get("/posts")
def get_blog_posts(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    category: Optional[str] = None,
    tag: Optional[str] = None,
    search: Optional[str] = None,
):
    collection = get_collection()
    query = {"draft": False}

    if category:
        query["categories"] = {"$regex": f"^{re.escape(category)}$", "$options": "i"}
    if tag:
        query["tags"] = {"$regex": f"^{re.escape(tag)}$", "$options": "i"}
    if search:
        query["$or"] = [
            {"title": {"$regex": re.escape(search), "$options": "i"}},
            {"description": {"$regex": re.escape(search), "$options": "i"}},
            {"content": {"$regex": re.escape(search), "$options": "i"}},
        ]

    skip = (page - 1) * limit
    total_count = collection.count_documents(query)
    posts = list(
        collection.find(query, {"content": 0})
        .sort("date", -1)
        .skip(skip)
        .limit(limit)
    )

    return {
        "posts": serialize_mongo_document(posts),
        "total_count": total_count,
        "page": page,
        "limit": limit,
        "total_pages": max(1, (total_count + limit - 1) // limit),
    }


@router.get("/posts/slugs")
def get_blog_post_slugs():
    """Get all published post slugs (used for static path generation)."""
    collection = get_collection()
    posts = list(collection.find({"draft": False}, {"slug": 1, "_id": 0}))
    return {"slugs": [p["slug"] for p in posts]}


@router.get("/categories")
def get_categories():
    collection = get_collection()
    categories = collection.distinct("categories", {"draft": False})
    return {"categories": sorted(categories)}


@router.get("/tags")
def get_tags():
    collection = get_collection()
    tags = collection.distinct("tags", {"draft": False})
    return {"tags": sorted(tags)}


@router.get("/posts/{slug}")
def get_blog_post(slug: str):
    collection = get_collection()
    post = collection.find_one({"slug": slug, "draft": False})
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return serialize_mongo_document(post)
