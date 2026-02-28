from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
db = get_database()
contact_submissions_collection = db["contact_submissions"]


@router.get("")
def get_contact_submissions(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}
        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        total_count = contact_submissions_collection.count_documents(match_statement)
        cursor = contact_submissions_collection.aggregate(pipeline)
        leads = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "contact_submissions": leads,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
