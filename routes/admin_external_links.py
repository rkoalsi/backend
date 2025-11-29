from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    File,
    UploadFile,
)
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from dotenv import load_dotenv
import boto3, uuid, os
from .helpers import notify_all_salespeople

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")  # Default to ap-south-1
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


@router.get("")
def get_external_links(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}
        pipeline = [
            {"$match": match_statement},
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        total_count = db.external_links.count_documents(match_statement)
        cursor = db.external_links.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "external_links": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{external_link_id}")
def delete_external_link(external_link_id: str):
    """
    Delete a external_link by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        doc = db.external_links.find_one({"_id": ObjectId(external_link_id)})
        result = db.external_links.update_one(
            {"_id": ObjectId(external_link_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Catalogue not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_external_link(external_link: dict):
    """
    Create the external_link with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in external_link.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.external_links.insert_one({**update_data})

        if result:
            # Fetch and return the updated document.
            return "Document Created"
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Catalogue not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.put("/{external_link_id}")
def update_external_link(external_link_id: str, external_link: dict):
    """
    Update the external_link with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Validate ObjectId format
        try:
            obj_id = ObjectId(external_link_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid external_link_id format")
        
        # Check if the document exists first
        existing_doc = db.external_links.find_one({"_id": obj_id})
        if not existing_doc:
            raise HTTPException(status_code=404, detail="External Link not found")
        
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in external_link.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        
        print("Update data:", update_data)
        print("Existing document:", existing_doc)
        
        # Perform the update
        result = db.external_links.update_one(
            {"_id": obj_id}, 
            {"$set": update_data}
        )
        
        # Check if update was successful
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="External Link not found")
        
        # Fetch and return the updated document
        updated_external_link = db.external_links.find_one({"_id": obj_id})
        return serialize_mongo_document(updated_external_link)
        
    except HTTPException:
        # Re-raise HTTPExceptions to preserve their status codes
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")