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
def get_catalogues(
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
        total_count = db.catalogues.count_documents(match_statement)
        cursor = db.catalogues.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "catalogues": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{catalogue_id}")
def delete_catalogue(catalogue_id: str):
    """
    Delete a catalogue by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        doc = db.catalogues.find_one({"_id": ObjectId(catalogue_id)})
        result = db.catalogues.update_one(
            {"_id": ObjectId(catalogue_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Catalogue not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_catalouge(catalogue: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in catalogue.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.catalogues.insert_one({**update_data})

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


@router.put("/{catalogue_id}")
def update_catalogue(catalogue_id: str, catalogue: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in catalogue.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.catalogues.update_one(
            {"_id": ObjectId(catalogue_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.catalogues.find_one({"_id": ObjectId(catalogue_id)})
            return serialize_mongo_document(updated_catalogue)
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Catalogue not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/upload")
async def upload_catalogue(file: UploadFile = File(...)):
    """
    Stream a large catalogue file directly to an S3 bucket.
    The file is uploaded in a memory‑efficient way using boto3's upload_fileobj.
    """
    try:
        # Create a unique key for the file in the bucket
        file_extension = file.filename.split(".")[-1]
        if file_extension.lower() != "pdf":
            raise HTTPException(status_code=400, detail="Only PDF files are allowed.")
        file_key = f"catalogues/{uuid.uuid4()}.pdf"

        # Upload the file using the file's file-like object.
        # This streams the file without reading it fully into memory.
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            file_key,
            ExtraArgs={"ContentType": "application/pdf"},
        )

        # Construct the file URL. This URL pattern depends on your S3 configuration.
        file_url = f"{AWS_S3_URL}/{file_key}"
        return {"file_url": file_url}
    except Exception as e:
        # Log the exception as needed
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/notify_salespeople")
async def notify():
    try:
        template = db.templates.find_one({"name": "catalogue_notification"})
        notify_all_salespeople(db, template, {})
    except Exception as e:
        # Log the exception as needed
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
