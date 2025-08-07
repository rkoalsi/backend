from fastapi import APIRouter, HTTPException, Query, Form, UploadFile, File
from fastapi.responses import JSONResponse
from config.root import connect_to_mongo, serialize_mongo_document 
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople, notify_office_coordinator_and_sales_admins
from dotenv import load_dotenv
import os, datetime, uuid, boto3, io
from typing import Optional
from botocore.exceptions import ClientError

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
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
def get_announcements(
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
        total_count = db.announcements.count_documents(match_statement)
        cursor = db.announcements.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "announcements": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{announcement_id}")
def delete_announcement(announcement_id: str):
    """
    Delete a announcement by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        db.announcements.delete_one(
            {"_id": ObjectId(announcement_id)},
        )

        return {"detail": "Announcement deleted successfully "}

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
async def create_announcement(
    title: str = Form(...),
    description: Optional[str] = Form(None),
    is_active: bool = Form(True),
    audio_file: Optional[UploadFile] = File(None),
    image_file: Optional[UploadFile] = File(None),
):
    """
    Create a new announcement with optional audio and image file uploads.
    """
    try:
        # Prepare announcement data
        announcement_data = {
            "title": title,
            "description": description,
            "is_active": is_active,
            "created_at": datetime.datetime.now(),
        }

        # Handle audio file if provided
        if audio_file:
            # Read file content
            file_content = await audio_file.read()

            # Generate a unique filename
            file_extension = os.path.splitext(audio_file.filename)[1]
            unique_filename = f"announcements/{uuid.uuid4()}{file_extension}"

            # Upload to S3
            try:
                s3_client.upload_fileobj(
                    io.BytesIO(file_content),
                    AWS_S3_BUCKET_NAME,
                    unique_filename,
                    ExtraArgs={
                        "ContentType": audio_file.content_type,
                        "ACL": "public-read",  # Make the file publicly accessible
                    },
                )

                # Store the S3 URL in the database
                announcement_data["audio_url"] = f"{AWS_S3_URL}/{unique_filename}"

            except ClientError as e:
                print(f"Error uploading to S3: {e}")
                raise HTTPException(
                    status_code=500, detail="Failed to upload audio file"
                )

        # Handle image file if provided
        if image_file:
            # Read file content
            image_content = await image_file.read()

            # Generate a unique filename
            image_extension = os.path.splitext(image_file.filename)[1]
            unique_image_filename = (
                f"announcements/images/{uuid.uuid4()}{image_extension}"
            )

            # Upload to S3
            try:
                s3_client.upload_fileobj(
                    io.BytesIO(image_content),
                    AWS_S3_BUCKET_NAME,
                    unique_image_filename,
                    ExtraArgs={
                        "ContentType": image_file.content_type,
                        "ACL": "public-read",  # Make the file publicly accessible
                    },
                )

                # Store the S3 URL in the database
                announcement_data["image_url"] = f"{AWS_S3_URL}/{unique_image_filename}"

            except ClientError as e:
                print(f"Error uploading to S3: {e}")
                raise HTTPException(
                    status_code=500, detail="Failed to upload image file"
                )

        # Insert into database
        result = db.announcements.insert_one(announcement_data)

        if result:
            # Notify salespeople about the new announcement
            template = db.templates.find_one({"name": "update_notification_1"})
            notify_all_salespeople(db, template, {})
            notify_office_coordinator_and_sales_admins(db, template, {})
            return "Document Created"
        else:
            raise HTTPException(status_code=500, detail="Failed to create announcement")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{announcement_id}")
async def update_announcement(
    announcement_id: str,
    title: str = Form(...),
    description: Optional[str] = Form(None),
    is_active: bool = Form(True),
    audio_file: Optional[UploadFile] = File(None),
    image_file: Optional[UploadFile] = File(None),
):
    """
    Update an existing announcement with optional audio and image file updates.
    """
    try:
        # Get existing announcement to check if there are files to replace
        existing = db.announcements.find_one({"_id": ObjectId(announcement_id)})
        if not existing:
            raise HTTPException(status_code=404, detail="Announcement not found")

        # Prepare update data
        update_data = {
            "title": title,
            "description": description,
            "is_active": is_active,
            "updated_at": datetime.datetime.now(),
        }

        # Handle audio file if provided
        if audio_file:
            # Delete old file from S3 if it exists
            if existing.get("audio_url"):
                try:
                    # Extract the key from the URL
                    old_key = existing["audio_url"].replace(f"{AWS_S3_URL}/", "")
                    s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=old_key)
                except ClientError as e:
                    print(f"Warning: Could not delete old file from S3: {e}")

            # Read file content
            file_content = await audio_file.read()

            # Generate a unique filename
            file_extension = os.path.splitext(audio_file.filename)[1]
            unique_filename = f"announcements/{uuid.uuid4()}{file_extension}"

            # Upload to S3
            try:
                s3_client.upload_fileobj(
                    io.BytesIO(file_content),
                    AWS_S3_BUCKET_NAME,
                    unique_filename,
                    ExtraArgs={
                        "ContentType": audio_file.content_type,
                        "ACL": "public-read",  # Make the file publicly accessible
                    },
                )

                # Store the S3 URL in the database
                update_data["audio_url"] = f"{AWS_S3_URL}/{unique_filename}"

            except ClientError as e:
                print(f"Error uploading to S3: {e}")
                raise HTTPException(
                    status_code=500, detail="Failed to upload audio file"
                )

        # Handle image file if provided
        if image_file:
            # Delete old image from S3 if it exists
            if existing.get("image_url"):
                try:
                    # Extract the key from the URL
                    old_image_key = existing["image_url"].replace(f"{AWS_S3_URL}/", "")
                    s3_client.delete_object(
                        Bucket=AWS_S3_BUCKET_NAME, Key=old_image_key
                    )
                except ClientError as e:
                    print(f"Warning: Could not delete old image from S3: {e}")

            # Read file content
            image_content = await image_file.read()

            # Generate a unique filename
            image_extension = os.path.splitext(image_file.filename)[1]
            unique_image_filename = (
                f"announcements/images/{uuid.uuid4()}{image_extension}"
            )

            # Upload to S3
            try:
                s3_client.upload_fileobj(
                    io.BytesIO(image_content),
                    AWS_S3_BUCKET_NAME,
                    unique_image_filename,
                    ExtraArgs={
                        "ContentType": image_file.content_type,
                        "ACL": "public-read",  # Make the file publicly accessible
                    },
                )

                # Store the S3 URL in the database
                update_data["image_url"] = f"{AWS_S3_URL}/{unique_image_filename}"

            except ClientError as e:
                print(f"Error uploading to S3: {e}")
                raise HTTPException(
                    status_code=500, detail="Failed to upload image file"
                )

        # Update the database
        result = db.announcements.update_one(
            {"_id": ObjectId(announcement_id)}, {"$set": update_data}
        )

        if result.modified_count > 0:
            return "Announcement Updated"
        else:
            return "No changes applied"
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
