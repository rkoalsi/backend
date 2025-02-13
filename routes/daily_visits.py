from fastapi import APIRouter, APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from bson import ObjectId
import boto3, os, uuid, logging, datetime, json
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


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
def get_daily_visits(created_by: str):
    try:
        daily_visits = list(db.daily_visits.find({"created_by": ObjectId(created_by)}))
        return serialize_mongo_document(daily_visits)
    except Exception as e:
        return e


@router.post("")
async def create_daily_visit(
    plan: str = Form(...), created_by: str = Form(...), selfie: UploadFile = File(None)
):
    """
    Creates a daily visit entry.
    - Uploads the selfie (if provided) to S3.
    - Saves the plan and associated data.
    """
    selfie_url = None

    # Upload the selfie to S3 if provided
    if selfie:
        # Create a unique filename using uuid
        file_extension = os.path.splitext(selfie.filename)[1]
        unique_filename = f"daily_visits/{datetime.datetime.now().date()}/{uuid.uuid4()}{file_extension}"
        try:
            s3_client.upload_fileobj(
                selfie.file,
                AWS_S3_BUCKET_NAME,
                unique_filename,
                ExtraArgs={
                    "ACL": "public-read",
                    "ContentType": selfie.content_type,
                },
            )
            selfie_url = f"{AWS_S3_URL}/{unique_filename}"
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error uploading file: {str(e)}"
            )
    # Create the daily visit record
    daily_visit = {
        "plan": plan,
        "created_by": ObjectId(created_by),
        "selfie": selfie_url,
        "created_at": datetime.datetime.now(),
    }

    print(daily_visit)
    db.daily_visits.insert_one({**daily_visit})
    return JSONResponse(
        status_code=201,
        content={
            "message": "You're doing great! Daily visit created successfully!",
            "daily_visit": serialize_mongo_document(daily_visit),
        },
    )


@router.get("/{daily_visit_id}")
def get_daily_visits(daily_visit_id: str):
    try:
        daily_visit = dict(db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)}))
        return serialize_mongo_document(daily_visit)
    except Exception as e:
        return e


@router.put("/{daily_visit_id}")
async def update_daily_visit_update(
    daily_visit_id: str,
    uploaded_by: str = Form(...),
    update_text: str = Form(...),
    update_id: str = Form(None),
    new_images: list[UploadFile] = File(None),
    delete_images: str = Form(None),
):
    """
    Appends or edits an update entry on a daily visit.

    - If update_id is provided, edit the existing update entry (matched by its _id).
    - Otherwise, create a new update entry.

    Each update entry includes:
      • text (the additional information)
      • an array of images (each with its URL, S3 key, and uploader)
      • timestamps

    New images are uploaded to S3, and any images whose S3 keys are provided in delete_images (JSON array)
    are removed from S3 and from the update entry.
    """
    # Retrieve the daily visit document.
    daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
    if not daily_visit:
        raise HTTPException(status_code=404, detail="Daily visit not found")
    # Get the current updates array (or initialize if not present).
    updates = daily_visit.get("updates", [])
    if update_id:
        # Find the update entry by its _id.
        update_entry = next(
            (u for u in updates if str(u.get("_id")) == update_id), None
        )
        if not update_entry:
            raise HTTPException(status_code=404, detail="Update entry not found")
        # Update its text.
        update_entry["text"] = update_text
        # If there are images to delete, parse the JSON and remove them.
        if delete_images:
            try:
                keys_to_delete = json.loads(
                    delete_images
                )  # e.g., ["daily_visits/2025-02-13/updates/xxx.png", ...]
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail="Invalid JSON for delete_images"
                )
            if "images" in update_entry:
                update_entry["images"] = [
                    img
                    for img in update_entry["images"]
                    if img.get("s3_key") not in keys_to_delete
                ]
            # Delete each specified file from S3.
            for key in keys_to_delete:
                try:
                    s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=key)
                except Exception as e:
                    raise HTTPException(
                        status_code=500, detail=f"Error deleting file from S3: {str(e)}"
                    )
        # If there are new images, upload and append them.
        if new_images:
            new_uploaded_images = []
            for image in new_images:
                file_extension = os.path.splitext(image.filename)[1]
                unique_filename = f"daily_visits/{datetime.datetime.now().date()}/updates/{uuid.uuid4()}{file_extension}"
                try:
                    s3_client.upload_fileobj(
                        image.file,
                        AWS_S3_BUCKET_NAME,
                        unique_filename,
                        ExtraArgs={
                            "ACL": "public-read",
                            "ContentType": image.content_type,
                        },
                    )
                    new_uploaded_images.append(
                        {
                            "url": f"{AWS_S3_URL}/{unique_filename}",
                            "s3_key": unique_filename,
                            "uploaded_by": ObjectId(uploaded_by),
                        }
                    )
                except Exception as e:
                    raise HTTPException(
                        status_code=500, detail=f"Error uploading file: {str(e)}"
                    )
            update_entry.setdefault("images", [])
            update_entry["images"].extend(new_uploaded_images)
        # Update the updated_at timestamp.
        update_entry["updated_at"] = datetime.datetime.now()
    else:
        # Create a new update entry. We assign a unique _id (as a string) and do not add an extra "id" field.
        new_entry = {
            "_id": ObjectId(),
            "text": update_text,
            "images": [],
            "uploaded_by": ObjectId(uploaded_by),
            "created_at": datetime.datetime.now(),
            "updated_at": datetime.datetime.now(),
        }
        if new_images:
            new_uploaded_images = []
            for image in new_images:
                file_extension = os.path.splitext(image.filename)[1]
                unique_filename = f"daily_visits/{datetime.datetime.now().date()}/updates/{uuid.uuid4()}{file_extension}"
                try:
                    s3_client.upload_fileobj(
                        image.file,
                        AWS_S3_BUCKET_NAME,
                        unique_filename,
                        ExtraArgs={
                            "ACL": "public-read",
                            "ContentType": image.content_type,
                        },
                    )
                    new_uploaded_images.append(
                        {
                            "url": f"{AWS_S3_URL}/{unique_filename}",
                            "s3_key": unique_filename,
                            "uploaded_by": ObjectId(uploaded_by),
                        }
                    )
                except Exception as e:
                    raise HTTPException(
                        status_code=500, detail=f"Error uploading file: {str(e)}"
                    )
            new_entry["images"] = new_uploaded_images
        updates.append(new_entry)
        daily_visit["updates"] = updates
    # Update the daily visit document.
    db.daily_visits.update_one(
        {"_id": ObjectId(daily_visit_id)},
        {"$set": {"updates": updates, "updated_at": datetime.datetime.now()}},
    )
    updated_daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})

    return JSONResponse(
        status_code=200,
        content={
            "message": "Daily visit updated successfully!",
            "daily_visit": serialize_mongo_document(updated_daily_visit),
        },
    )
