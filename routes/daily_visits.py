from fastapi import APIRouter, APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from bson import ObjectId
import boto3, os, uuid, logging, datetime, json, pytz
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from backend.config.whatsapp import send_whatsapp  # type: ignore

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
        daily_visits = list(
            db.daily_visits.find({"created_by": ObjectId(created_by)}).sort(
                {"created_at": -1}
            )
        )
        ist_timezone = pytz.timezone("Asia/Kolkata")

        for visit in daily_visits:
            if "created_at" in visit:
                utc_dt = visit["created_at"]
                # Make sure the datetime is timezone aware; if not, assume it's in UTC.
                if utc_dt.tzinfo is None:
                    utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
                # Convert from UTC to IST and format as desired.
                ist_dt = utc_dt.astimezone(ist_timezone)
                visit["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

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
        "updated_at": datetime.datetime.now(),
    }

    result = db.daily_visits.insert_one({**daily_visit})
    user = db.users.find_one({"email": "crmbarksales@gmail.com"})
    created_by = db.users.find_one({"_id": ObjectId(created_by)})
    template = db.templates.find_one({"name": "create_daily_visit"})
    send_whatsapp(
        user.get("phone"),
        {**template},
        {
            "name": user.get("first_name", ""),
            "salesperson_name": created_by.get("first_name", ""),
            "button_url": f"{str(result.inserted_id)}",
        },
    )
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

        # Convert created_at from UTC to IST if it exists
        if "created_at" in daily_visit:
            utc_dt = daily_visit["created_at"]
            # Ensure the datetime is timezone aware; assume it's UTC if not
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
            # Convert to IST (UTC+5:30)
            ist_timezone = pytz.timezone("Asia/Kolkata")
            ist_dt = utc_dt.astimezone(ist_timezone)
            # Format the datetime as a string, e.g., "YYYY-MM-DD HH:MM:SS"
            daily_visit["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

        return serialize_mongo_document(daily_visit)
    except Exception as e:
        return e


@router.put("/{daily_visit_id}")
async def update_daily_visit_update(
    daily_visit_id: str,
    uploaded_by: str = Form(...),
    update_text: str = Form(None),  # Changed from Form(...) to Form(None)
    update_id: str = Form(None),
    new_images: list[UploadFile] = File(None),
    delete_images: str = Form(None),
    plan: str = Form(None),
):
    """
    Appends or edits an update entry on a daily visit, and/or updates the main plan.
    - If plan is provided, updates the main daily visit plan.
    - If update_text is provided, then an update entry is created or edited.
    """
    # Retrieve the daily visit document.
    daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
    if not daily_visit:
        raise HTTPException(status_code=404, detail="Daily visit not found")

    # Update the main plan if provided.
    if plan is not None:
        daily_visit["plan"] = plan

    # Only process update entries if update_text is provided.
    if update_text is not None:
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
                    keys_to_delete = json.loads(delete_images)
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
                            status_code=500,
                            detail=f"Error deleting file from S3: {str(e)}",
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
            # Create a new update entry.
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
    update_fields = {"updated_at": datetime.datetime.now()}
    if plan is not None:
        update_fields["plan"] = daily_visit["plan"]
    if update_text is not None:
        update_fields["updates"] = daily_visit.get("updates", [])
    db.daily_visits.update_one(
        {"_id": ObjectId(daily_visit_id)},
        {"$set": update_fields},
    )
    updated_daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
    if "created_at" in updated_daily_visit:
        utc_dt = daily_visit["created_at"]
        # Ensure the datetime is timezone aware; assume it's UTC if not
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
        # Convert to IST (UTC+5:30)
        ist_timezone = pytz.timezone("Asia/Kolkata")
        ist_dt = utc_dt.astimezone(ist_timezone)
        # Format the datetime as a string, e.g., "YYYY-MM-DD HH:MM:SS"
        updated_daily_visit["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    user = db.users.find_one({"email": "crmbarksales@gmail.com"})
    created_by = db.users.find_one({"_id": ObjectId(daily_visit.get("created_by", ""))})
    template = db.templates.find_one({"name": "update_daily_visit"})
    send_whatsapp(
        user.get("phone"),
        {**template},
        {
            "name": user.get("first_name", ""),
            "salesperson_name": created_by.get("first_name", ""),
            "button_url": f"{daily_visit_id}",
        },
    )
    return JSONResponse(
        status_code=200,
        content={
            "message": "Daily visit updated successfully!",
            "daily_visit": serialize_mongo_document(updated_daily_visit),
        },
    )
