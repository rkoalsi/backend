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
    shops: str = Form(...), created_by: str = Form(...), selfie: UploadFile = File(None)
):
    """
    Creates a daily visit entry for multiple shops.
    - Uploads the selfie (if provided) to S3.
    - Parses and saves the shops data (each shop includes customer info, address, and reason).
    """
    selfie_url = None
    creator_id = ObjectId(created_by)

    # Check if a daily visit already exists for the user today
    start_of_day = datetime.datetime.combine(
        datetime.datetime.now().date(), datetime.time.min
    )
    end_of_day = datetime.datetime.combine(
        datetime.datetime.now().date(), datetime.time.max
    )
    if db.daily_visits.find_one(
        {
            "created_by": creator_id,
            "created_at": {"$gte": start_of_day, "$lte": end_of_day},
        }
    ):
        raise HTTPException(
            status_code=400, detail="Daily visit already created for today"
        )

    # Upload the selfie to S3 if provided
    if selfie:
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
    # Parse the shops JSON string into a Python list
    try:
        shops_data = json.loads(shops)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid shops data format")
    for shop in shops_data:
        if not shop.get("potential_customer", ""):
            shop["customer_id"] = ObjectId(shop["customer_id"])
            if shop.get("order_expected", False):
                db.expected_reorders.insert_one(
                    {
                        "address": shop.get("address"),
                        "customer_id": ObjectId(shop.get("customer_id")),
                        "customer_name": shop.get("customer_name"),
                        "created_by": ObjectId(created_by),
                        "created_at": datetime.datetime.now(),
                    }
                )
        else:
            result = db.potential_customers.insert_one(
                {
                    "name": shop["potential_customer_name"],
                    "address": shop["potential_customer_address"],
                    "tier": shop["potential_customer_tier"],
                    "mobile": shop["potential_customer_mobile"],
                    "created_by": ObjectId(created_by),
                    "created_at": datetime.datetime.now(),
                }
            )
            potential_customer_id = str(result.inserted_id)
            shop["potential_customer_id"] = ObjectId(potential_customer_id)
    # Create the daily visit record with the shops data
    daily_visit = {
        "shops": shops_data,
        "created_by": ObjectId(created_by),
        "selfie": selfie_url,
        "created_at": datetime.datetime.now(),
        "updated_at": datetime.datetime.now(),
    }

    result = db.daily_visits.insert_one(daily_visit)

    # Retrieve users and template for sending a WhatsApp message (update these queries as needed)
    user_obj = db.users.find_one({"email": "crmbarksales@gmail.com"})
    created_by_user = db.users.find_one({"_id": ObjectId(created_by)})
    template = db.templates.find_one({"name": "create_daily_visit"})

    send_whatsapp(
        user_obj.get("phone"),
        {**template},
        {
            "name": user_obj.get("first_name", ""),
            "salesperson_name": created_by_user.get("first_name", ""),
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
    update_text: str = Form(None),  # if provided, creates/edits an update entry
    update_id: str = Form(None),
    delete_update: str = Form(None),  # if provided, deletes the update entry
    new_images: list[UploadFile] = File(None),
    delete_images: str = Form(None),
    plan: str = Form(None),  # legacy field; used if shops is not provided
    shops: str = Form(None),  # new field: JSON string representing shops data
    customer_id: str = Form(None),  # new: selected customer's ID
    customer_name: str = Form(None),  # new: selected customer's name,
    potential_customer: bool = Form(None),
    potential_customer_id: str = Form(None),
    potential_customer_name: str = Form(None),
    potential_customer_address: str = Form(None),
    potential_customer_tier: str = Form(None),
    potential_customer_mobile: str = Form(None),
):
    """
    Appends or edits an update entry on a daily visit, and/or updates the main daily visit content.
    - If 'shops' is provided, it updates the main shops data.
    - Else, if 'plan' is provided, it updates the main plan (legacy).
    - If update_text is provided, then an update entry is created or edited.
    - Additionally, if customer_id and customer_name are provided, they are stored with the update entry.
    """
    # Retrieve the daily visit document.
    daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
    if not daily_visit:
        raise HTTPException(status_code=404, detail="Daily visit not found")
    update_fields = {"updated_at": datetime.datetime.now()}

    # Track if we need to update existing references to this potential customer
    updated_potential_customer = False
    updated_pc_id = None
    updated_pc_data = {}

    # Update the main content.
    if shops is not None:
        try:
            shops_data = json.loads(shops)
            daily_visit["shops"] = shops_data
            for shop in shops_data:
                if "potential_customer" in shop:
                    pc_id = shop.get("potential_customer_id")
                    name = shop.get("potential_customer_name", potential_customer_name)
                    address = shop.get(
                        "potential_customer_address", potential_customer_address
                    )
                    tier = shop.get("potential_customer_tier", potential_customer_tier)
                    mobile = shop.get(
                        "potential_customer_mobile", potential_customer_mobile
                    )
                    # Store these values to update in updates array later
                    updated_potential_customer = True
                    updated_pc_id = pc_id
                    updated_pc_data = {
                        "potential_customer_name": name,
                        "potential_customer_address": address,
                        "potential_customer_tier": tier,
                        "potential_customer_mobile": mobile,
                    }

                    shop.pop("address", None)
                    shop.pop("customer_name", None)
                    shop["potential_customer_id"] = ObjectId(pc_id)
                    potential_customer_data = {
                        "name": name,
                        "address": address,
                        "tier": tier,
                        "mobile": mobile,
                        "created_by": ObjectId(uploaded_by),
                        "created_at": datetime.datetime.now(),
                    }

                    doc = db.potential_customers.find_one(
                        {
                            "_id": ObjectId(pc_id)
                            # "created_by": ObjectId(uploaded_by),
                        }
                    )
                    if doc:
                        db.potential_customers.update_one(
                            {"_id": ObjectId(pc_id)},
                            {"$set": potential_customer_data},
                        )
                        potential_customer_id = pc_id
                    else:
                        potential_customer_id = db.potential_customers.insert_one(
                            potential_customer_data
                        ).inserted_id

        except Exception as e:
            print(e)
            raise HTTPException(status_code=400, detail="Invalid shops data format")

        # If we updated potential customer info, reflect those changes in the updates array
        if updated_potential_customer and updated_pc_id:
            updates = daily_visit.get("updates", [])
            for update in updates:
                if update.get("potential_customer") and str(
                    update.get("potential_customer_id")
                ) == str(updated_pc_id):
                    # Update this entry with the new potential customer information
                    update.update(updated_pc_data)

            # Add updates array to fields that need to be updated
            update_fields["updates"] = updates
            daily_visit["updates"] = updates

    elif plan is not None:
        daily_visit["plan"] = plan

    print(update_fields)
    if delete_update:
        updates = daily_visit.get("updates", [])
        new_updates = [u for u in updates if str(u.get("_id")) != delete_update]
        if len(new_updates) == len(updates):
            raise HTTPException(status_code=404, detail="Update entry not found")
        daily_visit["updates"] = new_updates
        # Optionally, remove associated images from S3 for the deleted update.
        # (You could iterate over the update's images and call s3_client.delete_object.)
        db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)},
            {"$set": {"updates": new_updates, "updated_at": datetime.datetime.now()}},
        )
        updated_daily_visit = db.daily_visits.find_one(
            {"_id": ObjectId(daily_visit_id)}
        )
        return JSONResponse(
            status_code=200,
            content={
                "message": "Update deleted successfully",
                "daily_visit": serialize_mongo_document(updated_daily_visit),
            },
        )
    # Process update entries if update_text is provided.
    if update_text is not None:
        updates = daily_visit.get("updates", [])
        if update_id:
            # Edit an existing update entry.
            update_entry = next(
                (u for u in updates if str(u.get("_id")) == update_id), None
            )
            if not update_entry:
                raise HTTPException(status_code=404, detail="Update entry not found")
            update_entry["text"] = update_text

            # Update customer info if provided.
            if customer_id is not None:
                update_entry["customer_id"] = ObjectId(customer_id)
            if customer_name is not None:
                update_entry["customer_name"] = customer_name
            if potential_customer:
                update_entry["potential_customer_id"] = ObjectId(potential_customer_id)
                update_entry["potential_customer"] = potential_customer
                update_entry["potential_customer_name"] = potential_customer_name
                update_entry["potential_customer_address"] = potential_customer_address
                update_entry["potential_customer_tier"] = potential_customer_tier
            # Process deletion of images if requested.
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
            # Process uploading new images.
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
            if customer_id is not None:
                new_entry["customer_id"] = ObjectId(customer_id)
            if customer_name is not None:
                new_entry["customer_name"] = customer_name
            print(update_fields)
            if potential_customer:
                new_entry["potential_customer_id"] = ObjectId(potential_customer_id)
                new_entry["potential_customer"] = potential_customer
                new_entry["potential_customer_name"] = potential_customer_name
                new_entry["potential_customer_address"] = potential_customer_address
                new_entry["potential_customer_tier"] = potential_customer_tier
                new_entry["potential_customer_mobile"] = potential_customer_mobile
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

    # Prepare update fields.
    if shops is not None:
        update_fields["shops"] = daily_visit["shops"]
    elif plan is not None:
        update_fields["plan"] = daily_visit["plan"]
    if update_text is not None or "updates" in update_fields:
        update_fields["updates"] = daily_visit.get("updates", [])
    print(json.dumps(serialize_mongo_document(update_fields), indent=4))
    db.daily_visits.update_one(
        {"_id": ObjectId(daily_visit_id)},
        {"$set": update_fields},
    )
    updated_daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})

    # Convert created_at to IST for display.
    if "created_at" in updated_daily_visit:
        utc_dt = daily_visit["created_at"]
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
        ist_timezone = pytz.timezone("Asia/Kolkata")
        ist_dt = utc_dt.astimezone(ist_timezone)
        updated_daily_visit["created_at"] = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    user_obj = db.users.find_one({"email": "crmbarksales@gmail.com"})
    created_by_user = db.users.find_one(
        {"_id": ObjectId(daily_visit.get("created_by", ""))}
    )
    template = db.templates.find_one({"name": "update_daily_visit"})
    send_whatsapp(
        user_obj.get("phone"),
        {**template},
        {
            "name": user_obj.get("first_name", ""),
            "salesperson_name": created_by_user.get("first_name", ""),
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
