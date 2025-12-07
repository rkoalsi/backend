from fastapi import APIRouter, Query, HTTPException, File, UploadFile, Form
from ..config.root import get_database, serialize_mongo_document
from typing import Optional, List
from bson import ObjectId
from datetime import datetime
import re, boto3, os, uuid

router = APIRouter()

db = get_database()
shipments_collection = db["shipments"]
customers_collection = db["customers"]
invoices_collection = db["invoices"]

# S3 Configuration
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    region_name=os.getenv("S3_REGION"),
)
bucket_name = os.getenv("S3_BUCKET_NAME")


@router.get("")
def get_shipments(
    created_by: str = Query(..., description="User ID of the salesperson"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by customer name or shipment number"),
    status: Optional[str] = Query(None, description="Filter by shipment status"),
    role: str = Query("salesperson", description="User role (admin or salesperson)"),
):
    """
    Retrieves shipments for customers assigned to the salesperson.
    For admins, returns all shipments.
    Joins shipments with customers collection to filter by SP code.
    """
    # Get the user
    user = db.users.find_one({"_id": ObjectId(created_by)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Check if user is admin
    is_admin = "admin" in role.lower() or "admin" in str(user.get("role", "")).lower()

    sp_code = ""
    if not is_admin:
        sp_code = user.get("code", "")
        if not sp_code:
            # Return empty results if user has no SP code
            return {
                "shipments": [],
                "total": 0,
                "page": page,
                "per_page": per_page,
                "total_pages": 0,
            }

    # Build aggregation pipeline
    pipeline = []

    # For salesperson, first get their customer IDs, then filter shipments
    if not is_admin:
        # Get all customer IDs for this salesperson
        customer_ids = []
        try:
            # Escape special regex characters in sp_code
            escaped_sp_code = re.escape(sp_code)
            customers = customers_collection.find(
                {
                    "$or": [
                        {"cf_sales_person": sp_code},
                        {"cf_sales_person": {"$elemMatch": {"$eq": sp_code}}},
                        {"cf_sales_person": {"$regex": f"(^\\s*|,\\s*){escaped_sp_code}(\\s*,|\\s*$)", "$options": "i"}},
                        {"salesperson_name": {"$regex": f"(^\\s*|,\\s*){escaped_sp_code}(\\s*,|\\s*$)", "$options": "i"}},
                        {"cf_sales_person": "Defaulter"},
                        {"cf_sales_person": "Company customers"},
                    ]
                },
                {"contact_id": 1, "customer_id": 1}
            )
            for customer in customers:
                if customer.get("contact_id"):
                    customer_ids.append(str(customer["contact_id"]))
                if customer.get("customer_id"):
                    customer_ids.append(str(customer["customer_id"]))
        except Exception as e:
            print(f"Error fetching customer IDs: {e}")
            customer_ids = []

        if not customer_ids:
            return {
                "shipments": [],
                "total": 0,
                "page": page,
                "per_page": per_page,
                "total_pages": 0,
            }

        # Filter shipments by customer IDs
        pipeline.append({
            "$match": {
                "$expr": {
                    "$in": [{"$toString": "$customer_id"}, customer_ids]
                }
            }
        })

    # Add search filter if provided
    if search:
        pipeline.append({
            "$match": {
                "$or": [
                    {"customer_name": {"$regex": search, "$options": "i"}},
                    {"shipment_number": {"$regex": search, "$options": "i"}},
                    {"tracking_number": {"$regex": search, "$options": "i"}},
                    {"salesorder_number": {"$regex": search, "$options": "i"}},
                ]
            }
        })

    # Add status filter if provided
    if status:
        pipeline.append({
            "$match": {
                "status": {"$regex": f"^{status}$", "$options": "i"}
            }
        })

    # Get total count before pagination and sorting
    count_pipeline = pipeline.copy()
    count_pipeline.append({"$count": "total"})

    # Sort by date descending (most recent first)
    pipeline.append({"$sort": {"date": -1, "created_at": -1}})

    try:
        count_result = list(shipments_collection.aggregate(count_pipeline, allowDiskUse=True))
        total = count_result[0]["total"] if count_result else 0
    except Exception as e:
        print(f"Error counting shipments: {e}")
        total = 0

    print(f"Total shipments found: {total} for sp_code: {sp_code}")

    # Add pagination
    skip = (page - 1) * per_page
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": per_page})

    # Lookup invoice_number from invoices collection using salesorder_id
    pipeline.append({
        "$lookup": {
            "from": "invoices",
            "let": {"so_id": "$salesorder_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$eq": ["$salesorder_id", "$$so_id"]
                        }
                    }
                },
                {"$project": {"invoice_number": 1}}
            ],
            "as": "invoice_info"
        }
    })

    # Add invoice_number field from lookup result
    pipeline.append({
        "$addFields": {
            "invoice_number": {"$arrayElemAt": ["$invoice_info.invoice_number", 0]}
        }
    })

    # Project only necessary fields
    pipeline.append({
        "$project": {
            "_id": 1,
            "shipment_id": 1,
            "shipment_number": 1,
            "salesorder_id": 1,
            "salesorder_number": 1,
            "customer_id": 1,
            "customer_name": 1,
            "status": 1,
            "tracking_number": 1,
            "carrier": 1,
            "date": 1,
            "created_at": 1,
            "shipping_charge": 1,
            "delivery_method": 1,
            "total": 1,
            "shipping_address": 1,
            "line_items": 1,
            "invoice_number": 1,
            "images": 1,
        }
    })

    # Execute the pipeline
    try:
        shipments = list(shipments_collection.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error fetching shipments: {e}")
        raise HTTPException(status_code=500, detail="Error fetching shipments")

    # Serialize the documents
    serialized_shipments = [serialize_mongo_document(doc) for doc in shipments]

    return {
        "shipments": serialized_shipments,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
    }


@router.get("/{shipment_id}")
def get_shipment(shipment_id: str):
    """
    Retrieve a single shipment by its _id.
    """
    try:
        shipment = shipments_collection.find_one({"_id": ObjectId(shipment_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid shipment ID")

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    return serialize_mongo_document(shipment)


@router.post("/{shipment_id}/images")
async def upload_shipment_images(
    shipment_id: str,
    images: List[UploadFile] = File(...),
    captions: Optional[str] = Form(None),
    uploaded_by: str = Form(...),
):
    """
    Upload multiple images for a shipment with optional captions.
    captions should be a JSON string array matching the images array length.
    """
    try:
        # Verify shipment exists
        shipment = shipments_collection.find_one({"_id": ObjectId(shipment_id)})
        if not shipment:
            raise HTTPException(status_code=404, detail="Shipment not found")

        # Parse captions if provided
        import json
        captions_list = []
        if captions:
            try:
                captions_list = json.loads(captions)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid captions format")

        # Ensure captions list matches images length
        while len(captions_list) < len(images):
            captions_list.append("")

        uploaded_images = []
        for idx, image in enumerate(images):
            # Validate file type
            if not image.content_type.startswith("image/"):
                raise HTTPException(status_code=400, detail=f"File {image.filename} is not an image")

            # Generate unique filename
            file_extension = os.path.splitext(image.filename)[1]
            unique_filename = f"shipment_images/{shipment_id}/{uuid.uuid4()}{file_extension}"

            try:
                # Upload to S3
                image.file.seek(0)
                s3_client.upload_fileobj(
                    image.file,
                    bucket_name,
                    unique_filename,
                    ExtraArgs={"ContentType": image.content_type},
                )

                # Construct S3 URL
                s3_url = f"{os.getenv('S3_URL')}/{unique_filename}"

                # Create image document
                image_doc = {
                    "url": s3_url,
                    "caption": captions_list[idx] if idx < len(captions_list) else "",
                    "uploaded_at": datetime.now(),
                    "uploaded_by": ObjectId(uploaded_by),
                }
                uploaded_images.append(image_doc)

            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error uploading image: {str(e)}")

        # Update shipment with new images
        current_images = shipment.get("images", [])
        updated_images = current_images + uploaded_images

        shipments_collection.update_one(
            {"_id": ObjectId(shipment_id)},
            {"$set": {"images": updated_images}}
        )

        return {
            "message": "Images uploaded successfully",
            "images": [serialize_mongo_document(img) for img in uploaded_images],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/{shipment_id}/images")
def get_shipment_images(shipment_id: str):
    """
    Get all images for a shipment.
    """
    try:
        shipment = shipments_collection.find_one({"_id": ObjectId(shipment_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid shipment ID")

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    images = shipment.get("images", [])
    return {"images": [serialize_mongo_document(img) for img in images]}


@router.delete("/{shipment_id}/images/{image_index}")
def delete_shipment_image(shipment_id: str, image_index: int):
    """
    Delete a specific image from a shipment by its index.
    """
    try:
        shipment = shipments_collection.find_one({"_id": ObjectId(shipment_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid shipment ID")

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    images = shipment.get("images", [])

    if image_index < 0 or image_index >= len(images):
        raise HTTPException(status_code=404, detail="Image not found")

    # Remove image from array
    images.pop(image_index)

    # Update shipment
    shipments_collection.update_one(
        {"_id": ObjectId(shipment_id)},
        {"$set": {"images": images}}
    )

    return {"message": "Image deleted successfully"}
