from fastapi import APIRouter, Query, HTTPException, File, UploadFile, Form, Response
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from typing import Optional, List
from bson import ObjectId
import re, uuid, boto3, os, requests
from datetime import date, datetime
from urllib.parse import urlparse
from .helpers import get_access_token

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]
invoice_collection = db["invoices"]
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    region_name=os.getenv("S3_REGION"),
)
bucket_name = os.getenv("S3_BUCKET_NAME")

org_id = os.getenv("ORG_ID")
ESTIMATE_URL = os.getenv("ESTIMATE_URL")
INVOICE_PDF_URL = os.getenv("INVOICE_PDF_URL")


def get_invoice(
    invoice_id: str,
):
    result = invoice_collection.find_one(
        {"_id": ObjectId(invoice_id), "status": {"$nin": ["void", "paid"]}}
    )
    if result:
        invoice = result
        invoice["status"] = str(invoice["status"]).capitalize()
        return serialize_mongo_document(invoice)
    return None


@router.get("")
def get_invoices(
    created_by: str = Query(""),
    # search: Optional[str] = Query(None, description="Search term for name or SKU code"),
):
    """
    Retrieves paginated invoices with optional filters.
    It also includes the number of days an invoice is overdue, calculated as the difference between today's date and the due_date.
    """
    # Retrieve the user document
    user = db.users.find_one({"_id": ObjectId(created_by)})
    code = user.get("code", "")

    # Define forbidden keywords for salesperson fields
    forbidden_keywords = (
        "(Company customers|defaulters|Amazon|staff purchase|marketing inv's)"
    )

    # Today's date in ISO format (YYYY-MM-DD)
    today_str = date.today().isoformat()
    escaped_sales_person = re.escape(code)

    # Build the query to match invoices past their due date and not marked as paid.
    query = {
        "due_date": {"$lt": today_str},
        "status": {"$nin": ["paid", "void"]},
        # Must match 'code' in either cf_sales_person or salesperson_name
        "$or": [
            {
                "cf_sales_person": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
            {
                "salesperson_name": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
        ],
        # Exclude documents if cf_sales_person or salesperson_name contains any forbidden keywords
        "cf_sales_person": {"$not": {"$regex": forbidden_keywords, "$options": "i"}},
        "salesperson_name": {"$not": {"$regex": forbidden_keywords, "$options": "i"}},
    }

    # Define the projection, including a new field to calculate the overdue days.
    project = {
        "_id": 1,
        "invoice_id": 1,
        "invoice_number": 1,
        "status": {"$toString": "overdue"},
        "date": 1,
        "due_date": 1,
        "customer_id": 1,
        "customer_name": 1,
        "total": 1,
        "balance": 1,
        "cf_sales_person": 1,
        "salesperson_name": 1,
        "created_at": 1,
        "overdue_by_days": {
            "$dateDiff": {
                "startDate": {"$dateFromString": {"dateString": "$due_date"}},
                "endDate": "$$NOW",
                "unit": "day",
            },
        },
        "invoice_notes": 1,
    }

    # Construct the aggregation pipeline
    pipeline = [
        {"$match": query},
        {
            "$lookup": {
                "from": "invoice_notes",  # Collection to join
                "localField": "invoice_number",  # Field from the invoices collection
                "foreignField": "invoice_number",  # Field from the invoice_notes collection
                "as": "invoice_notes",  # The result will be an array of matching documents
            }
        },
        {
            "$unwind": {
                "path": "$invoice_notes",  # Unwind the array of invoice_notes
                "preserveNullAndEmptyArrays": True,  # Keep invoices even if no notes exist
            }
        },
        {"$sort": {"due_date": -1}},  # Latest first
        {"$project": project},
    ]

    # Execute the pipeline
    try:
        fetched_invoices = list(db.invoices.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Serialize the documents
    all_invoices = [serialize_mongo_document(doc) for doc in fetched_invoices]

    # Count total matching documents
    try:
        total_invoices = db.invoices.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    print(total_invoices)
    # Return the response
    return {
        "invoices": all_invoices,
        "total": total_invoices,
    }


@router.post("/notes")
async def create_invoice_note(
    invoice_number: str = Form(...),
    created_by: str = Form(...),
    additional_info: Optional[str] = Form(None),
    images: Optional[List[UploadFile]] = File(None),
):
    # Validate that at least one of additional_info or images is provided
    if not additional_info and not images:
        raise HTTPException(
            status_code=400, detail="Either additional info or images are required."
        )

    saved_images = []
    if images:
        # Set up the S3 client with credentials from environment variables

        if not bucket_name:
            raise HTTPException(
                status_code=500, detail="S3_BUCKET_NAME not configured."
            )

        for image in images:
            # Generate a unique filename for each image
            unique_filename = (
                f"invoice_notes/{invoice_number}/{uuid.uuid4()}_{image.filename}"
            )
            try:
                # Ensure the file pointer is at the start
                image.file.seek(0)
                # Upload the image file object to S3
                s3_client.upload_fileobj(
                    image.file,
                    bucket_name,
                    unique_filename,
                    ExtraArgs={"ContentType": image.content_type},
                )
                saved_images.append(f"{os.getenv('S3_URL')}/{unique_filename}")
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error uploading image: {str(e)}"
                )

    note_document = {
        "invoice_number": invoice_number,
        "additional_info": additional_info,
        "images": saved_images,
        "created_by": ObjectId(created_by),
        "created_at": datetime.now(),
    }

    result = db.invoice_notes.insert_one(note_document)
    return {
        "message": "Invoice note created successfully",
        "id": str(result.inserted_id),
    }


@router.get("/notes")
async def get_invoice_note(invoice_number: str):
    """
    GET /notes?invoice_number=...

    Returns the invoice note for the given invoice number if it exists.
    Otherwise, returns a message that no note exists.
    """
    print("invoice_number", invoice_number)
    try:
        note = serialize_mongo_document(
            dict(db.invoice_notes.find_one({"invoice_number": invoice_number}))
        )
        if note:
            return note
    except Exception as e:
        return "Note not found"


@router.put("/notes")
async def update_invoice_note(
    invoice_number: str = Form(...),
    additional_info: Optional[str] = Form(None),
    images: Optional[List[UploadFile]] = File(None),
):
    """
    PUT /notes

    Updates an existing invoice note identified by invoice_number. You can update
    additional_info and/or upload new images. Uploaded images are stored on S3.
    """
    # Check if the note exists first
    note = db.invoice_notes.find_one({"invoice_number": invoice_number})
    if not note:
        raise HTTPException(
            status_code=404, detail="Invoice note not found. Please create one first."
        )
    print(note.get("images"))
    saved_images = note.get("images", [])
    if images:
        for image in images:
            # Generate a unique filename for each image
            unique_filename = (
                f"invoice_notes/{invoice_number}/{uuid.uuid4()}_{image.filename}"
            )
            try:
                # Ensure the file pointer is at the beginning
                image.file.seek(0)
                # Upload image to S3
                s3_client.upload_fileobj(
                    image.file,
                    bucket_name,
                    unique_filename,
                    ExtraArgs={"ContentType": image.content_type},
                )
                saved_images.append(f"{os.getenv('S3_URL')}/{unique_filename}")
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error uploading image: {str(e)}"
                )

    update_data = {}
    if additional_info is not None:
        update_data["additional_info"] = additional_info
    if saved_images:
        # In this example, we replace existing images with the new ones.
        update_data["images"] = saved_images

    if update_data:
        update_data["updated_at"] = datetime.now()
        db.invoice_notes.update_one(
            {"invoice_number": invoice_number}, {"$set": update_data}
        )

    return {"message": "Invoice note updated successfully"}


@router.delete("/notes/image")
async def delete_invoice_note_image(
    invoice_number: str = Query(..., description="The invoice number"),
    image_url: str = Query(..., description="The image filename to delete from S3"),
):
    """
    Delete an image from S3 and remove its reference from the invoice note document in MongoDB.
    """
    # Verify the invoice note exists in the database.
    note = db.invoice_notes.find_one({"invoice_number": invoice_number})
    if not note:
        raise HTTPException(status_code=404, detail="Invoice note not found.")
    parsed_url = urlparse(image_url)
    key = parsed_url.path.lstrip("/")
    # Attempt to delete the image from S3.
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error deleting image from S3: {str(e)}"
        )

    # Remove the image reference from the MongoDB document.
    result = db.invoice_notes.update_one(
        {"invoice_number": invoice_number}, {"$pull": {"images": image_url}}
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Image not found in invoice note.")

    return {"message": "Image deleted successfully"}


@router.get("/download_pdf/{invoice_id}")
async def download_pdf(invoice_id: str = ""):
    try:
        # Check if the order exists in the database
        invoice = db.invoices.find_one({"_id": ObjectId(invoice_id)})
        if invoice is None:
            raise HTTPException(status_code=404, detail="Invoice Not Found")

        # Get the invoice_id and make the request to Zoho
        invoice_id = invoice.get("invoice_id", "")
        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        response = requests.get(
            url=INVOICE_PDF_URL.format(org_id=org_id, invoice_id=invoice_id),
            headers=headers,
            allow_redirects=False,  # Prevent automatic redirects
        )

        # Check if the response from Zoho is successful (200)
        if response.status_code == 200:
            # Return the PDF content
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename=order_{invoice_id}.pdf"
                },
            )
        elif response.status_code == 307:
            raise HTTPException(
                status_code=307,
                detail="Redirect encountered. Check Zoho endpoint or token.",
            )
        else:
            # Raise an exception if Zoho's API returns an error
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch PDF: {response.text}",
            )

    except HTTPException as e:
        print(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{invoice_id}")
def read_invoice(invoice_id: str):
    """
    Retrieve an Invoice by its _id field.
    """
    invoice = get_invoice(invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return invoice
