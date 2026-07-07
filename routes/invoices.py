from fastapi import APIRouter, Query, HTTPException, File, UploadFile, Form, Response, Depends
from pydantic import BaseModel
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import get_current_user
from typing import Optional, List
from bson import ObjectId
import re, uuid, boto3, os, requests, json
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
from .helpers import get_access_token, fetch_overdue_invoices, fetch_associated_credit_notes

router = APIRouter()

db = get_database()
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
        open_credit_notes = list(
            db.credit_notes.aggregate(
                [
                    {
                        "$match": {
                            "customer_id": invoice.get("customer_id"),
                            "status": {"$nin": ["void", "closed"]},
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total": {
                                "$sum": {"$toDouble": {"$ifNull": ["$balance", 0]}}
                            },
                        }
                    },
                ]
            )
        )
        invoice["open_credit_note_amt"] = (
            open_credit_notes[0]["total"] if open_credit_notes else 0
        )
        associated_cns = fetch_associated_credit_notes(
            db,
            [
                {
                    "invoice_id": invoice.get("invoice_id"),
                    "customer_id": invoice.get("customer_id"),
                }
            ],
        )
        invoice["associated_credit_notes"] = associated_cns.get(
            invoice.get("invoice_id"), []
        )
        invoice_note = db.invoice_notes.find_one(
            {"invoice_number": invoice.get("invoice_number")}
        )
        invoice["invoice_notes"] = (
            serialize_mongo_document(invoice_note) if invoice_note else None
        )
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
    escaped_sales_person = re.escape(code)

    # Must match 'code' in either cf_sales_person or salesperson_name, and
    # exclude documents whose cf_sales_person/salesperson_name contain any
    # forbidden keyword.
    extra_query = {
        "$and": [
            {
                "$or": [
                    {"cf_sales_person": code},
                    {"cf_sales_person": {"$elemMatch": {"$eq": code}}},
                    {
                        "cf_sales_person": {
                            "$regex": f"(^\\s*|,\\s*){escaped_sales_person}(\\s*,|\\s*$)",
                            "$options": "i",
                        }
                    },
                    {
                        "salesperson_name": {
                            "$regex": f"(^\\s*|,\\s*){escaped_sales_person}(\\s*,|\\s*$)",
                            "$options": "i",
                        }
                    },
                    {"cf_sales_person": "Defaulter"},
                    {"cf_sales_person": "Company customers"},
                ]
            },
            {
                "$and": [
                    {"cf_sales_person": {"$not": {"$regex": forbidden_keywords, "$options": "i"}}},
                    {"salesperson_name": {"$not": {"$regex": forbidden_keywords, "$options": "i"}}}
                ]
            }
        ]
    }

    matched = fetch_overdue_invoices(db, extra_query)

    invoice_numbers = [d.get("invoice_number") for d in matched]
    customer_ids = [d.get("customer_id") for d in matched]

    associated_cns = fetch_associated_credit_notes(db, matched)

    notes_by_invoice = {
        n["invoice_number"]: n
        for n in db.invoice_notes.find({"invoice_number": {"$in": invoice_numbers}})
    }
    credit_note_totals = {}
    for row in db.credit_notes.aggregate([
        {"$match": {"customer_id": {"$in": customer_ids}, "status": {"$nin": ["void", "closed"]}}},
        {"$group": {"_id": "$customer_id", "total": {"$sum": {"$toDouble": {"$ifNull": ["$balance", 0]}}}}},
    ]):
        credit_note_totals[row["_id"]] = row["total"]

    all_invoices = []
    for doc in matched:
        all_invoices.append(serialize_mongo_document({
            "_id": doc.get("_id"),
            "invoice_id": doc.get("invoice_id"),
            "invoice_number": doc.get("invoice_number"),
            "status": "overdue",
            "date": doc.get("date"),
            "due_date": doc.get("due_date"),
            "customer_id": doc.get("customer_id"),
            "customer_name": doc.get("customer_name"),
            "total": doc.get("total"),
            "balance": doc.get("balance"),
            "cf_sales_person": doc.get("cf_sales_person"),
            "salesperson_name": doc.get("salesperson_name"),
            "created_at": doc.get("created_at"),
            "overdue_by_days": doc.get("overdue_by_days"),
            "invoice_notes": notes_by_invoice.get(doc.get("invoice_number")),
            "open_credit_note_amt": credit_note_totals.get(doc.get("customer_id"), 0),
            "associated_credit_notes": associated_cns.get(doc.get("invoice_id"), []),
        }))

    # Return the response
    return {
        "invoices": all_invoices,
        "total": len(all_invoices),
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


class InvoiceFollowUpUpdate(BaseModel):
    invoice_number: str
    sp_remarks: Optional[str] = None
    payment_cleared_details: Optional[str] = None
    expected_payment_date: Optional[str] = None
    office_team_remarks: Optional[str] = None


def _sales_person_owns_invoice(invoice_number: str, code: str) -> bool:
    invoice = db.invoices.find_one({"invoice_number": invoice_number})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found.")
    escaped_code = re.escape(code or "")
    pattern = re.compile(rf"(^\s*|,\s*){escaped_code}(\s*,|\s*$)", re.IGNORECASE)
    cf_sp = invoice.get("cf_sales_person") or ""
    sp_name = invoice.get("salesperson_name") or ""
    return bool(pattern.search(cf_sp) or pattern.search(sp_name))


@router.patch("/notes/fields")
async def update_invoice_note_fields(
    payload: InvoiceFollowUpUpdate,
    current_user: dict = Depends(get_current_user),
):
    """
    Partial update of the payments-due follow-up fields on an invoice_notes
    document: sp_remarks, payment_cleared_details, expected_payment_date
    (sales person editable) and office_team_remarks (admin/sales_admin only).
    """
    user_data = current_user.get("data", current_user)
    role = user_data.get("role", "")
    code = user_data.get("code", "")

    update_data = {}
    if payload.sp_remarks is not None:
        update_data["sp_remarks"] = payload.sp_remarks
    if payload.payment_cleared_details is not None:
        update_data["payment_cleared_details"] = payload.payment_cleared_details
    if payload.expected_payment_date is not None:
        update_data["expected_payment_date"] = payload.expected_payment_date
    if payload.office_team_remarks is not None:
        if role not in ("admin", "sales_admin"):
            raise HTTPException(
                status_code=403,
                detail="Only admin/office team can edit Office Team Remarks.",
            )
        update_data["office_team_remarks"] = payload.office_team_remarks

    if not update_data:
        raise HTTPException(status_code=400, detail="No fields provided to update.")

    if role == "sales_person" and not _sales_person_owns_invoice(
        payload.invoice_number, code
    ):
        raise HTTPException(
            status_code=403, detail="You do not have access to this invoice."
        )

    update_data["updated_at"] = datetime.now()
    db.invoice_notes.update_one(
        {"invoice_number": payload.invoice_number},
        {
            "$set": update_data,
            "$setOnInsert": {
                "invoice_number": payload.invoice_number,
                "created_at": datetime.now(),
            },
        },
        upsert=True,
    )
    return {"message": "Invoice follow-up fields updated successfully"}


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


@router.get("/download_pdf/zoho/{zoho_invoice_id}")
async def download_pdf_by_zoho_id(zoho_invoice_id: str):
    """Download invoice PDF directly from Zoho Books using Zoho's invoice_id."""
    try:
        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        response = requests.get(
            url=INVOICE_PDF_URL.format(org_id=org_id, invoice_id=zoho_invoice_id),
            headers=headers,
            allow_redirects=False,
        )
        if response.status_code == 200:
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename=invoice_{zoho_invoice_id}.pdf"},
            )
        # Do NOT propagate Zoho's status code verbatim: a 401/403 from Zoho
        # (expired OAuth token, wrong org, etc.) would otherwise reach the
        # browser and the axios interceptor would log the user out. Surface
        # upstream failures as a gateway error instead.
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch PDF from Zoho ({response.status_code}): {response.text}",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                status_code=502,
                detail="Redirect encountered. Check Zoho endpoint or token.",
            )
        else:
            # Do NOT propagate Zoho's status code verbatim. A 401/403 from Zoho
            # (expired OAuth token, wrong org, etc.) would reach the browser and
            # the axios interceptor would treat it as the customer's session
            # expiring and log them out. Surface upstream failures as a gateway
            # error so the client just sees a failed download.
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch PDF from Zoho ({response.status_code}): {response.text}",
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
