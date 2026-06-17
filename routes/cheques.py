from fastapi import APIRouter, HTTPException, Query, File, UploadFile, Depends
from fastapi.responses import JSONResponse
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import get_current_user
from .notifications import create_notification
from bson.objectid import ObjectId
from dotenv import load_dotenv
import boto3, os, uuid, datetime, re

load_dotenv()

router = APIRouter()
db = get_database()

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def _parse_salesperson_codes(value) -> list:
    """Normalize cf_sales_person / salesperson_name to a flat list of non-empty codes."""
    if not value:
        return []
    if isinstance(value, list):
        codes = []
        for item in value:
            codes.extend([c.strip() for c in str(item).split(",") if c.strip()])
        return list(dict.fromkeys(codes))  # deduplicate, preserve order
    return [c.strip() for c in str(value).split(",") if c.strip()]


def _notify_salespersons(salesperson_codes: list, customer_name: str):
    """Send in-app notification to every user whose code appears in salesperson_codes."""
    if not salesperson_codes:
        return
    for code in salesperson_codes:
        user = db.users.find_one(
            {
                "code": {"$regex": f"^{re.escape(code)}$", "$options": "i"},
                "role": {"$in": ["sales_person", "sales_admin"]},
                "status": "active",
            },
            {"_id": 1},
        )
        if user:
            create_notification(
                db,
                str(user["_id"]),
                "cheque_uploaded",
                f"Cheque uploaded for {customer_name}",
                f"Admin has uploaded a cheque image for customer {customer_name}.",
                "/cheques",
            )


@router.post("/upload")
async def upload_cheque_image(file: UploadFile = File(...)):
    try:
        ext = (file.filename or "").rsplit(".", 1)[-1].lower()
        allowed = {"jpg", "jpeg", "png", "webp", "pdf"}
        if ext not in allowed:
            raise HTTPException(
                status_code=400,
                detail="Only jpg/jpeg/png/webp/pdf files are allowed.",
            )
        file_key = f"cheques/{uuid.uuid4()}.{ext}"
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            file_key,
            ExtraArgs={"ContentType": file.content_type or "application/octet-stream"},
        )
        return {"url": f"{AWS_S3_URL}/{file_key}", "s3_key": file_key}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/customers")
def search_customers(name: str = Query("", min_length=0)):
    try:
        query: dict = {}
        if name.strip():
            query["contact_name"] = {"$regex": re.escape(name.strip()), "$options": "i"}
        cursor = db.customers.find(
            query,
            {
                "contact_name": 1,
                "contact_id": 1,
                "cf_sales_person": 1,
                "salesperson_name": 1,
            },
        ).limit(20)
        results = serialize_mongo_document(list(cursor))
        return {"customers": results}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/search/invoices")
def search_invoice(invoice_number: str = Query(...)):
    try:
        invoice = db.invoices.find_one(
            {
                "invoice_number": {
                    "$regex": f"^{re.escape(invoice_number.strip())}$",
                    "$options": "i",
                }
            },
            {
                "invoice_number": 1,
                "customer_name": 1,
                "customer_id": 1,
                "salesperson_name": 1,
                "cf_sales_person": 1,
            },
        )
        if not invoice:
            raise HTTPException(status_code=404, detail="Invoice not found")
        return serialize_mongo_document(invoice)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
async def create_cheque(payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_id = current_user.get("data", {}).get("_id") or current_user.get("_id")
        user_name = (
            current_user.get("data", {}).get("name")
            or current_user.get("name", "Admin")
        )

        customer_name = payload.get("customer_name", "").strip()
        customer_id = payload.get("customer_id", "")
        invoice_number = payload.get("invoice_number", "")
        invoice_id = payload.get("invoice_id", "")
        # salesperson_codes is already a flat list sent from frontend
        salesperson_codes = payload.get("salesperson_codes", [])
        if isinstance(salesperson_codes, str):
            salesperson_codes = _parse_salesperson_codes(salesperson_codes)
        images = payload.get("images", [])
        notes = payload.get("notes", "")

        if not customer_name:
            raise HTTPException(status_code=400, detail="customer_name is required")
        if not images:
            raise HTTPException(status_code=400, detail="At least one image is required")

        doc = {
            "customer_id": customer_id,
            "customer_name": customer_name,
            "invoice_number": invoice_number,
            "invoice_id": invoice_id,
            "salesperson_codes": salesperson_codes,
            "images": images,
            "notes": notes,
            "uploaded_by": ObjectId(user_id),
            "uploaded_by_name": user_name,
            "created_at": datetime.datetime.utcnow(),
        }

        result = db.cheques.insert_one(doc)
        _notify_salespersons(salesperson_codes, customer_name)

        return {"_id": str(result.inserted_id), "message": "Cheque created successfully"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("")
def get_cheques(
    page: int = Query(0, ge=0),
    limit: int = Query(20, ge=1),
    customer_name: str = Query(None),
    invoice_number: str = Query(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        user_data = current_user.get("data", current_user)
        user_role = user_data.get("role", "")
        user_code = user_data.get("code", "")

        query: dict = {}
        if user_role == "sales_person":
            if not user_code:
                return {"cheques": [], "total": 0, "page": page, "limit": limit}
            query["salesperson_codes"] = {
                "$elemMatch": {"$regex": f"^{re.escape(user_code)}$", "$options": "i"}
            }

        if customer_name and customer_name.strip():
            query["customer_name"] = {"$regex": re.escape(customer_name.strip()), "$options": "i"}
        if invoice_number and invoice_number.strip():
            query["invoice_number"] = {"$regex": re.escape(invoice_number.strip()), "$options": "i"}

        total = db.cheques.count_documents(query)
        docs = list(
            db.cheques.find(query)
            .sort("created_at", -1)
            .skip(page * limit)
            .limit(limit)
        )
        return {
            "cheques": serialize_mongo_document(docs),
            "total": total,
            "page": page,
            "limit": limit,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/{cheque_id}/images")
async def add_images(cheque_id: str, files: list[UploadFile] = File(...), current_user: dict = Depends(get_current_user)):
    """Admin uploads additional images to an existing cheque entry."""
    try:
        user_data = current_user.get("data", current_user)
        user_role = user_data.get("role", "")
        if user_role not in ("admin", "super_admin", "sales_admin"):
            raise HTTPException(status_code=403, detail="Only admins can add images")

        new_images = []
        for file in files:
            ext = (file.filename or "").rsplit(".", 1)[-1].lower()
            if ext not in {"jpg", "jpeg", "png", "webp", "pdf"}:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")
            file_key = f"cheques/{uuid.uuid4()}.{ext}"
            s3_client.upload_fileobj(
                file.file,
                AWS_S3_BUCKET_NAME,
                file_key,
                ExtraArgs={"ContentType": file.content_type or "application/octet-stream"},
            )
            new_images.append({"url": f"{AWS_S3_URL}/{file_key}", "s3_key": file_key})

        result = db.cheques.update_one(
            {"_id": ObjectId(cheque_id)},
            {"$push": {"images": {"$each": new_images}}},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Cheque not found")
        return {"added": new_images}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.patch("/{cheque_id}/notes")
async def update_notes(cheque_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    """Admin can edit the notes on a cheque entry."""
    try:
        user_data = current_user.get("data", current_user)
        user_role = user_data.get("role", "")
        if user_role not in ("admin", "super_admin", "sales_admin"):
            raise HTTPException(status_code=403, detail="Only admins can edit notes")
        notes = payload.get("notes", "")
        result = db.cheques.update_one(
            {"_id": ObjectId(cheque_id)},
            {"$set": {"notes": notes, "notes_updated_at": datetime.datetime.utcnow()}},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Cheque not found")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/{cheque_id}/comments")
async def add_comment(cheque_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    """Sales person (or admin) adds a comment to a cheque entry."""
    try:
        user_data = current_user.get("data", current_user)
        user_id = user_data.get("_id", "")
        user_name = user_data.get("name", "") or user_data.get("first_name", "")
        user_role = user_data.get("role", "")
        text = payload.get("text", "").strip()
        if not text:
            raise HTTPException(status_code=400, detail="Comment text is required")

        comment = {
            "_id": ObjectId(),
            "text": text,
            "created_by": ObjectId(user_id),
            "created_by_name": user_name,
            "role": user_role,
            "created_at": datetime.datetime.utcnow(),
        }
        result = db.cheques.update_one(
            {"_id": ObjectId(cheque_id)},
            {"$push": {"comments": comment}},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Cheque not found")

        # If sales person comments → notify admins
        if user_role in ("sales_person", "sales_admin"):
            cheque = db.cheques.find_one({"_id": ObjectId(cheque_id)}, {"customer_name": 1})
            customer_name = cheque.get("customer_name", "") if cheque else ""
            admins = db.users.find(
                {"role": {"$in": ["admin", "super_admin"]}, "status": "active"},
                {"_id": 1},
            )
            for admin in admins:
                create_notification(
                    db,
                    str(admin["_id"]),
                    "cheque_comment",
                    f"Comment on cheque – {customer_name}",
                    f"{user_name} commented: {text[:80]}",
                    "/admin/cheques",
                )

        return serialize_mongo_document({"_id": comment["_id"], "ok": True})
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{cheque_id}")
def delete_cheque(cheque_id: str, current_user: dict = Depends(get_current_user)):
    try:
        user_data = current_user.get("data", current_user)
        user_role = user_data.get("role", "")
        if user_role not in ("admin", "super_admin"):
            raise HTTPException(status_code=403, detail="Only admins can delete cheques")

        cheque = db.cheques.find_one({"_id": ObjectId(cheque_id)})
        if not cheque:
            raise HTTPException(status_code=404, detail="Cheque not found")

        for img in cheque.get("images", []):
            s3_key = img.get("s3_key")
            if s3_key:
                try:
                    s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=s3_key)
                except Exception:
                    pass

        db.cheques.delete_one({"_id": ObjectId(cheque_id)})
        return {"message": "Cheque deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
