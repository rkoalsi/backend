from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Query
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from backend.config.constants import GST_STATE_CODES, STATE_CODES  # type: ignore
import re, requests, os, json, time
from bson.objectid import ObjectId
from dotenv import load_dotenv
from .helpers import get_access_token
from PIL import Image
from io import BytesIO
import boto3, traceback
from datetime import datetime
from typing import Optional
from bson.json_util import dumps
from fastapi.responses import JSONResponse
from math import ceil

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()
org_id = os.getenv("ORG_ID")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
# Dictionary to map GST state codes to state names


def validate_gst_number(gst_number: str):
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
    """
    Validate GST number, extract PAN, and determine state code.

    Args:
        gst_number (str): The GSTIN number.

    Returns:
        dict: Validation result with GST number validity, PAN, and state code.
    """
    gst_pattern = r"^(\d{2})([A-Z]{5}\d{4}[A-Z])(\d)([A-Z])([0-9A-Z])$"

    match = re.match(gst_pattern, gst_number)
    if not match:
        return {"valid": False, "error": "Invalid GST Number format"}

    state_code = match.group(1)
    pan = match.group(2)

    # Check if state code exists in the mapping
    state = GST_STATE_CODES.get(state_code, "Invalid State Code")
    # x = requests.get(
    #     url=f"https://books.zoho.com/api/v3/search/gstin?gstin={gst_number}&organization_id={org_id}",
    #     headers=headers,
    # )
    # print(x.json())
    # business_name = x.json()["data"]["business_name"]
    # status = str(x.json()["data"]["status"]).lower()
    return {
        "valid": True,
        "gst_number": gst_number,
        "pan": pan,
        "state_code": state_code,
        "state": state,
        # "status": status,
        # "business_name": business_name,
    }


def create_address_on_zoho(address, customer):
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
    x = requests.post(
        f"https://www.zohoapis.com/books/v3/contacts/{customer.get('contact_id')}/address?organization_id={org_id}",
        headers=headers,
        json=address,
    )
    response = x.json()
    return response


@router.get("")
def get_customers(
    name: Optional[str] = None,
    role: str = "salesperson",
    user_code: Optional[str] = None,  # Salesperson's code (if applicable)
    sort: Optional[str] = None,
):
    customers = []
    query = {}

    # Filter by role and status
    if role == "salesperson":
        query["status"] = "active"
        # If salesperson, restrict to assigned customers or special cases
        if user_code:
            query["$or"] = [
                {"cf_sales_person": {"$regex": f"\\b{user_code}\\b", "$options": "i"}},
                {"cf_sales_person": "Defaulter"},
                {"cf_sales_person": "Company customers"},
            ]

    # Filter by name if provided
    if name:
        query["contact_name"] = re.compile(name, re.IGNORECASE)

    # Sort logic
    sort_order = [("status", 1)]  # Default: Ascending order of status
    if sort and sort.lower() == "desc":
        sort_order = [("status", -1)]  # Descending order

    customers = [
        serialize_mongo_document(doc)
        for doc in db.customers.find(query).sort(sort_order)
    ]
    # Return the response as JSON
    return {"customers": customers}


@router.get("/salesperson")
def get_customers_for_sales_person(
    code: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search: Optional[str] = None,
):
    """
    Returns active customers that do NOT match the given salesperson code,
    with pagination + optional search support.
    """
    or_condition = [
        {"cf_sales_person": {"$not": {"$regex": f"\\b{code}\\b", "$options": "i"}}},
        {"cf_sales_person": {"$not": {"$regex": "Defaulter", "$options": "i"}}},
        {"cf_sales_person": {"$not": {"$regex": "Company customers", "$options": "i"}}},
    ]

    # We'll build a top-level $and to combine status + $or + optional search
    query = {
        "$and": [
            {"status": "active"},
            {"$or": or_condition},
        ]
    }

    # If we have a search term, add a contact_name regex condition as well
    if search:
        # We can push this into the $and array
        query["$and"].append({"contact_name": {"$regex": search, "$options": "i"}})

    total_count = db.customers.count_documents(query)

    skip = (page - 1) * limit
    cursor = db.customers.find(query).skip(skip).limit(limit)

    customers = [serialize_mongo_document(doc) for doc in cursor]
    total_pages = ceil(total_count / limit) if total_count else 1

    return {
        "customers": customers,
        "totalCount": total_count,
        "totalPages": total_pages,
        "currentPage": page,
        "limit": limit,
    }


@router.get("/validate_gst")
def validate_gst(gst_in: str = Query(..., min_length=15, max_length=15)):
    print(gst_in)
    return validate_gst_number(gst_in)


@router.post("/address")
def add_address(data: dict):
    print(data)
    order_id = data.get("order_id")
    address = data.get("address")
    state = address.get("state")
    address["country_code"] = "IN"
    address["state_code"] = STATE_CODES[state]
    print(json.dumps(address, indent=4))
    if not order_id:
        return HTTPException(status_code=400, detail="Order Is Required")
    if not address:
        return HTTPException(status_code=400, detail="Address Is Required")
    order = db.orders.find_one({"_id": ObjectId(order_id)})
    customer_id = order.get("customer_id", "")
    customer = db.customers.find_one({"_id": ObjectId(customer_id)})
    response = create_address_on_zoho(customer=customer, address=address)
    if response.get("code") == 0:
        zoho_address = response.get("address_info")
        result = db.customers.update_one(
            {"_id": ObjectId(customer_id)}, {"$push": {"addresses": zoho_address}}
        )
        if result.modified_count > 0:
            return {"status": "success", "message": "Address added successfully"}
        else:
            return {
                "status": "failure",
                "message": "Customer not found or address not added",
            }


# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
)


@router.post("/upload/sign")
async def signature_upload(
    signature: UploadFile = File(...),
    customer_id: str = Form(...),
    order_id: str = Form(...),
):
    try:
        print(f"Customer ID: {customer_id}")
        content = await signature.read()
        image = Image.open(BytesIO(content))
        time_var = int(time.time())
        file_name = f"{customer_id}_sign_{time_var}.png"
        image.save(file_name, format="PNG")

        # Process transparency
        if image.mode in ("RGBA", "LA") or (
            image.mode == "P" and "transparency" in image.info
        ):
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[3])
            image = background

        processed_file_name = f"{customer_id}_sign_with_white_background_{time_var}.png"
        image.save(processed_file_name, format="PNG")

        # Upload to S3
        s3_white_key = f"signatures/{processed_file_name}"
        s3_raw_key = f"signatures/{file_name}"
        s3_client.upload_file(
            processed_file_name,
            S3_BUCKET_NAME,
            s3_white_key,
            ExtraArgs={"ACL": "public-read"},
        )
        s3_client.upload_file(
            file_name, S3_BUCKET_NAME, s3_raw_key, ExtraArgs={"ACL": "public-read"}
        )

        s3_white_url = (
            f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_white_key}"
        )
        s3_raw_url = (
            f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_raw_key}"
        )
        db.signatures.insert_one(
            {
                "customer_id": ObjectId(customer_id),
                "order_id": ObjectId(order_id),
                "raw_signature": s3_raw_url,
                "white_signature": s3_white_url,
            }
        )
        db.orders.update_one(
            {
                "_id": ObjectId(order_id),
            },
            {"$set": {"signed_by_customer": True, "signature_url": s3_white_url}},
        )
        os.remove(processed_file_name)
        os.remove(file_name)
        return JSONResponse(
            {
                "message": "Signature uploaded and saved to S3",
                "raw_signature": s3_raw_url,
                "white_signature": s3_white_url,
            }
        )
    except Exception as e:
        error_message = traceback.format_exc()
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {error_message}"
        )


@router.post("")
def create_customer(customer: dict):
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
    contact_name = customer.get("customer_name", "")
    contact_mobile = customer.get("customer_mobile", "")
    contact_email = customer.get("customer_email", "")
    contact_salutation = customer.get("customer_salutation", "Mr.")
    company_name = customer.get("company_name", "")
    contact_type = customer.get("customer_type", "customer")
    payment_terms_label = customer.get("payment_terms", "Due on Receipt")
    payment_terms = (
        0
        if payment_terms_label == "Due on Receipt"
        else int(payment_terms_label.split(" ")[1])
    )
    billing_address = customer.get("billing_address", {})
    shipping_address = customer.get("shipping_address", {})
    gst_treatment = customer.get("gst_treatment", "business_gst")
    gst_number = customer.get("gst_number", "")
    contact_sub_type = customer.get("customer_sub_type", "business")
    validation = validate_gst_number(gst_number)
    pan = validation.get("pan")
    gst_valid = validation.get("valid")
    # business_name = validation.get("business_name")
    place_of_supply = validation.get("state_code")
    if gst_valid:
        # GST Validation needs to be done before hand
        payload = {
            "contact_name": contact_name,
            "company_name": company_name,
            "contact_type": contact_type,
            "currency_id": "3220178000000000099",
            "payment_terms": payment_terms,
            "payment_terms_label": payment_terms_label,
            "payment_terms_id": "",
            "credit_limit": 0,
            "billing_address": billing_address,
            "shipping_address": shipping_address,
            "contact_persons": [
                {
                    "first_name": contact_name,
                    "mobile": contact_mobile,
                    "phone": contact_mobile,
                    "email": contact_email,
                    "salutation": contact_salutation,
                    "is_primary_contact": True,
                }
            ],
            "default_templates": {},
            "custom_fields": [
                {
                    "customfield_id": "3220178000000075176",
                    "value": datetime.now().strftime("%Y-%m-%d"),
                },
                {"customfield_id": "3220178000000075170", "value": contact_email},
                {"customfield_id": "3220178000000075172", "value": contact_mobile},
                {"customfield_id": "3220178000000075174", "value": "55"},
                {"customfield_id": "3220178000000075188", "value": ""},
                {"customfield_id": "3220178000000075208", "value": "Retail"},
                {"customfield_id": "3220178000194368001", "value": "1"},
                {"customfield_id": "3220178000152684465", "value": ""},
                {"customfield_id": "3220178000171113003", "value": "no"},
                {"customfield_id": "3220178000196241001", "value": ["Pupscribe"]},
                {
                    "customfield_id": "3220178000000075214",
                    "value": payment_terms_label.upper(),
                },
                {"customfield_id": "3220178000000075212", "value": ["Inclusive"]},
                {
                    "customfield_id": "3220178000221198007",
                    "value": ["Company customers", "SP2"],
                },
                {"customfield_id": "3220178000241613643", "value": "No"},
            ],
            "is_taxable": True,
            "language_code": "en",
            "tags": [
                {"tag_id": "3220178000000000337", "tag_option_id": ""},
                {"tag_id": "3220178000000000333", "tag_option_id": ""},
                {"tag_id": "3220178000000000335", "tag_option_id": ""},
                {"tag_id": "3220178000000000339", "tag_option_id": ""},
            ],
            "gst_no": gst_number,
            "gst_treatment": gst_treatment,
            "place_of_contact": place_of_supply,
            "pan_no": pan,
            "customer_sub_type": contact_sub_type,
            "opening_balances": [
                {
                    "opening_balance_amount": "",
                    "exchange_rate": 1,
                    "location_id": "3220178000143298047",
                }
            ],
            "legal_name": company_name,
            "trader_name": company_name,
            "documents": [],
            "msme_type": "",
            "udyam_reg_no": "",
        }
        resp = requests.post(
            url=f"https://books.zoho.com/api/v3/contacts?organization_id={org_id}",
            headers=headers,
            json=payload,
        )
        contact = resp.json()["contact"]
        return contact


@router.get("/{customer_id}")
def get_customer(customer_id: str):
    print(customer_id)
    query = {"_id": ObjectId(customer_id)}
    print(query)
    customer = serialize_mongo_document(db.customers.find_one(query))
    return {"customer": customer}


@router.put("/{customer_id}")
async def update_customer(customer_id: str, product: dict):
    # Ensure '_id' is not in the update data
    update_data = {k: v for k, v in product.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # If 'cf_sales_person' is in the update data, ensure it's a comma-separated list
    if "cf_sales_person" in update_data:
        if isinstance(update_data["cf_sales_person"], str):
            update_data["cf_sales_person"] = [
                s.strip() for s in update_data["cf_sales_person"].split(",")
            ]

    # Perform the update
    result = db.customers.update_one(
        {"_id": ObjectId(customer_id)},
        {"$set": update_data},
    )

    # Prepare payload for Zoho API if 'cf_sales_person' is updated
    if "cf_sales_person" in update_data:
        customer = db.customers.find_one({"_id": ObjectId(customer_id)})
        payload = {
            "custom_fields": [
                {
                    "value": (
                        update_data["cf_sales_person"]
                        if update_data["cf_sales_person"][0] != ""
                        else []
                    ),
                    "customfield_id": "3220178000221198007",
                    "label": "Sales person",
                    "index": 11,
                }
            ]
        }
        x = requests.put(
            url=f"https://www.zohoapis.com/books/v3/contacts/{customer.get('contact_id')}?organization_id={org_id}",
            headers={"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"},
            json=payload,
        )
        print(payload)
        print(x.json()["message"])

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Customer not found")

    return {"message": "Customer updated"}
