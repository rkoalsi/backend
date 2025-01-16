from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Query
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
import re, requests, os, json, time
from bson.objectid import ObjectId
from dotenv import load_dotenv
from .helpers import get_access_token
from PIL import Image
from io import BytesIO
import boto3, traceback
from datetime import datetime

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()
org_id = os.getenv("ORG_ID")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
# Dictionary to map GST state codes to state names
GST_STATE_CODES = {
    "01": "JK",
    "02": "HP",
    "03": "PB",
    "04": "CH",
    "05": "UT",
    "06": "HR",
    "07": "DL",
    "08": "RJ",
    "09": "UP",
    "10": "BR",
    "11": "SK",
    "12": "AR",
    "13": "NL",
    "14": "MN",
    "15": "MZ",
    "16": "TR",
    "17": "ML",
    "18": "AS",
    "19": "WB",
    "20": "JH",
    "21": "OR",
    "22": "CT",
    "23": "MP",
    "24": "GJ",
    "25": "DD",
    "26": "DN",
    "27": "MH",
    "28": "AP",
    "29": "KA",
    "30": "GA",
    "31": "LD",
    "32": "KL",
    "33": "TN",
    "34": "PY",
    "35": "AN",
    "36": "TG",
    "37": "AP",
    "38": "LD",
}

STATE_CODES = {
    "Andhra Pradesh": "AP",
    "Arunachal Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BR",
    "Chhattisgarh": "CG",
    "Goa": "GA",
    "Gujarat": "GJ",
    "Haryana": "HR",
    "Himachal Pradesh": "HP",
    "Jharkhand": "JH",
    "Karnataka": "KA",
    "Kerala": "KL",
    "Madhya Pradesh": "MP",
    "Maharashtra": "MH",
    "Manipur": "MN",
    "Meghalaya": "ML",
    "Mizoram": "MZ",
    "Nagaland": "NL",
    "Odisha": "OD",
    "Punjab": "PB",
    "Rajasthan": "RJ",
    "Sikkim": "SK",
    "Tamil Nadu": "TN",
    "Telangana": "TG",
    "Tripura": "TR",
    "Uttar Pradesh": "UP",
    "Uttarakhand": "UK",
    "West Bengal": "WB",
    "Andaman and Nicobar Islands": "AN",
    "Chandigarh": "CH",
    "Dadra and Nagar Haveli and Daman and Diu": "DD",
    "Delhi": "DL",
    "Jammu and Kashmir": "JK",
    "Ladakh": "LA",
    "Lakshadweep": "LD",
    "Puducherry": "PY",
}


def validate_gst_number(gst_number: str):
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token("books")}"}
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
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token("books")}"}
    x = requests.post(
        f"https://www.zohoapis.com/books/v3/contacts/{customer.get("contact_id")}/address?organization_id={org_id}",
        headers=headers,
        json=address,
    )
    response = x.json()
    return response


@router.get("")
def get_customers(name: str | None = None):
    customers = []
    query = {"status": "active"}

    if name:
        query["company_name"] = re.compile(name, re.IGNORECASE)
    customers = [
        serialize_mongo_document(
            {
                **doc,
            }
        )
        for doc in db.customers.find(query)
    ]
    return {"customers": customers}


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


# payload = {
#     "contact_name": "Mr. Rohan Kalsi",
#     "company_name": "",
#     "contact_type": "customer",
#     "currency_id": "3220178000000000099",
#     "payment_terms": 0,
#     "payment_terms_label": "Due On Receipt",
#     "payment_terms_id": "",
#     "credit_limit": 0,
#     "billing_address": {
#         "attention": "Home",
#         "address": "Imperial Heights, Goregaon West",
#         "country": "India",
#         "street2": "",
#         "city": "Mumbai",
#         "state": "Maharashtra",
#         "zip": "400104",
#         "phone": "8104298709",
#     },
#     "shipping_address": {
#         "attention": "Home",
#         "address": "Imperial Heights, Goregaon West",
#         "country": "India",
#         "street2": "",
#         "city": "Mumbai",
#         "state": "Maharashtra",
#         "zip": "400104",
#         "phone": "8104298709",
#     },
#     "contact_persons": [
#         {
#             "first_name": "Rohan",
#             "last_name": "Kalsi",
#             "mobile": "08104298709",
#             "phone": "08104298709",
#             "email": "rkoalsi2000@gmail.com",
#             "salutation": "Mr.",
#             "is_primary_contact": True,
#         }
#     ],
#     "default_templates": {},
#     "custom_fields": [
#         {"customfield_id": "3220178000000075176", "value": "2025-01-15"},
#         {"customfield_id": "3220178000000075170", "value": "rkoalsi2000@gmail.com"},
#         {"customfield_id": "3220178000000075172", "value": "8104298709"},
#         {"customfield_id": "3220178000000075174", "value": "55"},
#         {"customfield_id": "3220178000000075188", "value": ""},
#         {"customfield_id": "3220178000000075208", "value": "Retail"},
#         {"customfield_id": "3220178000194368001", "value": "1"},
#         {"customfield_id": "3220178000152684465", "value": ""},
#         {"customfield_id": "3220178000171113003", "value": "no"},
#         {"customfield_id": "3220178000196241001", "value": ["Pupscribe"]},
#         {"customfield_id": "3220178000000075214", "value": "NET 30"},
#         {"customfield_id": "3220178000000075212", "value": ["Inclusive"]},
#         {
#             "customfield_id": "3220178000221198007",
#             "value": ["Company customers", "SP2"],
#         },
#         {"customfield_id": "3220178000241613643", "value": "No"},
#     ],
#     "is_taxable": True,
#     "language_code": "en",
#     "tags": [
#         {"tag_id": "3220178000000000337", "tag_option_id": ""},
#         {"tag_id": "3220178000000000333", "tag_option_id": ""},
#         {"tag_id": "3220178000000000335", "tag_option_id": ""},
#         {"tag_id": "3220178000000000339", "tag_option_id": ""},
#     ],
#     "gst_no": "27AAAAP0267H2ZN",
#     "gst_treatment": "business_gst",
#     "place_of_contact": "MH",
#     "pan_no": "AAAAP0267H",
#     "customer_sub_type": "business",
#     "opening_balances": [
#         {
#             "opening_balance_amount": "",
#             "exchange_rate": 1,
#             "location_id": "3220178000143298047",
#         }
#     ],
#     "legal_name": "GP PARSIK SAHAKARI BANK LIMITED",
#     "trader_name": "GP PARSIK SAHAKARI BANK LIMITED",
#     "documents": [],
#     "msme_type": "",
#     "udyam_reg_no": "",
# }


@router.post("")
def create_customer(customer: dict):
    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token("books")}"}
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
