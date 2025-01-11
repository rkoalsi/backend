from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
import re, requests, os, json
from bson.objectid import ObjectId
from dotenv import load_dotenv
from .helpers import get_access_token
from PIL import Image
from io import BytesIO

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()
org_id = os.getenv("ORG_ID")
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

    return {
        "valid": True,
        "gst_number": gst_number,
        "pan": pan,
        "state_code": state_code,
        "state": state,
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


@router.get("/{customer_id}")
def get_customer(customer_id: str):
    print(customer_id)
    query = {"_id": ObjectId(customer_id)}
    print(query)
    customer = serialize_mongo_document(db.customers.find_one(query))
    return {"customer": customer}


@router.get("/validate_gst")
def validate_gst(gst_in: str):
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


@router.post("/upload/sign")
async def signature_upload(
    signature: UploadFile = File(...), customer_id: str = Form(...)
):
    try:
        print(f"Customer ID: {customer_id}")
        # Read the uploaded file
        content = await signature.read()
        image = Image.open(BytesIO(content))

        # Save the image as it is
        raw_file_path = os.path.join("./signatures", f"{customer_id}_sign.png")
        os.makedirs(
            os.path.dirname(raw_file_path), exist_ok=True
        )  # Ensure directory exists
        image.save(raw_file_path, format="PNG")

        # Check if the image has transparency
        if image.mode in ("RGBA", "LA") or (
            image.mode == "P" and "transparency" in image.info
        ):
            # Convert transparent background to white
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[3])  # Use alpha channel as mask
            image = background

        # Save the processed image
        processed_file_path = os.path.join(
            "./signatures", f"{customer_id}_sign_with_white_background.png"
        )
        image.save(processed_file_path, format="PNG")

        return JSONResponse(
            {
                "message": "Signature uploaded and saved locally",
                "file_path": processed_file_path,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# @router.get("/", response_class=HTMLResponse)
# def index():
#     return "<h1>Backend is running<h1>"
