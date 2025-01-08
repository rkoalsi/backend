from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, disconnect_on_exit, parse_data  # type: ignore
from .helpers import validate_file, process_upload, get_access_token
import re
from bson.objectid import ObjectId

router = APIRouter()

client, db = connect_to_mongo()

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


@router.get("")
def get_customers(name: str | None = None):
    customers = []
    query = {"status": "active"}

    if name:
        query["company_name"] = re.compile(name, re.IGNORECASE)

    customers = parse_data(db.customers.find(query))
    return {"customers": customers}


@router.get("/{customer_id}")
def get_customer(customer_id: str):
    print(customer_id)
    query = {"_id": ObjectId(customer_id)}
    print(query)
    customer = parse_data(db.customers.find_one(query))
    return {"customer": customer}


@router.get("/validate_gst")
def validate_gst(gst_in: str):
    return validate_gst_number(gst_in)


# @router.get("/", response_class=HTMLResponse)
# def index():
#     return "<h1>Backend is running<h1>"
