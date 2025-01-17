from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import datetime, json

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()


# comment
def handle_estimate(data: dict):
    estimate = data.get("estimate")
    estimate_id = estimate.get("estimate_id")
    exists = serialize_mongo_document(
        db.estimates.find_one({"estimate_id": estimate_id})
    )
    if not exists:
        db.estimates.insert_one(
            {
                **estimate,
                "created_at": datetime.datetime.now(),
            }
        )
    else:
        print("Estimate Exists", json.dumps((exists), indent=4))
        print("New Estimate Data", json.dumps(data, indent=4))


def handle_customer(data: dict):
    contact = data.get("contact")
    contact_id = contact.get("contact_id")
    exists = serialize_mongo_document(db.customers.find_one({"contact_id": contact_id}))
    if not exists:
        db.customers.insert_one(
            {
                **customer,
                "created_at": datetime.datetime.now(),
            }
        )
    else:
        print("Customer Exists", json.dumps((exists), indent=4))
        print("New Customer Data", json.dumps(data, indent=4))


@router.post("/estimate")
def estimate(data: dict):
    print(data)
    handle_estimate(data)
    return "Estimate Webhook Received Successfully"


@router.post("/customer")
def customer(data: dict):
    print(data)
    handle_customer(data)
    return "Customer Webhook Received Successfully"
