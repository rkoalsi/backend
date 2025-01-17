from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import os, requests

load_dotenv()

org_id = os.getenv("ORG_ID")

router = APIRouter()

client, db = connect_to_mongo()


def handle_estimate(data: dict):
    estimate = data.get("estimate")
    estimate_id = estimate.get("estimate_id")
    exists = db.estimates.find_one({"estimate_id": estimate_id})
    if not exists:
        db.estimates.insert_one(customer)


def handle_customer(data: dict):
    customer = data.get("customer")
    customer_id = customer.get("customer_id")
    exists = db.customers.find_one({"contact_id": customer_id})
    if not exists:
        db.customers.insert_one(customer)


@router.post("/estimate")
def estimate(data: dict):
    handle_estimate(data)
    return "Estimate Webhook Received Successfully"


@router.post("/customer")
def customer(data: dict):
    handle_customer(data)
    return "Customer Webhook Received Successfully"
