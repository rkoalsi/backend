from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, parse_data  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import os, requests

load_dotenv()

CUSTOMER_URL = os.getenv("CUSTOMER_URL")
ITEMS_URL = os.getenv("ITEMS_URL")
org_id = os.getenv("ORG_ID")

router = APIRouter()

client, db = connect_to_mongo()


def clean_data(data):
    """
    Removes fields with empty string values from a dictionary.
    """
    return {key: value for key, value in data.items() if value != ""}


@router.get("/")
def home():
    return "DB Updation with Zoho Routes"


@router.get("/customers")
async def get_customers_from_zoho() -> str:
    try:
        access_token = get_access_token("books")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        updated_count = 0
        new_count = 0
        page = 1

        while True:
            response = requests.get(
                url=CUSTOMER_URL.format(page=page, org_id=org_id),
                headers=headers,
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail="Failed to fetch data from Zoho",
                )

            data = response.json()
            contacts = data.get("contacts", [])

            for contact in contacts:
                contact = clean_data(contact)
                existing_customer = db.customers.find_one(
                    {"contact_id": contact["contact_id"]}
                )

                if existing_customer:
                    # Compare only fields that are not empty and update if needed
                    updates = {
                        key: value
                        for key, value in contact.items()
                        if existing_customer.get(key) != value
                    }

                    if updates:  # Only update if there are actual changes
                        db.customers.update_one(
                            {"contact_id": contact["contact_id"]},
                            {"$set": updates},
                        )
                        updated_count += 1
                else:
                    # Add new customer
                    db.customers.insert_one(contact)
                    new_count += 1

            if not data["page_context"]["has_more_page"]:
                break
            page += 1

        return f"{new_count} new customers added, {updated_count} customers updated."

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products")
async def get_products_from_zoho() -> str:
    try:
        access_token = get_access_token("books")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        updated_count = 0
        new_count = 0
        page = 1

        while True:
            response = requests.get(
                url=ITEMS_URL.format(page=page, org_id=org_id),
                headers=headers,
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail="Failed to fetch item data from Zoho",
                )

            data = response.json()
            items = data.get("items", [])

            for item in items:
                item = clean_data(item)
                # Check if item exists by a unique field (e.g., "contact_id")
                existing_item = db.products.find_one({"item_id": item["item_id"]})

                if existing_item:
                    # Update only if there are changes
                    if (
                        existing_item != item
                    ):  # Compare entire document or specific fields
                        updates = {
                            key: value
                            for key, value in item.items()
                            if existing_item.get(key) != value
                        }
                        if updates:
                            db.products.update_one(
                                {"item_id": item["item_id"]},
                                {"$set": updates},
                            )
                            updated_count += 1
                else:
                    # Add new item
                    db.products.insert_one(item)
                    new_count += 1

            if not data["page_context"]["has_more_page"]:
                break
            page += 1

        return f"{new_count} new products added, {updated_count} products updated."

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
