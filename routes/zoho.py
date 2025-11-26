from fastapi import APIRouter, HTTPException
from config.root import get_database
from .helpers import get_access_token
from dotenv import load_dotenv
import os, requests

load_dotenv()

CUSTOM_FIELDS_URL = os.getenv("CUSTOM_FIELDS_URL")
CUSTOMERS_URL = os.getenv("CUSTOMERS_URL")
CUSTOMER_URL = os.getenv("CUSTOMER_URL")
ITEMS_URL = os.getenv("ITEMS_URL")
org_id = os.getenv("ORG_ID")

router = APIRouter()

db = get_database()


def clean_data(data):
    """
    Removes fields with empty string values from a dictionary.
    """
    return {key: value for key, value in data.items() if value != ""}


@router.get("/")
def home():
    return "DB Updation with Zoho Routes"


@router.get("/custom_fields")
async def custom_fields():
    try:
        access_token = get_access_token("books")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        x = requests.get(url=CUSTOM_FIELDS_URL.format(org_id=org_id), headers=headers)
        custom_fields = x.json()["custom_fields"]
        response = []
        for i in custom_fields:
            data = {
                "customfield_id": i.get("customfield_id"),
                "name": i.get("label"),
                "data_type": i.get("data_type"),
                "value": (
                    i.get("values")
                    if i.get("default_value") == "[]"
                    else (
                        i.get("help_text")
                        if i.get("help_text") != ""
                        else i.get("value")
                    )
                ),
            }
            response.append(data)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customers")
async def get_customers_from_zoho() -> str:
    try:
        access_token = get_access_token("books")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        updated_count = 0
        new_count = 0

        # Step 1: Fetch all pages of customers using has_more_page
        def fetch_customers_page(page):
            response = requests.get(
                url=CUSTOMERS_URL.format(page=page, org_id=org_id),
                headers=headers,
            )
            print(response.json())
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to fetch data from Zoho (page {page}): {response.text}",
                )
            return response.json()

        all_contacts = []
        page = 1
        while True:
            data = fetch_customers_page(page)
            all_contacts.extend(data.get("contacts", []))
            if not data["page_context"]["has_more_page"]:
                break
            page += 1

        # Step 2: Fetch additional data for each contact sequentially
        for contact in all_contacts:
            contact_id = contact["contact_id"]
            additional_response = requests.get(
                url=CUSTOMER_URL.format(customer_id=contact_id, org_id=org_id),
                headers=headers,
            )
            if additional_response.status_code != 200:
                raise HTTPException(
                    status_code=additional_response.status_code,
                    detail=f"Failed to fetch additional data for contact_id {contact_id}: {additional_response.text}",
                )

            populated_contact = clean_data(additional_response.json()["contact"])
            existing_customer = db.customers.find_one({"contact_id": contact_id})

            if existing_customer:
                # Compare only fields that are not empty and update if needed
                updates = {
                    key: value
                    for key, value in populated_contact.items()
                    if existing_customer.get(key) != value
                }

                if updates:  # Only update if there are actual changes
                    db.customers.update_one(
                        {"contact_id": contact_id},
                        {"$set": updates},
                    )
                    updated_count += 1
            else:
                # Add new customer
                db.customers.insert_one(populated_contact)
                new_count += 1

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
