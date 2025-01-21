from fastapi import APIRouter, BackgroundTasks
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import datetime, json, os, requests, asyncio

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()

now = datetime.datetime.utcnow()
TOTAL_WAREHOUSE_URL = os.getenv("TOTAL_WAREHOUSE_URL")
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
org_id = os.getenv("ORG_ID")
access_token = get_access_token("books")
headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
collection = db["products"]


def get_zoho_stock(day=now.day, month=now.month, year=now.year):
    print(f"Fetching stock for {now.replace(month=month).strftime('%b')}-{year}")
    to_date = now.replace(month=month, day=day).date()
    warehouse_stock = []
    z = requests.get(
        url=TOTAL_WAREHOUSE_URL.format(date1=to_date, org_id=org_id), headers=headers
    )
    total_pages = int(z.json().get("page_context", {}).get("total_pages", 0)) + 1

    for i in range(1, total_pages):
        y = requests.get(
            url=WAREHOUSE_URL.format(page=i, date1=to_date, org_id=org_id),
            headers=headers,
        )
        warehouse_stock.extend(y.json().get("warehouse_stock_info", []))

    arr = []
    for item in warehouse_stock:
        for w in item["warehouses"]:
            if w["warehouse_name"] == "Pupscribe Enterprises Private Limited":
                arr.append(
                    {
                        "name": item["item_name"],
                        "stock": int(w["quantity_available"]),
                    }
                )
    print("Got Stock")
    return arr


async def update_stock():
    """
    Update the stock field in active products based on their name (async).
    """
    active_products = collection.find({"status": "active"})
    stock_data = get_zoho_stock()  # synchronous call
    stock_dict = {item["name"]: item["stock"] for item in stock_data}

    updated_count = 0
    for product in active_products:
        product_name = product.get("name", "")
        # Check if product exists in stock data
        stock = stock_dict.get(product_name)
        if stock is not None:
            collection.update_one({"_id": product["_id"]}, {"$set": {"stock": stock}})
            updated_count += 1
            print(f"Updated product '{product_name}' with stock: {stock}")
        else:
            print(f"No stock data for product '{product_name}'")

    print(f"Total products updated with stock: {updated_count}")
    return updated_count


# Wrapper function to call async code in a background task
def run_update_stock():
    """
    Runs the async `update_stock` inside a sync function
    so it can be scheduled as a background task in FastAPI.
    """
    asyncio.run(update_stock())


@router.post("/update/stock")
def update_stock_webhook(data: dict, background_tasks: BackgroundTasks):
    """
    Receives a webhook to update product stock from Zoho in the background.
    Returns immediately while the update runs.
    """
    print("Webhook data:", json.dumps(data, indent=4, default=str))
    # Schedule the stock update to run in the background
    background_tasks.add_task(run_update_stock)

    return {"message": "Stock update has been scheduled in the background."}


def handle_estimate(data: dict):
    estimate = data.get("estimate")
    print("Estimate", json.dumps(estimate, indent=4, default=str))  # <-- default=str
    estimate_id = estimate.get("estimate_id", "")
    if estimate_id != "":
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
            print("Estimate Exists", json.dumps(exists, indent=4, default=str))
            print("New Estimate Data", json.dumps(data, indent=4, default=str))
    else:
        print("Estimate Does Not Exist. Webhook Received")


UNWANTED_KEYS = [
    "is_associated_to_branch",
    "is_bcy_only_contact",
    "is_credit_limit_migration_completed",
    "language_code",
    "language_code_formatted",
    "is_client_review_asked",
    "documents",
    "is_crm_customer",
    "is_linked_with_zohocrm",
    "price_precision",
    "exchange_rate",
    "can_show_customer_ob",
    "opening_balance_amount",
    "outstanding_ob_receivable_amount",
    "outstanding_ob_payable_amount",
    "outstanding_receivable_amount",
    "outstanding_receivable_amount_bcy",
    "outstanding_payable_amount",
    "outstanding_payable_amount_bcy",
    "unused_credits_receivable_amount",
    "unused_credits_receivable_amount_bcy",
    "unused_credits_payable_amount",
    "unused_credits_payable_amount_bcy",
    "unused_retainer_payments",
    "payment_reminder_enabled",
    "is_sms_enabled",
    "is_consent_agreed",
    "is_client_review_settings_enabled",
    "approvers_list",
    "integration_references",
    "allow_parent_for_payment_and_view",
    "ach_supported",
    "cards",
    "checks",
    "bank_accounts",
    "vpa_list",
    "last_modified_time",
    "default_templates",
    "custom_field_hash",
    "source",
    "portal_status",
    "owner_id",
    "msme_type",
    "consent_date",
    "source_formatted",
    "submitted_by_email",
    "submitted_by",
    "source",
    "invited_by",
    "outstanding_receivable_amount_formatted",
    "twitter",
    "unused_credits_receivable_amount_formatted",
    "zcrm_contact_id",
    "unused_credits_receivable_amount_bcy_formatted",
    "outstanding_payable_amount_formatted",
    "pricebook_id",
    "approver_id",
    "submitted_date_formatted",
    "opening_balance_amount_bcy_formatted",
    "tags",
    "unused_credits_payable_amount_formatted",
    "outstanding_receivable_amount_bcy_formatted",
    "crm_owner_id",
    "msme_type_formatted",
    "facebook",
    "unused_retainer_payments_formatted",
    "owner_name",
    "tax_reg_label",
    "vat_reg_no",
    "credit_limit_exceeded_amount_formatted",
    "pricebook_name",
    "submitted_by_name",
    "zohopeople_client_id",
    "submitted_by",
    "submitter_id",
    "udyam_reg_no",
    "tds_tax_id",
]


def handle_customer(data: dict):
    contact = data.get("contact")
    contact_id = contact.get("contact_id")

    # Check if the customer already exists in the database
    existing_customer = serialize_mongo_document(
        db.customers.find_one({"contact_id": contact_id})
    )

    def is_address_present(address, existing_addresses):
        # Check if the address is already present in the existing addresses
        return any(
            all(address.get(key) == existing_addr.get(key) for key in address.keys())
            for existing_addr in existing_addresses
        )

    def clean_data(document):
        # Remove unwanted keys from the document
        for key in UNWANTED_KEYS:
            document.pop(key, None)
        return document

    # Clean contact data
    contact = clean_data(contact)

    if not existing_customer:
        # Insert the new customer with addresses
        addresses = []

        # Add billing_address to addresses if it exists
        if "billing_address" in contact and contact["billing_address"]:
            addresses.append(contact["billing_address"])

        # Add shipping_address to addresses if it exists and is not a duplicate of billing_address
        if "shipping_address" in contact and contact["shipping_address"]:
            if not is_address_present(contact["shipping_address"], addresses):
                addresses.append(contact["shipping_address"])

        # Remove billing_address and shipping_address from contact
        contact.pop("billing_address", None)
        contact.pop("shipping_address", None)

        db.customers.insert_one(
            {
                **contact,
                "addresses": addresses,
                "created_at": datetime.datetime.now(),
                "updated_at": datetime.datetime.now(),
            }
        )
        print("New customer inserted.")
    else:
        print("Customer exists. Checking for updates...")

        # Prepare the update document
        update_fields = {}

        # Update individual fields if they have changed
        for key, value in contact.items():
            if (
                key not in ["billing_address", "shipping_address", "addresses"]
                and existing_customer.get(key) != value
            ):
                update_fields[key] = value

        # Handle addresses
        existing_addresses = existing_customer.get("addresses", [])
        new_addresses = []

        # Add billing_address to new_addresses if it doesn't already exist
        if "billing_address" in contact and contact["billing_address"]:
            if not is_address_present(contact["billing_address"], existing_addresses):
                new_addresses.append(contact["billing_address"])

        # Add shipping_address to new_addresses if it doesn't already exist
        if "shipping_address" in contact and contact["shipping_address"]:
            if not is_address_present(contact["shipping_address"], existing_addresses):
                new_addresses.append(contact["shipping_address"])

        # Add new addresses to the update
        if new_addresses:
            update_fields["addresses"] = existing_addresses + new_addresses

        # Remove billing_address and shipping_address from contact
        update_fields.pop("billing_address", None)
        update_fields.pop("shipping_address", None)

        # Update the customer if there are changes
        if update_fields:
            update_fields["updated_at"] = datetime.datetime.now()
            db.customers.update_one(
                {"contact_id": contact_id},
                {"$set": update_fields, "$unset": {key: "" for key in UNWANTED_KEYS}},
            )
            # Convert datetime to string for JSON serialization
            update_fields_serialized = {
                key: (
                    value.isoformat() if isinstance(value, datetime.datetime) else value
                )
                for key, value in update_fields.items()
            }
            print("Customer updated:", json.dumps(update_fields_serialized, indent=4))
        else:
            print("No updates required for the customer.")


@router.post("/estimate")
def estimate(data: dict):
    print(json.dumps(data, indent=4))
    handle_estimate(data)
    return "Estimate Webhook Received Successfully"


@router.post("/customer")
def customer(data: dict):
    print(json.dumps(data, indent=4))
    handle_customer(data)
    return "Customer Webhook Received Successfully"
