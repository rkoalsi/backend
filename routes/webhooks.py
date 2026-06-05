from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from ..config.root import get_database, serialize_mongo_document
from ..config.scheduler import schedule_job, remove_scheduled_jobs
from ..config.whatsapp import send_whatsapp
from .helpers import get_access_token
from .notifications import create_notification
from dotenv import load_dotenv
import datetime, json, os, requests, time, threading
from dateutil.parser import parse
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from .helpers import send_email
from typing import Dict, Any, List
from bson import ObjectId
from collections import defaultdict
from calendar import monthrange
from pathlib import Path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()

router = APIRouter()

db = get_database()

now = datetime.datetime.utcnow()
TOTAL_WAREHOUSE_URL = os.getenv("TOTAL_WAREHOUSE_URL")
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
org_id = os.getenv("ORG_ID")
collection = db["products"]

_access_token_cache = {"token": None, "expires_at": None}

update_stock_lock = threading.Lock()

# Google Sheets setup for tpack sync
BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT_FILE = BASE_DIR / "creds.json"
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
TPACK_SHEET_ID = "1gz50Djg0OYH6bGOTf2iXmktylLOhybP2vMtZQJisuHo"
TPACK_API_URL = "https://tpack.bubbleapps.io/version-live/api/1.1/wf/pupscribe-zoho-data"
TPACK_HEADERS = {"Authorization":f"Bearer {os.getenv('TPACK_TOKEN')}", "Content-Type": "application/json"}

_sheets_service_webhooks = None

def get_sheets_service_webhooks():
    """Get or create Google Sheets service for webhooks"""
    global _sheets_service_webhooks
    if _sheets_service_webhooks is None:
        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SHEETS_SCOPES
        )
        _sheets_service_webhooks = build("sheets", "v4", credentials=credentials)
    return _sheets_service_webhooks


def sync_stock_to_tpack():
    """
    Fetch SKU codes from the Google Sheet, look them up in the products collection,
    and send item_name, cf_sku_code, and stock (Pupscribe Warehouse) to tpack API.
    """
    try:
        print("Starting tpack stock sync...")

        # Get SKU codes from Google Sheet
        sheets_service = get_sheets_service_webhooks()
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=TPACK_SHEET_ID,
            range="A:Z"  # Get all columns to find the SKU Code (Final) column
        ).execute()

        values = result.get("values", [])
        if not values:
            print("No data found in the Google Sheet.")
            return

        # Find the "SKU Code (Final)" column index
        header_row = values[0]
        sku_col_index = None
        for i, header in enumerate(header_row):
            if header and "SKU Code (Final)" in header:
                sku_col_index = i
                break

        if sku_col_index is None:
            print("Could not find 'SKU Code (Final)' column in the sheet.")
            return

        # Extract SKU codes from the sheet (skip header row)
        sku_codes = []
        for row in values[1:]:
            if len(row) > sku_col_index and row[sku_col_index]:
                sku_codes.append(row[sku_col_index].strip())

        print(f"Found {len(sku_codes)} SKU codes in the sheet.")

        if not sku_codes:
            print("No SKU codes found in the sheet.")
            return

        # Look up products by cf_sku_code in the database
        products = list(collection.find(
            {"cf_sku_code": {"$in": sku_codes}},
            {"item_name": 1, "cf_sku_code": 1, "stock": 1, "_id": 0}
        ))

        print(f"Found {len(products)} matching products in the database.")

        if not products:
            print("No matching products found in the database.")
            return

        # Prepare data for API
        stock_data: List[Dict[str, Any]] = []
        for product in products:
            stock_data.append({
                "item_name": product.get("item_name", ""),
                "cf_sku_code": product.get("cf_sku_code", ""),
                "stock": product.get("stock", 0)
            })

        # Send to tpack API
        print(f"Sending {len(stock_data)} products to tpack API...")
        response = requests.post(
            TPACK_API_URL,
            json={"products": stock_data},
            headers=TPACK_HEADERS,
            timeout=60
        )
        print(f"{response.json()}")
        if response.status_code == 200:
            print(f"Successfully sent stock data to tpack API. Response: {response.text[:200]}")
        else:
            print(f"Failed to send stock data to tpack API. Status: {response.status_code}, Response: {response.text[:500]}")

    except Exception as e:
        print(f"Error in sync_stock_to_tpack: {e}")


def parse_datetime(value):
    """
    Parses a datetime string into a datetime object.
    If the value is already a datetime object, returns it as is.
    If parsing fails, returns the current datetime.
    """
    if isinstance(value, datetime.datetime):
        return value
    try:
        return parse(value)
    except (ValueError, TypeError):
        return datetime.datetime.now()



def create_special_margins_for_new_product(
    product_id: str, product_name: str, brand_name: str
):
    """
    Background task to create special margins for a new product based on existing brand margins.
    Queries by the stored brand field directly instead of inferring brand from product names.
    Uses the most common (mode) margin for that brand per customer as the tiebreaker.
    """
    try:
        print(
            f"Creating special margins for new product: {product_name} (Brand: {brand_name})"
        )

        # Find all customers that already have margins for this brand using the brand field.
        existing_brand_margins = list(
            db.special_margins.find(
                {"brand": brand_name},
                {"customer_id": 1, "margin": 1}
            )
        )

        if not existing_brand_margins:
            print(f"No customers have margins for brand '{brand_name}', nothing to do")
            return

        # Group margins by customer and pick the mode (most common) margin per customer.
        customer_margins = defaultdict(list)
        for doc in existing_brand_margins:
            cid = doc.get("customer_id")
            margin = doc.get("margin")
            if cid and margin is not None:
                customer_margins[cid].append(margin)

        print(
            f"Found {len(customer_margins)} customers with margins for brand '{brand_name}'"
        )

        product_obj_id = ObjectId(product_id)
        now = datetime.datetime.now()
        special_margins_to_insert = []

        for customer_id, margins in customer_margins.items():
            # Pick the most common margin for this brand; first alphabetically on tie.
            margin = max(set(margins), key=margins.count)

            existing_special_margin = db.special_margins.find_one(
                {"customer_id": customer_id, "product_id": product_obj_id}
            )

            if not existing_special_margin:
                special_margins_to_insert.append(
                    {
                        "customer_id": customer_id,
                        "product_id": product_obj_id,
                        "margin": margin,
                        "name": product_name,
                        "brand": brand_name,
                        "created_at": now,
                        "updated_at": now,
                    }
                )
                print(
                    f"Will create special margin for customer {customer_id}: {product_name} @ {margin}"
                )

        if special_margins_to_insert:
            result = db.special_margins.insert_many(special_margins_to_insert)
            print(
                f"Successfully created {len(result.inserted_ids)} special margins for new product"
            )
        else:
            print("No new special margins needed for this product")

    except Exception as e:
        print(f"Error creating special margins for new product: {e}")


def handle_item(data: dict, background_tasks: BackgroundTasks):
    item = data.get("item")
    item_id = item.get("item_id", "")
    if item_id != "":
        exists = serialize_mongo_document(db.products.find_one({"item_id": item_id}))
        if not exists:
            item_name = str(item.get("name"))
            brand_name = str(item.get("brand"))
            result = db.products.insert_one(
                {
                    "item_id": item.get("item_id", ""),
                    "name": item.get("name", ""),
                    "item_name": item_name,
                    "unit": item.get("unit", "pcs"),
                    "brand": brand_name,
                    "status": item.get("status", "active"),
                    "is_combo_product": item.get("is_combo_product", False),
                    "rate": item.get("rate", 1),
                    "item_tax_preferences": item.get("item_tax_preferences", []),
                    "account_name": item.get("account_name", ""),
                    "purchase_rate": item.get("purchase_rate", 0),
                    "item_type": item.get("item_type", "sales"),
                    "product_type": item.get("product_type", "goods"),
                    "is_taxable": item.get("is_taxable", True),
                    "track_batch_number": item.get("track_batch_number", False),
                    "hsn_or_sac": item.get("hsn_or_sac", ""),
                    "sku": item.get("sku", ""),
                    "upc_code": item.get("upc", ""),
                    "manufacturer": item.get("manufacturer", ""),
                    "cf_item_code": item.get("custom_field_hash", {}).get(
                        "cf_item_code", ""
                    ),
                    "cf_sku_code": item.get("custom_field_hash", {}).get(
                        "cf_sku_code", ""
                    ),
                    "series": item.get("custom_field_hash", {}).get("cf_series", ""),
                    "category": item.get("custom_field_hash", {}).get(
                        "cf_category", ""
                    ),
                    "sub_category": item.get("custom_field_hash", {}).get(
                        "cf_sub_category", ""
                    ),
                    "created_at": parse_datetime(item.get("created_time")),
                    "updated_at": parse_datetime(item.get("last_modified_time")),
                }
            )
            new_product_id = str(result.inserted_id)
            background_tasks.add_task(
                create_special_margins_for_new_product,
                new_product_id,
                item_name,
                brand_name,
            )
            template = serialize_mongo_document(
                dict(db.templates.find_one({"name": "item_creation_update"}))
            )
            to_notify = [
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC1_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC1"),
                },
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC3_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC3"),
                },
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC4_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC4"),
                },
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC5_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC5"),
                },
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC6_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC6"),
                },
                {
                    "name": os.getenv("NOTIFY_NUMBER_TO_CC7_NAME"),
                    "phone": os.getenv("NOTIFY_NUMBER_TO_CC7"),
                },
            ]
            for person in to_notify:
                params = {
                    "name": person["name"],
                    "item_name": item.get("name", ""),
                    "brand": brand_name,
                }
                send_whatsapp(
                    to=person["phone"],
                    template_doc=template,
                    params=params,
                )
                if person.get("phone"):
                    user_doc = db.users.find_one({"phone": person["phone"]}, {"_id": 1})
                    if user_doc:
                        create_notification(
                            db,
                            str(user_doc["_id"]),
                            "new_product",
                            f"New product: {item.get('name', '')}",
                            f"{brand_name} — {item.get('name', '')} has been added.",
                            "/admin/products",
                        )

            background_tasks.add_task(run_update_stock)
        else:
            print("Item Exists")
            update_data = {}

            # Handle 'created_time' and 'last_modified_time' separately
            if "created_time" in item:
                parsed_created = parse_datetime(item.get("created_time"))
                update_data["created_at"] = parsed_created
                print(
                    f"Parsed created_at: {parsed_created} (Type: {type(parsed_created)})"
                )

            if "last_modified_time" in item:
                parsed_updated = parse_datetime(item.get("last_modified_time"))
                update_data["updated_at"] = parsed_updated
                print(
                    f"Parsed updated_at: {parsed_updated} (Type: {type(parsed_updated)})"
                )
            # if "brand" in item:
            #     item_name = str(item.get("name"))
            #     brand_name = item_name.split(" ", 1)[0]

            #     update_data["brand"] = brands.get(
            #         brand_name.lower(), brand_name.capitalize()
            #     )

            if "custom_field_hash" in item:
                update_data["cf_sku_code"] = item.get("custom_field_hash", {}).get(
                    "cf_sku_code", ""
                )
                update_data["cf_item_code"] = item.get("custom_field_hash", {}).get(
                    "cf_item_code", ""
                )
                update_data["series"] = item.get("custom_field_hash", {}).get(
                    "cf_series", ""
                )
                update_data["category"] = item.get("custom_field_hash", {}).get(
                    "cf_category", ""
                )
                update_data["sub_category"] = item.get("custom_field_hash", {}).get(
                    "cf_sub_category", ""
                )
            if "manufacturer" in item:
                update_data["manufacturer"] = item.get("manufacturer", "")
            if "status" in item:
                update_data["status"] = item.get("status", "")
            if "upc" in item:
                update_data["upc_code"] = item.get("upc", "")
            # Iterate over other fields to detect changes
            for field, value in item.items():
                # Exclude 'status', 'created_time', 'last_modified_time', and 'created_at' from updates
                if field in [
                    # "status",
                    "created_time",
                    "last_modified_time",
                    "created_at",
                    # "brand",
                ]:
                    continue

                # Check if the field exists in the document and if its value has changed
                if field in exists and exists[field] != value:
                    update_data[field] = value
                elif field == "item_tax_preferences" and field not in exists:
                    update_data[field] = value

            # If there are fields to update, perform the update
            if update_data:
                try:
                    db.products.update_one({"item_id": item_id}, {"$set": update_data})
                    print(
                        "Updated Fields:",
                        json.dumps(update_data, indent=4, default=str),
                    )
                except Exception as e:
                    print(f"Error updating document with item_id {item_id}: {e}")
            else:
                print("No fields to update.")
    else:
        print("Item Does Not Exist. Webhook Received")


def get_cached_access_token():
    """
    Get or refresh the Zoho access token with caching to improve performance.
    """
    global _access_token_cache
    if (
        not _access_token_cache["token"]
        or _access_token_cache["expires_at"] < datetime.datetime.utcnow()
    ):
        access_token = get_access_token("inventory")
        print("Access token obtained:", access_token)
        _access_token_cache["token"] = access_token
        _access_token_cache["expires_at"] = (
            datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        )
        print("Access token refreshed.")
    else:
        print("Using cached access token.")
    return _access_token_cache["token"]


def fetch_with_retries(url, headers, retries=3, timeout=10, page_number=None):
    """
    Fetch data from a URL with retry logic and timeout.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()  # Raise an exception for HTTP errors
            print(f"Page {page_number}: Successfully fetched.")
            return response
        except requests.RequestException as e:
            if attempt < retries:
                wait_time = 2**attempt
                print(
                    f"Page {page_number}: Attempt {attempt} failed. Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)  # Exponential backoff
            else:
                print(
                    f"Page {page_number}: Failed after {retries} attempts. Error: {e}"
                )
                return None


def get_zoho_stock(day=None, month=None, year=None, col_name="zoho Stock"):
    """
    Unified function that fetches stock data from Zoho Inventory with proper error handling
    and supports both API response structures.
    """
    # Set the date
    if day and month and year:
        try:
            now_date = datetime.datetime(year, month, day)
        except ValueError as e:
            print(f"Invalid date provided: {e}")
            return []
    else:
        # If no complete date provided, use current date logic
        _now = datetime.datetime.now()
        if month is None:
            month = _now.month
        if year is None:
            year = _now.year

        if day is None:
            if month == _now.month and year == _now.year:
                # Current month - use current day
                day = _now.day
            else:
                # Previous month - use last day of that month
                day = monthrange(year, month)[1]

        now_date = datetime.datetime(year, month, day)

    to_date = now_date.date()
    sheet_name = f'{now_date.strftime("%b")} {year}'
    print(f"Fetching stock for {now_date.strftime('%b')}-{year} with date {to_date}")

    _PUPSCRIBE_LOCATION_ID = "3220178000000403010"
    access_token = get_access_token('inventory')
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    # Use inventorysummary with location_id — returns WH-specific quantity_available_for_sale
    # (the warehouse report API stopped populating warehouse_name in sub-entries as of 2026-05)
    arr = []
    page = 1
    while True:
        inv_url = (
            f"https://inventory.zoho.com/api/v1/reports/inventorysummary"
            f"?page={page}&per_page=200&filter_by=TransactionDate.CustomDate"
            f"&show_actual_stock=false&to_date={to_date}"
            f"&organization_id={org_id}&location_id={_PUPSCRIBE_LOCATION_ID}"
        )
        try:
            resp = requests.get(inv_url, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"inventorysummary page {page} failed: {e}")
            break

        if not data.get("inventory"):
            break

        for item in data["inventory"][0].get("item_details", []):
            item_name = (item.get("item_name") or "").strip().lower()
            if not item_name:
                continue
            try:
                stock_quantity = int(item.get("quantity_available_for_sale") or 0)
            except (ValueError, TypeError):
                print(f"Invalid stock quantity for '{item_name}': {item.get('quantity_available_for_sale')}")
                continue
            arr.append({"name": item_name, "stock": stock_quantity})

        print(f"inventorysummary page {page}: {len(arr)} items so far")
        if not data.get("page_context", {}).get("has_more_page"):
            break
        page += 1

    print(f"Total stock items (Pupscribe WH): {len(arr)}")
    return arr


def update_stock():
    """
    Update the stock field in active products based on their name (synchronous).
    """
    # Fetch active products
    try:
        active_products = list(collection.find({}, {"_id": 1, "name": 1}))
        print(f"Fetched {len(active_products)} active products from the database.")
    except Exception as e:
        print(f"Failed to fetch active products from the database: {e}")
        return 0

    # Fetch stock data from Zoho
    stock_data = get_zoho_stock()
    if not stock_data:
        print("No stock data fetched from Zoho.")
        return 0

    stock_dict = {item["name"]: item["stock"] for item in stock_data}
    print(f"Stock data contains {len(stock_dict)} items.")

    # Prepare bulk updates
    updates = []
    not_found_products = []
    for product in active_products:
        product_name = product.get("name", "").strip().lower()
        stock = stock_dict.get(product_name)
        if stock is not None:
            updates.append(
                UpdateOne(
                    {"_id": product["_id"]},
                    {"$set": {"stock": stock}},
                )
            )
        else:
            # Track products not found for better debugging
            not_found_products.append(product_name)
            print(f"No stock data for product '{product_name}'")

    # Debug: Show potential matches for products not found
    if not_found_products:
        print(f"\n=== DEBUG: {len(not_found_products)} products not found in stock data ===")
        for product_name in not_found_products[:10]:  # Show first 10
            # Find similar names in stock_dict (for debugging)
            similar = [k for k in stock_dict.keys() if product_name[:20] in k or k[:20] in product_name]
            if similar:
                print(f"Product: '{product_name}'")
                print(f"  Similar in stock: {similar[:3]}")
            else:
                print(f"Product: '{product_name}' - No similar matches")

    # Execute bulk updates
    updated_count = 0
    if updates:
        try:
            result = collection.bulk_write(updates)
            updated_count = result.modified_count
            print(f"Total products updated with stock: {updated_count}")
        except Exception as e:
            print(f"Failed to execute bulk updates: {e}")
    else:
        print("No updates required.")

    # Sync stock data to tpack API (run after DB update, before notifications)
    try:
        sync_stock_to_tpack()
    except Exception as e:
        print(f"Error syncing stock to tpack: {e}")

    # Check for in-stock notification requests
    try:
        # Build a map of product_id -> stock for products that are now in stock
        in_stock_product_ids = set()
        for product in active_products:
            product_name = product.get("name", "").strip().lower()
            stock = stock_dict.get(product_name)
            if stock is not None and stock > 0:
                in_stock_product_ids.add(product["_id"])

        if not in_stock_product_ids:
            print("No products currently in stock, skipping notification check.")
            return updated_count

        # Find pending notify requests for products that are now in stock
        pending_requests = list(db["product_notify_requests"].find({
            "product_id": {"$in": list(in_stock_product_ids)},
            "notified": False,
        }))

        if not pending_requests:
            print("No pending stock notification requests.")
            return updated_count

        print(f"Found {len(pending_requests)} pending stock notification requests.")

        template_doc = db.templates.find_one({"name": "in_stock_notification"})
        if not template_doc:
            print("WhatsApp template 'in_stock_notification' not found. Skipping notifications.")
            return updated_count

        notified_ids = []
        for req in pending_requests:
            try:
                order = db.orders.find_one({"_id": req.get("order_id")})
                if not order:
                    print(f"Order not found for notify request {req['_id']}")
                    continue

                salesperson = db.users.find_one({"_id": ObjectId(order.get("created_by", ""))})
                if not salesperson or not salesperson.get("phone"):
                    print(f"Salesperson or phone not found for order {order['_id']}")
                    continue

                params = {
                    "salesperson_name": salesperson.get("name", ""),
                    "customer_name": req.get("customer_name", ""),
                    "product_name": req.get("product_name", ""),
                    "product_brand": req.get("product_brand", ""),
                }

                send_whatsapp(
                    to=salesperson["phone"],
                    template_doc=serialize_mongo_document(dict(template_doc)),
                    params=params,
                )
                create_notification(
                    db,
                    str(salesperson["_id"]),
                    "product_back_in_stock",
                    f"{req.get('product_name', 'Product')} is back in stock",
                    f"{req.get('product_brand', '')} {req.get('product_name', '')} is now available for {req.get('customer_name', '')}.",
                    f"/orders/past/{str(req.get('order_id', ''))}",
                )
                notified_ids.append(req["_id"])
                print(f"Sent in-stock notification to {salesperson.get('name')} for {req.get('product_name')}")

            except Exception as e:
                print(f"Error sending notification for request {req['_id']}: {e}")

        # Mark notified requests
        if notified_ids:
            db["product_notify_requests"].update_many(
                {"_id": {"$in": notified_ids}},
                {"$set": {"notified": True, "notified_at": datetime.datetime.now()}},
            )
            print(f"Marked {len(notified_ids)} notification requests as notified.")

    except Exception as e:
        print(f"Error processing stock notifications: {e}")

    return updated_count


def run_update_stock():
    """
    Runs the `update_stock` function.
    """
    if update_stock_lock.locked():
        print("Update stock is already running. Skipping new call.")
        return
    with update_stock_lock:
        try:
            update_stock()
        except Exception as e:
            print(f"Error running update_stock: {e}")


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


FORBIDDEN_KEYWORDS = [
    "Company customers",
    "defaulters",
    "Amazon",
    "staff purchase",
    "marketing inv's",
    "End Client",
    "Company clients (all rohit's customers & distributors))",
]


def is_forbidden(name: str) -> bool:
    """
    Returns True if the name contains any forbidden keyword (case-insensitive).
    """
    lowered = name.lower()
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword.lower() in lowered:
            return True
    return False


def handle_invoice(data: dict):
    invoice = data.get("invoice")
    invoice_id = invoice.get("invoice_id", "")
    invoice_status = invoice.get("status", "")
    invoice_sales_person = invoice.get("cf_sales_person", "")
    salesperson = invoice.get("salesperson_name", "")
    created_at = invoice.get("date")
    invoice_due_date_str = invoice.get("due_date")
    customer_name = invoice.get("customer_name")
    total = invoice.get("total")
    balance = invoice.get("balance")
    invoice_number = invoice.get("invoice_number")
    due_date = datetime.datetime.strptime(invoice_due_date_str, "%Y-%m-%d")
    if invoice_id != "":
        exists = serialize_mongo_document(
            db.invoices.find_one({"invoice_id": invoice_id})
        )
        if not exists:
            db.invoices.insert_one(
                {
                    **invoice,
                    "created_at": datetime.datetime.now(),
                }
            )
        else:
            print("Invoice Exists")
            db.invoices.update_one(
                {"invoice_id": invoice_id},
                {"$set": {**invoice, "updated_at": datetime.datetime.now()}},
            )
            print("New Invoice Data Updated")
        if invoice_status == "paid":
            print(
                f"Invoice {invoice_id} is marked as 'paid'. Removing all scheduled jobs."
            )
            remove_scheduled_jobs(invoice_id)
            return
        # 1) Gather and de-duplicate salespeople from both fields
        all_salespeople = set()
        print("Custom Field Invoice Sales Person", invoice_sales_person)
        print("Invoice Sales Person", salesperson)
        if invoice_sales_person:
            for name in invoice_sales_person.split(","):
                name = name.strip()
                if name:
                    all_salespeople.add(name)

        if salesperson:
            for name in salesperson.split(","):
                name = name.strip()
                if name:
                    all_salespeople.add(name)
        print("All Sales People:", all_salespeople)
        # 2) Filter out any forbidden names
        valid_salespeople = []
        if any(is_forbidden(sp.strip()) for sp in all_salespeople):
            # Check if due_date is today (date only, ignoring time)
            today = datetime.datetime.utcnow().date()
            due_date_only = due_date.date()
            if due_date_only == today and (
                invoice_status != "void" or invoice_status != "paid"
            ):
                msg_params = {
                    "to": os.getenv("OVERDUE_ADMIN_TO"),
                    "invoice_number": invoice.get("invoice_number", ""),
                    "created_at": invoice.get("date", ""),
                    "due_date": due_date.strftime("%Y-%m-%d"),
                    "customer_name": invoice.get("customer_name", ""),
                    "total": invoice.get("total", ""),
                    "balance": invoice.get("balance", ""),
                    "salesperson_name": os.getenv("OVERDUE_ADMIN_NAME"),
                    "invoice_id": invoice_id,
                }
                schedule_job(
                    msg_params,
                    run_date=datetime.datetime.now() + datetime.timedelta(minutes=1),
                    job_suffix="due_date",
                )
                msg_params["to"] = os.getenv("NOTIFY_NUMBER_TO_CC5")
                msg_params["salesperson_name"] = os.getenv("NOTIFY_NUMBER_TO_CC5_NAME")
                schedule_job(
                    msg_params,
                    run_date=datetime.datetime.now() + datetime.timedelta(minutes=1),
                    job_suffix="due_date",
                )
                print(
                    "At least one salesperson is forbidden. Scheduled admin notification email."
                )
            else:
                print(
                    "At least one salesperson is forbidden. Skipping admin notification email."
                )
            # Do not schedule emails for salespeople since at least one is forbidden
            return
        sales_admin = db.users.find_one({"email": "barksalesamit@gmail.com"})
        sales_admin_phone = sales_admin.get("phone")
        sales_admin_name = sales_admin.get("name")
        for sp in all_salespeople:
            user = db.users.find_one({"code": sp})
            if user:
                valid_salespeople.append(
                    {
                        "email": user.get("email", ""),
                        "name": user.get("name"),
                        "phone": user.get("phone"),
                    }
                )

        # 3) Schedule one job for each valid (unique) salesperson
        for sp in valid_salespeople:
            name = sp.get("name")
            email = sp.get("email")
            phone = sp.get("phone")
            if not phone:
                print(f"Phone does not exist for SP:{name}")
            msg_params = {
                "to": phone,
                "invoice_number": invoice_number,
                "created_at": created_at,
                "due_date": due_date.strftime("%Y-%m-%d"),
                "customer_name": customer_name,
                "total": total,
                "balance": balance,
                "salesperson_name": name,
                "invoice_id": invoice_id,
            }
            one_week_before = due_date - datetime.timedelta(weeks=1)
            if one_week_before > datetime.datetime.now():
                msg_params["type"] = "one_week_before"
                schedule_job(
                    msg_params,
                    run_date=one_week_before + datetime.timedelta(hours=10),
                    job_suffix="one_week_before",
                )
                msg_params["to"] = sales_admin_phone
                msg_params["salesperson_name"] = sales_admin_name
                schedule_job(
                    msg_params,
                    run_date=one_week_before + datetime.timedelta(hours=10),
                    job_suffix="one_week_before",
                )
                print(
                    f"Scheduled one-week-before email for invoice {invoice_number} to {email} at {one_week_before}."
                )
            else:
                print(
                    f"One week before due_date {one_week_before} is in the past. Skipping one-week-before email for invoice {invoice_number} to {name}."
                )

            # Schedule email on due_date
            current_dt = datetime.datetime.now()
            current_date = current_dt.date()
            if due_date.date() == current_date:
                # If due_date is today, execute now (or schedule immediately)
                msg_params["type"] = "due_date"
                schedule_job(
                    msg_params,
                    run_date=current_dt,  # execute immediately
                    job_suffix="due_date",
                )
                msg_params["to"] = sales_admin_phone
                msg_params["salesperson_name"] = sales_admin_name
                schedule_job(
                    msg_params,
                    run_date=current_dt,  # execute immediately
                    job_suffix="due_date",
                )
                print(
                    f"Scheduled due-date email for invoice {invoice_number} to {email} to run immediately since due_date {due_date} is today."
                )
            elif due_date > current_dt:
                # due_date is in the future (and not today), schedule as before
                msg_params["type"] = "due_date"
                schedule_job(
                    msg_params,
                    run_date=due_date + datetime.timedelta(hours=10),
                    job_suffix="due_date",
                )
                msg_params["to"] = sales_admin_phone
                msg_params["salesperson_name"] = sales_admin_name
                schedule_job(
                    msg_params,
                    run_date=due_date
                    + datetime.timedelta(hours=10),  # execute immediately
                    job_suffix="due_date",
                )
                print(
                    f"Scheduled due-date email for invoice {invoice_number} to {name} at {due_date}."
                )
            else:
                print(
                    f"Due date {due_date} is in the past. Skipping due-date email for invoice {invoice_number} to {name}."
                )
    else:
        print("Invoice Does Not Exist. Webhook Received")


def handle_estimate(data: dict):
    estimate = data.get("estimate")
    estimate_id = estimate.get("estimate_id", "")
    estimate_status = estimate.get("status", "")
    if estimate_id != "":
        # Sort all keys alphabetically
        sorted_estimate = sort_dict_keys(estimate)
        current_time = datetime.datetime.now()

        # Parse datetime fields
        datetime_fields = [
            'created_time', 'date', 'last_modified_time', 'expiry_date',
            'created_time_formatted', 'last_modified_time_formatted'
        ]

        for field in datetime_fields:
            if field in sorted_estimate and sorted_estimate[field]:
                parsed_dt = parse_datetime(sorted_estimate[field])
                if isinstance(parsed_dt, datetime.datetime):
                    sorted_estimate[field] = parsed_dt

        created_at = sorted_estimate.get("created_time", current_time)
        sorted_estimate["updated_at"] = current_time

        # Use atomic upsert to avoid duplicate inserts when concurrent webhook
        # calls arrive for the same estimate_id (Zoho often fires multiple webhooks
        # for a single event). $setOnInsert only runs when a new doc is created,
        # so created_at is never overwritten on subsequent updates.
        result = db.estimates.update_one(
            {"estimate_id": estimate_id},
            {
                "$set": sorted_estimate,
                "$setOnInsert": {"created_at": created_at},
            },
            upsert=True,
        )

        # If the document already existed, also sync the linked order status
        if result.matched_count > 0:
            estimate_number = estimate.get("estimate_number", "")
            estimate_url = estimate.get("estimate_url", "")
            db.orders.update_one(
                {"estimate_id": estimate_id},
                {
                    "$set": {
                        "status": estimate_status,
                        "estimate_url": estimate_url,
                        "estimate_number": estimate_number,
                    }
                },
            )
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

    import json  # make sure to import json if not already

    def clean_data(document):
        # Remove unwanted keys from the document
        for key in UNWANTED_KEYS:
            document.pop(key, None)
        return document

    def addresses_are_equal(addr1, addr2):
        """Compare two addresses for equality, ignoring order of keys"""
        # Remove None values and compare
        clean_addr1 = {k: v for k, v in addr1.items() if v is not None}
        clean_addr2 = {k: v for k, v in addr2.items() if v is not None}
        return clean_addr1 == clean_addr2

    # Clean contact data
    contact = clean_data(contact)

    if not existing_customer:
        addresses = []
        existing_ids = set()

        if "billing_address" in contact and contact["billing_address"]:
            addr = contact["billing_address"]
            addr_id = addr.get("address_id")
            if addr_id not in existing_ids:
                addresses.append(addr)
                existing_ids.add(addr_id)

        if "shipping_address" in contact and contact["shipping_address"]:
            addr = contact["shipping_address"]
            addr_id = addr.get("address_id")
            if addr_id not in existing_ids:
                addresses.append(addr)
                existing_ids.add(addr_id)

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

        # Handle address updates
        existing_addresses = existing_customer.get("addresses", [])
        existing_address_map = {
            addr.get("address_id"): addr for addr in existing_addresses
        }

        updated_addresses = list(existing_addresses)  # Start with existing addresses
        addresses_changed = False

        # Collect all addresses from the contact
        incoming_addresses = []

        # Add billing address
        if "billing_address" in contact and contact["billing_address"]:
            incoming_addresses.append(contact["billing_address"])

        # Add shipping address
        if "shipping_address" in contact and contact["shipping_address"]:
            incoming_addresses.append(contact["shipping_address"])

        # Add addresses from contact.addresses
        if "addresses" in contact and isinstance(contact["addresses"], list):
            incoming_addresses.extend(contact["addresses"])

        # Remove duplicates based on address_id
        unique_incoming = {}
        for addr in incoming_addresses:
            addr_id = addr.get("address_id")
            if addr_id:
                unique_incoming[addr_id] = addr

        # Process each incoming address
        for addr_id, new_addr in unique_incoming.items():
            if addr_id in existing_address_map:
                # Address exists, check if it needs updating
                existing_addr = existing_address_map[addr_id]
                if not addresses_are_equal(existing_addr, new_addr):
                    # Update the address in the list
                    for i, addr in enumerate(updated_addresses):
                        if addr.get("address_id") == addr_id:
                            updated_addresses[i] = new_addr
                            addresses_changed = True
                            print(f"Address {addr_id} updated")
                            break
            else:
                # New address, add it
                updated_addresses.append(new_addr)
                addresses_changed = True
                print(f"New address {addr_id} added")

        # Update addresses if there were changes
        if addresses_changed:
            update_fields["addresses"] = updated_addresses

        # Clean up fields we don't want to store separately
        update_fields.pop("billing_address", None)
        update_fields.pop("shipping_address", None)
        update_fields.pop("cf_margin", None)
        update_fields.pop("cf_in_ex", None)

        print("Existing addresses:", existing_addresses)
        print(
            "Updated addresses:",
            updated_addresses if addresses_changed else "No changes",
        )

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


def handle_accepted_estimate(data: dict):
    estimate = data.get("estimate")
    estimate_id = estimate.get("estimate_id", "")
    estimate_number = estimate.get("estimate_number", "")
    if estimate_id != "":
        template_doc = db.templates.find_one({"name": "accepted_estimate"})
        if not template_doc:
            print("Template 'accepted_estimate' not found, skipping notification")
            return
        template = serialize_mongo_document(dict(template_doc))

        notify_emails = ["pupscribeinvoicee@gmail.com", "barksalesamit@gmail.com"]
        for email in notify_emails:
            user_doc = db.users.find_one({"email": email})
            if not user_doc:
                print(f"User {email} not found, skipping WhatsApp for accepted_estimate")
                continue
            to = serialize_mongo_document(dict(user_doc))
            params = {"name": to.get("first_name"), "estimate_number": estimate_number}
            send_whatsapp(to.get("phone"), {**template}, {**params})
            create_notification(
                db,
                str(user_doc["_id"]),
                "estimate_accepted",
                f"Estimate {estimate_number} accepted",
                f"Estimate {estimate_number} has been accepted by the customer.",
                "/admin/orders",
            )
    else:
        print("Estimate Does Not Exist. Webhook Received")


def handle_draft_sales_order(data: dict):
    salesorder = data.get("salesorder")
    salesorder_id = salesorder.get("salesorder_id", "")
    salesorder_number = salesorder.get("salesorder_number", "")
    if salesorder_id != "":
        warehouse_team = serialize_mongo_document(
            list(db.users.find({"designation": "Warehouse Team"}))
        )

        template = serialize_mongo_document(
            dict(db.templates.find_one({"name": "draft_sales_order"}))
        )
        for person in warehouse_team:
            params = {
                "name": person.get("first_name"),
                "sales_order_number": salesorder_number,
            }
            send_whatsapp(person.get("phone"), {**template}, {**params})
            create_notification(
                db,
                person["_id"] if isinstance(person.get("_id"), str) else str(person["_id"]),
                "draft_sales_order",
                f"Draft sales order {salesorder_number}",
                f"Sales order {salesorder_number} has been drafted.",
                "/admin/orders",
            )
    else:
        print("Sales Order Does Not Exist. Webhook Received")


def handle_draft_invoice(data: dict):
    invoice = data.get("invoice")
    invoice_id = invoice.get("invoice_id", "")
    invoice_number = invoice.get("invoice_number", "")
    if invoice_id != "":
        template_doc = db.templates.find_one({"name": "draft_invoice"})
        if not template_doc:
            print("Template 'draft_invoice' not found, skipping notification")
            return
        template = serialize_mongo_document(dict(template_doc))

        recipients = []
        for query in [{"email": "barkbutleracc@gmail.com"}, {"designation": "Customer Care"}]:
            user_doc = db.users.find_one(query)
            if user_doc:
                recipients.append(serialize_mongo_document(dict(user_doc)))
            else:
                print(f"User matching {query} not found, skipping draft_invoice WhatsApp")

        for person in recipients:
            params = {"name": person.get("first_name"), "invoice_number": invoice_number}
            send_whatsapp(person.get("phone"), {**template}, {**params})
            create_notification(
                db,
                str(person["_id"]),
                "draft_invoice",
                f"Draft invoice {invoice_number}",
                f"Invoice {invoice_number} has been drafted.",
                "/admin/invoices",
            )
    else:
        print("Invoice Does Not Exist. Webhook Received")


def handle_shipment(data: dict):
    shipment = data.get("shipmentorder") or data.get("shipment_order")
    if not shipment:
        print("No shipment data found in webhook")
        return

    # Get shipment_id - try multiple possible field names
    shipment_id = str(
        shipment.get("shipment_order_id") or
        shipment.get("shipmentorder_id") or
        shipment.get("shipment_id") or
        ""
    )

    # Find or create shipment in database
    shipment_mongo_id = None
    if shipment_id:
        shipment["shipment_id"] = shipment_id
        existing_shipment = db.shipments.find_one({"shipment_id": shipment_id})

        # Sort all keys alphabetically
        sorted_data = sort_dict_keys(shipment)
        current_time = datetime.datetime.now()

        # Parse datetime fields
        datetime_fields = [
            'created_time', 'date', 'last_modified_time',
            'created_time_formatted', 'last_modified_time_formatted'
        ]

        for field in datetime_fields:
            if field in sorted_data and sorted_data[field]:
                parsed_dt = parse_datetime(sorted_data[field])
                if isinstance(parsed_dt, datetime.datetime):
                    sorted_data[field] = parsed_dt

        if existing_shipment:
            # Update existing shipment
            sorted_data["updated_at"] = current_time
            if "created_at" not in sorted_data and "created_at" in existing_shipment:
                sorted_data["created_at"] = existing_shipment["created_at"]
            elif "created_at" not in sorted_data:
                sorted_data["created_at"] = sorted_data.get("created_time", current_time)

            db.shipments.update_one(
                {"shipment_id": shipment_id}, {"$set": sorted_data}
            )
            shipment_mongo_id = str(existing_shipment["_id"])
            print(f"Updated shipment with shipment_id {shipment_id}")
        else:
            # Create new shipment
            sorted_data["created_at"] = sorted_data.get("created_time", current_time)
            sorted_data["updated_at"] = current_time
            insert_result = db.shipments.insert_one(sorted_data)
            shipment_mongo_id = str(insert_result.inserted_id)
            print(f"Created new shipment with shipment_id {shipment_id}")

    # Continue with existing notification logic
    invoices = shipment.get("invoices", [])
    invoice_number = invoices[-1].get("invoice_number", "") if len(invoices) > 0 else ""
    salesorder_number = (
        shipment.get("salesorder_number", "") if len(invoices) == 0 else ""
    )
    customer_name = shipment.get("customer_name", "")
    tracking_partner = shipment.get("carrier", "delivery_method")
    tracking_url = shipment.get("tracking_link", "")
    tracking_number = shipment.get("reference_number", "tracking_number")

    invoice = None
    if invoice_number != "":
        fetched = db["invoices"].find_one({"invoice_number": invoice_number})
        if fetched:
            invoice = serialize_mongo_document(dict(fetched))
            invoice_sales_person = invoice.get("cf_sales_person", "")
            salesperson = invoice.get("salesperson_name", "")
            button_url = f"{invoice.get('_id')}"
            # If the invoice is voided, fall back to the salesorder lookup below
            if invoice.get("status") == "void":
                so_ref = invoice.get("reference_number", "")
                # Extract the SO number (before any " | " suffix)
                salesorder_number = so_ref.split("|")[0].strip() if so_ref else ""
                invoice = None
                invoice_number = ""

    if salesorder_number != "":
        # Prefer a non-void invoice linked to this sales order
        invoice_query = {"reference_number": {"$regex": salesorder_number}, "status": {"$ne": "void"}}
        found_invoice = db["invoices"].find_one(invoice_query)
        if not found_invoice:
            # Fall back to any invoice if no non-void one exists
            invoice_query = {"reference_number": {"$regex": salesorder_number}}
            found_invoice = db["invoices"].find_one(invoice_query)

        if found_invoice:
            invoice = serialize_mongo_document(dict(found_invoice))
            salesorder_number = invoice.get("invoice_number", salesorder_number)

    if invoice is not None and (salesorder_number != "" or invoice_number != ""):
        invoice_sales_person = invoice.get("cf_sales_person", "")
        salesperson = invoice.get("salesperson_name", "")
        button_url = f"{invoice.get('_id')}"

        def _safe_user(query: dict) -> dict:
            """Fetch a user safely; returns an empty dict if not found."""
            doc = db.users.find_one(query)
            if not doc:
                print(f"Shipment notification: user not found for query {query}")
                return {}
            return serialize_mongo_document(dict(doc))

        sales_admin_1 = _safe_user({"designation": "Customer Care"})
        sales_admin_2 = _safe_user({"email": "pupscribeoffcoordinator@gmail.com"})
        sales_admin_3 = _safe_user({"email": "events@barkbutler.in"})
        sales_admin_4 = _safe_user({"email": "hitesh@barkbutler.in"})
        company_number = _safe_user({"role": "company_number"})
        all_salespeople = set()
        print("Custom Field Invoice Sales Person", invoice_sales_person)
        print("Invoice Sales Person", salesperson)

        if invoice_sales_person:
            for name in invoice_sales_person.split(","):
                name = name.strip()
                if name:
                    all_salespeople.add(name)

        if salesperson:
            for name in salesperson.split(","):
                name = name.strip()
                if name:
                    all_salespeople.add(name)

        print("All Sales People:", all_salespeople)

        # Check shipment status and use appropriate template and parameters
        if shipment.get('status',"") == 'delivered':
            template = serialize_mongo_document(
                dict(db.templates.find_one({"name": "shipment_delivery_notification"}))
            )

            # Extract and format delivery_date from shipment_delivery_notification field
            delivery_datetime_str = shipment.get("shipment_delivered_date", "")
            delivery_date = ""
            if delivery_datetime_str:
                try:
                    # Parse the datetime string "2025-11-19 15:30"
                    dt = datetime.datetime.strptime(delivery_datetime_str, "%Y-%m-%d %H:%M")
                    # Format as "19/11/2025"
                    delivery_date = dt.strftime("%d/%m/%Y")
                except ValueError:
                    print(f"Failed to parse delivery date: {delivery_datetime_str}")

            params = {
                "invoice_number": (
                    invoice_number if invoice_number != "" else salesorder_number
                ),
                "carrier_name": tracking_partner,
                "delivery_date": delivery_date,
                "awb_no": tracking_number,
                "customer_name":customer_name,
            }
        else:
            template = serialize_mongo_document(
                dict(db.templates.find_one({"name": "shipment_notification"}))
            )

            params = {
                "invoice_number": (
                    invoice_number if invoice_number != "" else salesorder_number
                ),
                "customer_name": customer_name,
                "tracking_url": tracking_url,
                "tracking_number": tracking_number,
                "button_url": button_url,
            }
        valid_salespeople = [sales_admin_1, sales_admin_2, sales_admin_4, company_number]

        if any(is_forbidden(sp.strip()) for sp in all_salespeople):
            # Send to admin users
            for person in [sales_admin_1, sales_admin_2, sales_admin_3, sales_admin_4, company_number]:
                phone = str(person.get("phone"))
                print("if", phone)
                if phone:  # Validate phone exists and is not empty
                    try:
                        send_whatsapp(phone, {**template}, {**params})
                    except Exception as e:
                        print(
                            f"Failed to send WhatsApp to admin {person.get('name', 'Unknown')}: {e}"
                        )
                else:
                    print(
                        f"No valid phone number for admin: {person.get('name', 'Unknown')}"
                    )
        else:
            # Send to specific salespeople
            for sp in all_salespeople:
                user = db.users.find_one({"code": sp})
                if user:
                    valid_salespeople.append(
                        {
                            "email": user.get("email", ""),
                            "name": user.get("name"),
                            "phone": user.get("phone"),
                        }
                    )

            for sp in valid_salespeople:
                name = sp.get("name", "Unknown")
                phone = str(sp.get("phone"))
                print("else", phone)
                try:
                    send_whatsapp(phone, {**template}, {**params})
                except Exception as e:
                    print(f"Failed to send WhatsApp to {name}: {e}")

        # In-app notification for shipment → all valid recipients by user lookup
        try:
            is_delivered = shipment.get("status", "") == "delivered"
            notif_type = "shipment_delivered" if is_delivered else "shipment_dispatched"
            notif_title = (
                f"Shipment delivered – {params.get('invoice_number', '')}"
                if is_delivered
                else f"Shipment dispatched – {params.get('invoice_number', '')}"
            )
            notif_body = (
                f"{customer_name} order delivered on {params.get('delivery_date', '')}."
                if is_delivered
                else f"{customer_name} order dispatched. Tracking: {tracking_number}."
            )
            shipment_link = f"/shipments/{shipment_mongo_id}" if shipment_mongo_id else "/shipments"

            notif_recipients = set()
            for person in [sales_admin_1, sales_admin_2, sales_admin_3, sales_admin_4, company_number]:
                uid = person.get("_id") or (db.users.find_one({"phone": person.get("phone")}, {"_id": 1}) or {}).get("_id")
                if uid:
                    notif_recipients.add(str(uid))
            for sp in all_salespeople:
                sp_user = db.users.find_one({"code": sp}, {"_id": 1})
                if sp_user:
                    notif_recipients.add(str(sp_user["_id"]))

            for uid in notif_recipients:
                create_notification(db, uid, notif_type, notif_title, notif_body, shipment_link)
        except Exception as _e:
            print(f"[notifications] shipment error: {_e}")
    else:
        print("Invoice Not Found for Given Shipment")


def handle_credit_note(data: dict):
    creditnote = data.get("creditnote")
    if not creditnote:
        print("No credit note data found in webhook")
        return

    creditnote_id = str(creditnote.get("creditnote_id", ""))

    if creditnote_id:
        existing = db.credit_notes.find_one({"creditnote_id": creditnote_id})

        # Sort all keys alphabetically
        sorted_data = sort_dict_keys(creditnote)
        current_time = datetime.datetime.now()

        # Parse datetime fields
        datetime_fields = [
            'created_time', 'date', 'last_modified_time',
            'created_time_formatted', 'last_modified_time_formatted'
        ]

        for field in datetime_fields:
            if field in sorted_data and sorted_data[field]:
                parsed_dt = parse_datetime(sorted_data[field])
                if isinstance(parsed_dt, datetime.datetime):
                    sorted_data[field] = parsed_dt

        if existing:
            # Update existing credit note
            sorted_data["updated_at"] = current_time
            if "created_at" not in sorted_data and "created_at" in existing:
                sorted_data["created_at"] = existing["created_at"]
            elif "created_at" not in sorted_data:
                sorted_data["created_at"] = sorted_data.get("created_time", current_time)

            db.credit_notes.update_one(
                {"creditnote_id": creditnote_id}, {"$set": sorted_data}
            )
            print(f"Updated credit note with creditnote_id {creditnote_id}")
        else:
            # Create new credit note
            sorted_data["created_at"] = sorted_data.get("created_time", current_time)
            sorted_data["updated_at"] = current_time
            db.credit_notes.insert_one(sorted_data)
            print(f"Created new credit note with creditnote_id {creditnote_id}")
    else:
        print("Credit Note ID not found. Webhook Received")



def handle_customer_payment(data: dict):
    payment = data.get("payment")
    if not payment:
        print("No customer payment data found in webhook")
        return

    payment_id = str(payment.get("payment_id", ""))

    if payment_id:
        existing = db.customer_payments.find_one({"payment_id": payment_id})

        # Sort all keys alphabetically
        sorted_data = sort_dict_keys(payment)
        current_time = datetime.datetime.now()

        # Parse datetime fields
        datetime_fields = [
            'created_time', 'date', 'last_modified_time',
            'created_time_formatted', 'last_modified_time_formatted'
        ]

        for field in datetime_fields:
            if field in sorted_data and sorted_data[field]:
                parsed_dt = parse_datetime(sorted_data[field])
                if isinstance(parsed_dt, datetime.datetime):
                    sorted_data[field] = parsed_dt

        if existing:
            # Update existing customer payment
            sorted_data["updated_at"] = current_time
            if "created_at" not in sorted_data and "created_at" in existing:
                sorted_data["created_at"] = existing["created_at"]
            elif "created_at" not in sorted_data:
                sorted_data["created_at"] = sorted_data.get("created_time", current_time)

            db.customer_payments.update_one(
                {"payment_id": payment_id}, {"$set": sorted_data}
            )
            print(f"Updated customer payment with payment_id {payment_id}")
        else:
            # Create new customer payment
            sorted_data["created_at"] = sorted_data.get("created_time", current_time)
            sorted_data["updated_at"] = current_time
            db.customer_payments.insert_one(sorted_data)
            print(f"Created new customer payment with payment_id {payment_id}")
    else:
        print("Payment ID not found. Webhook Received")


def handle_bill(data: dict):
    bill = data.get("bill")
    if not bill:
        print("No bill data found in webhook")
        return

    bill_id = str(bill.get("bill_id", ""))
    if not bill_id:
        print("Bill ID not found. Webhook Received")
        return

    existing = db.bills.find_one({"bill_id": bill_id})
    sorted_data = sort_dict_keys(bill)
    current_time = datetime.datetime.now()

    datetime_fields = [
        'created_time', 'date', 'last_modified_time', 'due_date',
        'created_time_formatted', 'last_modified_time_formatted',
    ]
    for field in datetime_fields:
        if field in sorted_data and sorted_data[field]:
            parsed_dt = parse_datetime(sorted_data[field])
            if isinstance(parsed_dt, datetime.datetime):
                sorted_data[field] = parsed_dt

    if existing:
        sorted_data["updated_at"] = current_time
        sorted_data["created_at"] = existing.get("created_at", sorted_data.get("created_time", current_time))
        db.bills.update_one({"bill_id": bill_id}, {"$set": sorted_data})
        print(f"Updated bill with bill_id {bill_id}")
    else:
        sorted_data["created_at"] = sorted_data.get("created_time", current_time)
        sorted_data["updated_at"] = current_time
        db.bills.insert_one(sorted_data)
        print(f"Created new bill with bill_id {bill_id}")


def handle_delete_estimate(data: dict):
    estimate = data.get("estimate") or {}
    estimate_id = estimate.get("estimate_id", "")
    if estimate_id:
        result = db.estimates.delete_one({"estimate_id": estimate_id})
        print(f"Deleted estimate {estimate_id}: {result.deleted_count} document(s) removed")
    else:
        print("No estimate_id found in delete webhook")


def handle_transfer_order(data: dict):
    transfer_order = data.get("transferorder") or data.get("transfer_order") or {}
    transfer_order_id = str(transfer_order.get("transfer_order_id", ""))
    if not transfer_order_id:
        print("No transfer_order_id found in webhook")
        return

    exists = serialize_mongo_document(
        db.transfer_orders.find_one({"transfer_order_id": transfer_order_id})
    )

    sorted_to = sort_dict_keys(transfer_order)
    current_time = datetime.datetime.now()

    datetime_fields = ["created_time", "date", "last_modified_time"]
    for field in datetime_fields:
        if field in sorted_to and sorted_to[field]:
            parsed_dt = parse_datetime(sorted_to[field])
            if isinstance(parsed_dt, datetime.datetime):
                sorted_to[field] = parsed_dt

    if not exists:
        sorted_to["created_at"] = sorted_to.get("created_time", current_time)
        sorted_to["updated_at"] = current_time
        db.transfer_orders.insert_one(sorted_to)
        print(f"Created new transfer order {transfer_order_id}")
    else:
        sorted_to["updated_at"] = current_time
        if "created_at" not in sorted_to and "created_at" in exists:
            sorted_to["created_at"] = exists["created_at"]
        elif "created_at" not in sorted_to:
            sorted_to["created_at"] = sorted_to.get("created_time", current_time)
        db.transfer_orders.update_one(
            {"transfer_order_id": transfer_order_id},
            {"$set": sorted_to},
        )
        print(f"Updated transfer order {transfer_order_id}")


def handle_delete_transfer_order(data: dict):
    transfer_order = data.get("transferorder") or data.get("transfer_order") or {}
    transfer_order_id = str(transfer_order.get("transfer_order_id", ""))
    if transfer_order_id:
        result = db.transfer_orders.delete_one({"transfer_order_id": transfer_order_id})
        print(f"Deleted transfer order {transfer_order_id}: {result.deleted_count} document(s) removed")
    else:
        print("No transfer_order_id found in delete webhook")


def handle_delete_invoice(data: dict):
    invoice = data.get("invoice") or {}
    invoice_id = invoice.get("invoice_id", "")
    if invoice_id:
        result = db.invoices.delete_one({"invoice_id": invoice_id})
        print(f"Deleted invoice {invoice_id}: {result.deleted_count} document(s) removed")
    else:
        print("No invoice_id found in delete webhook")


def handle_delete_customer_payment(data: dict):
    payment = data.get("payment") or {}
    payment_id = payment.get("payment_id", "")
    if payment_id:
        result = db.customer_payments.delete_one({"payment_id": payment_id})
        print(f"Deleted customer payment {payment_id}: {result.deleted_count} document(s) removed")
    else:
        print("No payment_id found in delete webhook")


def handle_delete_sales_order(data: dict):
    salesorder = data.get("salesorder") or {}
    salesorder_id = salesorder.get("salesorder_id", "")
    if salesorder_id:
        result = db.sales_orders.delete_one({"salesorder_id": salesorder_id})
        print(f"Deleted sales order {salesorder_id}: {result.deleted_count} document(s) removed")
    else:
        print("No salesorder_id found in delete webhook")


def handle_delete_package(data: dict):
    package = data.get("package") or {}
    package_id = package.get("package_id", "")
    if package_id:
        result = db.packages.delete_one({"package_id": package_id})
        print(f"Deleted package {package_id}: {result.deleted_count} document(s) removed")
    else:
        print("No package_id found in delete webhook")


def handle_delete_assembly(data: dict):
    assembly = data.get("bundle") or data.get("assembly") or {}
    bundle_id = assembly.get("bundle_id", "")
    if bundle_id:
        result = db.assemblies.delete_one({"bundle_id": bundle_id})
        print(f"Deleted assembly {bundle_id}: {result.deleted_count} document(s) removed")
    else:
        print("No bundle_id found in delete webhook")


def handle_delete_bill(data: dict):
    bill = data.get("bill") or {}
    bill_id = bill.get("bill_id", "")
    if bill_id:
        result = db.bills.delete_one({"bill_id": bill_id})
        print(f"Deleted bill {bill_id}: {result.deleted_count} document(s) removed")
    else:
        print("No bill_id found in delete webhook")


def handle_delete_purchase_order(data: dict):
    purchaseorder = data.get("purchaseorder") or {}
    purchaseorder_number = purchaseorder.get("purchaseorder_number", "")
    if purchaseorder_number:
        result = db.purchase_orders.delete_one({"purchaseorder_number": purchaseorder_number})
        print(f"Deleted purchase order {purchaseorder_number}: {result.deleted_count} document(s) removed")
    else:
        print("No purchaseorder_number found in delete webhook")


def handle_delete_item(data: dict):
    item = data.get("item") or {}
    item_id = item.get("item_id", "")
    if item_id:
        result = db.products.delete_one({"item_id": item_id})
        print(f"Deleted item {item_id}: {result.deleted_count} document(s) removed")
    else:
        print("No item_id found in delete webhook")


def handle_delete_vendor(data: dict):
    contact = data.get("contact") or {}
    contact_id = contact.get("contact_id", "")
    if contact_id:
        result = db.vendors.delete_one({"contact_id": contact_id})
        print(f"Deleted vendor {contact_id}: {result.deleted_count} document(s) removed")
    else:
        print("No contact_id found in vendor delete webhook")


def handle_delete_customer(data: dict):
    contact = data.get("contact") or {}
    contact_id = contact.get("contact_id", "")
    if contact_id:
        result = db.customers.delete_one({"contact_id": contact_id})
        print(f"Deleted customer {contact_id}: {result.deleted_count} document(s) removed")
    else:
        print("No contact_id found in customer delete webhook")


def handle_delete_credit_note(data: dict):
    creditnote = data.get("creditnote") or {}
    creditnote_id = creditnote.get("creditnote_id", "")
    if creditnote_id:
        result = db.credit_notes.delete_one({"creditnote_id": creditnote_id})
        print(f"Deleted credit note {creditnote_id}: {result.deleted_count} document(s) removed")
    else:
        print("No creditnote_id found in delete webhook")


def handle_sales_order(data: dict):
    salesorder = data.get("salesorder")
    if not salesorder:
        print("No sales order data found in webhook")
        return

    salesorder_id = str(salesorder.get("salesorder_id", ""))
    if not salesorder_id:
        print("Sales Order ID not found. Webhook Received")
        return

    existing = db.sales_orders.find_one({"salesorder_id": salesorder_id})
    sorted_data = sort_dict_keys(salesorder)
    current_time = datetime.datetime.now()

    datetime_fields = [
        'created_time', 'date', 'last_modified_time', 'shipment_date',
        'delivery_date', 'created_time_formatted', 'last_modified_time_formatted',
    ]
    for field in datetime_fields:
        if field in sorted_data and sorted_data[field]:
            parsed_dt = parse_datetime(sorted_data[field])
            if isinstance(parsed_dt, datetime.datetime):
                sorted_data[field] = parsed_dt

    if existing:
        sorted_data["updated_at"] = current_time
        sorted_data["created_at"] = existing.get("created_at", sorted_data.get("created_time", current_time))
        db.sales_orders.update_one({"salesorder_id": salesorder_id}, {"$set": sorted_data})
        print(f"Updated sales order with salesorder_id {salesorder_id}")
    else:
        sorted_data["created_at"] = sorted_data.get("created_time", current_time)
        sorted_data["updated_at"] = current_time
        db.sales_orders.insert_one(sorted_data)
        print(f"Created new sales order with salesorder_id {salesorder_id}")


def handle_assembly(data: dict):
    assembly = data.get("bundle") or data.get("assembly")
    if not assembly:
        print("No assembly data found in webhook")
        return

    bundle_id = str(assembly.get("bundle_id", ""))
    if not bundle_id:
        print("Bundle/Assembly ID not found. Webhook Received")
        return

    existing = db.assemblies.find_one({"bundle_id": bundle_id})
    sorted_data = sort_dict_keys(assembly)
    current_time = datetime.datetime.now()

    datetime_fields = [
        'created_time', 'date', 'last_modified_time',
        'created_time_formatted', 'last_modified_time_formatted',
    ]
    for field in datetime_fields:
        if field in sorted_data and sorted_data[field]:
            parsed_dt = parse_datetime(sorted_data[field])
            if isinstance(parsed_dt, datetime.datetime):
                sorted_data[field] = parsed_dt

    if existing:
        sorted_data["updated_at"] = current_time
        sorted_data["created_at"] = existing.get("created_at", sorted_data.get("created_time", current_time))
        db.assemblies.update_one({"bundle_id": bundle_id}, {"$set": sorted_data})
        print(f"Updated assembly with bundle_id {bundle_id}")
    else:
        sorted_data["created_at"] = sorted_data.get("created_time", current_time)
        sorted_data["updated_at"] = current_time
        db.assemblies.insert_one(sorted_data)
        print(f"Created new assembly with bundle_id {bundle_id}")


def handle_package(data: dict):
    package = data.get("package")
    if not package:
        print("No package data found in webhook")
        return

    package_id = str(package.get("package_id", ""))
    if not package_id:
        print("Package ID not found. Webhook Received")
        return

    existing = db.packages.find_one({"package_id": package_id})
    sorted_data = sort_dict_keys(package)
    current_time = datetime.datetime.now()

    datetime_fields = [
        'created_time', 'date', 'last_modified_time', 'shipment_date',
        'delivery_date', 'created_time_formatted', 'last_modified_time_formatted',
    ]
    for field in datetime_fields:
        if field in sorted_data and sorted_data[field]:
            parsed_dt = parse_datetime(sorted_data[field])
            if isinstance(parsed_dt, datetime.datetime):
                sorted_data[field] = parsed_dt

    if existing:
        sorted_data["updated_at"] = current_time
        sorted_data["created_at"] = existing.get("created_at", sorted_data.get("created_time", current_time))
        db.packages.update_one({"package_id": package_id}, {"$set": sorted_data})
        print(f"Updated package with package_id {package_id}")
    else:
        sorted_data["created_at"] = sorted_data.get("created_time", current_time)
        sorted_data["updated_at"] = current_time
        db.packages.insert_one(sorted_data)
        print(f"Created new package with package_id {package_id}")


@router.post("/estimate")
def estimate(data: dict):
    handle_estimate(data)
    return "Estimate Webhook Received Successfully"


@router.post("/invoice")
def invoice(data: dict):
    handle_invoice(data)
    return "Invoice Webhook Received Successfully"


@router.post("/customer")
def customer(data: dict):
    handle_customer(data)
    return "Customer Webhook Received Successfully"


@router.post("/item")
def item(data: dict, background_tasks: BackgroundTasks):
    handle_item(data, background_tasks)
    return "Item Webhook Received Successfully"


@router.post("/accepted_estimate")
def accepted_estimate(
    data: dict,
):
    handle_accepted_estimate(data)
    return "Accepted Estimate Webhook Received Successfully"


@router.post("/draft_sales_order")
def draft_sales_order(
    data: dict,
):
    handle_draft_sales_order(data)
    return "Draft Sales Order Webhook Received Successfully"


@router.post("/draft_invoice")
def draft_invoice(
    data: dict,
):
    handle_draft_invoice(data)
    return "Draft Invoice Webhook Received Successfully"


@router.post("/shipment")
def shipment(
    data: dict,
):
    handle_shipment(data)
    return "Shipment Webhook Received Successfully"


@router.post("/credit_note")
def credit_note(data: dict):
    handle_credit_note(data)
    return "Credit Note Webhook Received Successfully"


@router.post("/customer_payment")
def customer_payment(data: dict):
    handle_customer_payment(data)
    return "Customer Payment Webhook Received Successfully"


def sort_dict_keys(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively sort dictionary keys alphabetically
    """
    if isinstance(data, dict):
        return {key: sort_dict_keys(value) for key, value in sorted(data.items())}
    elif isinstance(data, list):
        return [sort_dict_keys(item) for item in data]
    else:
        return data


@router.post("/purchase_order")
async def purchase_order_webhook(request: Request):
    try:
        # Get the raw JSON data
        raw_data = await request.json()
        print(f"Received webhook data: {raw_data}")
        raw_data = raw_data["purchaseorder"]
        # Validate that we have the required purchaseorder_number field
        if "purchaseorder_number" not in raw_data:
            raise HTTPException(
                status_code=400, detail="Missing required field: purchaseorder_number"
            )

        purchaseorder_number = raw_data["purchaseorder_number"]

        # Check if purchase order exists in database
        existing_po = db.purchase_orders.find_one(
            {"purchaseorder_number": purchaseorder_number}
        )

        # Sort all keys alphabetically
        sorted_data = sort_dict_keys(raw_data)

        # Prepare the document for MongoDB
        current_time = datetime.datetime.now()

        if existing_po:
            # Update existing purchase order
            sorted_data["updated_at"] = current_time
            # Keep the original created_at if it exists
            if "created_at" not in sorted_data and "created_at" in existing_po:
                sorted_data["created_at"] = existing_po["created_at"]
            elif "created_at" not in sorted_data:
                sorted_data["created_at"] = current_time

            # Update the document
            result = db.purchase_orders.update_one(
                {"purchaseorder_number": purchaseorder_number}, {"$set": sorted_data}
            )

            print(f"Updated purchase order {purchaseorder_number}")

            return {
                "status": "success",
                "action": "updated",
                "purchaseorder_number": purchaseorder_number,
                "modified_count": result.modified_count,
            }

        else:
            # Create new purchase order
            sorted_data["created_at"] = current_time
            sorted_data["updated_at"] = current_time

            # Insert the new document
            result = db.purchase_orders.insert_one(sorted_data)

            print(f"Created new purchase order {purchaseorder_number}")

            return {
                "status": "success",
                "action": "created",
                "purchaseorder_number": purchaseorder_number,
                "inserted_id": str(result.inserted_id),
            }

    except ValueError as e:
        print(f"Invalid JSON data: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON data")

    except Exception as e:
        print(f"Error processing purchase order webhook: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/vendor")
async def vendor_webhook(request: Request):
    """
    Webhook endpoint to receive vendor data from Zoho Books
    """
    try:
        # Get the raw JSON data
        raw_data = await request.json()
        print(f"Received vendor webhook data: {raw_data}")
        raw_data = raw_data["contact"]
        # Validate that we have the required contact_id field
        if "contact_id" not in raw_data:
            raise HTTPException(
                status_code=400, detail="Missing required field: contact_id"
            )

        contact_id = raw_data["contact_id"]

        # Check if vendor exists in database
        existing_vendor = db.vendors.find_one({"contact_id": contact_id})

        # Sort all keys alphabetically
        sorted_data = sort_dict_keys(raw_data)

        # Prepare the document for MongoDB
        current_time = datetime.datetime.now()

        if existing_vendor:
            # Update existing vendor
            sorted_data["updated_at"] = current_time
            # Keep the original created_at if it exists
            if "created_at" not in sorted_data and "created_at" in existing_vendor:
                sorted_data["created_at"] = existing_vendor["created_at"]
            elif "created_at" not in sorted_data:
                sorted_data["created_at"] = current_time

            # Update the document
            result = db.vendors.update_one(
                {"contact_id": contact_id}, {"$set": sorted_data}
            )

            print(f"Updated vendor with contact_id {contact_id}")

            return {
                "status": "success",
                "action": "updated",
                "contact_id": contact_id,
                "vendor_name": sorted_data.get("contact_name", "Unknown"),
                "modified_count": result.modified_count,
            }

        else:
            # Create new vendor
            sorted_data["created_at"] = current_time
            sorted_data["updated_at"] = current_time

            # Insert the new document
            result = db.vendors.insert_one(sorted_data)

            print(f"Created new vendor with contact_id {contact_id}")

            return {
                "status": "success",
                "action": "created",
                "contact_id": contact_id,
                "vendor_name": sorted_data.get("contact_name", "Unknown"),
                "inserted_id": str(result.inserted_id),
            }

    except ValueError as e:
        print(f"Invalid JSON data: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON data")

    except Exception as e:
        print(f"Error processing vendor webhook: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/bill")
def bill(data: dict):
    handle_bill(data)
    return "Bill Webhook Received Successfully"


@router.post("/delete_estimate")
def delete_estimate(data: dict):
    handle_delete_estimate(data)
    return "Delete Estimate Webhook Received Successfully"


@router.post("/delete_invoice")
def delete_invoice(data: dict):
    handle_delete_invoice(data)
    return "Delete Invoice Webhook Received Successfully"


@router.post("/delete_customer_payment")
def delete_customer_payment(data: dict):
    handle_delete_customer_payment(data)
    return "Delete Customer Payment Webhook Received Successfully"


@router.post("/delete_sales_order")
def delete_sales_order(data: dict):
    handle_delete_sales_order(data)
    return "Delete Sales Order Webhook Received Successfully"


@router.post("/delete_package")
def delete_package(data: dict):
    handle_delete_package(data)
    return "Delete Package Webhook Received Successfully"


@router.post("/delete_assembly")
def delete_assembly(data: dict):
    handle_delete_assembly(data)
    return "Delete Assembly Webhook Received Successfully"


@router.post("/delete_bill")
def delete_bill(data: dict):
    handle_delete_bill(data)
    return "Delete Bill Webhook Received Successfully"


@router.post("/delete_purchase_order")
def delete_purchase_order(data: dict):
    handle_delete_purchase_order(data)
    return "Delete Purchase Order Webhook Received Successfully"


@router.post("/delete_item")
def delete_item(data: dict):
    handle_delete_item(data)
    return "Delete Item Webhook Received Successfully"


@router.post("/transfer_order")
def transfer_order(data: dict):
    handle_transfer_order(data)
    return "Transfer Order Webhook Received Successfully"


@router.post("/delete_transfer_order")
def delete_transfer_order(data: dict):
    handle_delete_transfer_order(data)
    return "Delete Transfer Order Webhook Received Successfully"


@router.post("/delete_vendor")
def delete_vendor(data: dict):
    handle_delete_vendor(data)
    return "Delete Vendor Webhook Received Successfully"


@router.post("/delete_customer")
def delete_customer(data: dict):
    handle_delete_customer(data)
    return "Delete Customer Webhook Received Successfully"


@router.post("/delete_credit_note")
def delete_credit_note(data: dict):
    handle_delete_credit_note(data)
    return "Delete Credit Note Webhook Received Successfully"


@router.post("/sales_order")
def sales_order(data: dict):
    handle_sales_order(data)
    return "Sales Order Webhook Received Successfully"


@router.post("/assembly")
def assembly(data: dict):
    handle_assembly(data)
    return "Assembly Webhook Received Successfully"


@router.post("/package")
def package(data: dict):
    handle_package(data)
    return "Package Webhook Received Successfully"
