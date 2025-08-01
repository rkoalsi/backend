from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from backend.config.scheduler import schedule_job, remove_scheduled_jobs  # type: ignore
from backend.config.whatsapp import send_whatsapp  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import datetime, json, os, requests, time, threading
from dateutil.parser import parse
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from .helpers import send_email
from typing import Dict, Any
from bson import ObjectId
from collections import defaultdict

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()

now = datetime.datetime.utcnow()
TOTAL_WAREHOUSE_URL = os.getenv("TOTAL_WAREHOUSE_URL")
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
org_id = os.getenv("ORG_ID")
collection = db["products"]

_access_token_cache = {"token": None, "expires_at": None}

update_stock_lock = threading.Lock()


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


brands = {
    "zippy": "Zippy Paws",
    "waggie": "Waggie Wag",
    "fofos": "FOFOS",
    "truelove": "Truelove",
    "barkbutler": "Barkbutler",
    "dogfest": "Dogfest",
    "catfest": "Catfest",
    "dux": "Dux",
    "squeeezys": "Squeeezys",
    "joyser": "Joyser",
}


def extract_brand_from_product(product_name):
    """Extract brand name from product name."""
    words = product_name.split()
    if not words:
        return ""

    if words[0].lower() in brands.keys():
        return brands[words[0].lower()]


def create_special_margins_for_new_product(
    product_id: str, product_name: str, brand_name: str
):
    """
    Background task to create special margins for a new product based on existing brand margins.
    """
    try:
        # MongoDB connection (you might want to use your existing db connection)

        print(
            f"Creating special margins for new product: {product_name} (Brand: {brand_name})"
        )

        # Get all existing special margins to find customers with this brand
        existing_margins = list(
            db.special_margins.find({}, {"customer_id": 1, "name": 1, "margin": 1})
        )

        # Group by customer and extract brand margins
        customer_brand_margins = defaultdict(dict)

        for margin_doc in existing_margins:
            customer_id = str(margin_doc.get("customer_id"))
            product_name_in_margin = str(margin_doc.get("name", ""))
            margin_value = margin_doc.get("margin")

            if customer_id and margin_value is not None:
                # Extract brand from the product name in special margins
                brand_from_margin = extract_brand_from_product(product_name_in_margin)

                # If this customer doesn't have this brand margin yet, add it
                if (
                    brand_from_margin
                    and brand_from_margin not in customer_brand_margins[customer_id]
                ):
                    customer_brand_margins[customer_id][
                        brand_from_margin
                    ] = margin_value

        # Check if any customers have margins for this brand
        customers_with_brand_margins = []
        for customer_id, brand_margins in customer_brand_margins.items():
            if brand_name in brand_margins:
                customers_with_brand_margins.append(
                    {
                        "customer_id": customer_id,
                        "margin": brand_margins[brand_name],
                    }
                )

        print(
            f"Found {len(customers_with_brand_margins)} customers with margins for brand '{brand_name}'"
        )

        # Create special margins for the new product
        special_margins_to_insert = []

        for customer_data in customers_with_brand_margins:
            customer_id = customer_data["customer_id"]
            margin = customer_data["margin"]

            # Check if this customer-product combination already exists
            existing_special_margin = db.special_margins.find_one(
                {
                    "customer_id": ObjectId(customer_id),
                    "product_id": ObjectId(product_id),
                }
            )

            if not existing_special_margin:
                special_margins_to_insert.append(
                    {
                        "customer_id": ObjectId(customer_id),
                        "product_id": ObjectId(product_id),
                        "margin": margin,
                        "name": product_name,
                    }
                )
                print(
                    f"Will create special margin for customer {customer_id}: {product_name} with margin {margin}"
                )

        # Bulk insert special margins
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
                    "status": item.get("status", "inactive"),
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
                    "brand": brands.get(brand_name.lower(), brand_name.capitalize()),
                }
                send_whatsapp(
                    to=person["phone"],
                    template_doc=template,
                    params=params,
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


def get_zoho_stock(day=None, month=None, year=None):
    """
    Fetch stock data from Zoho Inventory with retries and timeout handling.
    Fetches multiple pages concurrently to optimize performance.
    """
    # Set the date
    if day and month and year:
        try:
            now_date = datetime.datetime(year, month, day)
        except ValueError as e:
            print(f"Invalid date provided: {e}")
            return []
    else:
        now_date = datetime.datetime.utcnow()
    to_date = now_date.date()
    print(f"Fetching stock for {now_date.strftime('%b-%Y')} with date {to_date}")

    warehouse_stock = []
    access_token = get_cached_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    # Fetch the total number of pages
    try:
        response = fetch_with_retries(
            url=TOTAL_WAREHOUSE_URL.format(date1=to_date, org_id=org_id),
            headers=headers,
            retries=3,
            timeout=10,
            page_number="Total Pages",
        )
        if response is None:
            print("Failed to retrieve the total number of pages.")
            return []

        total_pages = int(response.json().get("page_context", {}).get("total_pages", 1))
        print(f"Total pages to fetch: {total_pages}")

        # Define the maximum number of concurrent threads
        max_workers = 5  # Adjust based on API rate limits and performance
        failed_pages = []

        def fetch_page(page_number):
            page_url = WAREHOUSE_URL.format(
                page=page_number, date1=to_date, org_id=org_id
            )
            response = fetch_with_retries(
                url=page_url,
                headers=headers,
                retries=3,
                timeout=10,
                page_number=page_number,
            )
            if response is not None:
                try:
                    page_data = response.json()
                    warehouse_stock_info = page_data.get("warehouse_stock_info", [])
                    return warehouse_stock_info
                except json.JSONDecodeError as e:
                    print(f"Page {page_number}: JSON decode error: {e}")
                    return None
            else:
                return None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all page fetch tasks
            future_to_page = {
                executor.submit(fetch_page, i): i for i in range(1, total_pages + 1)
            }
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    data = future.result()
                    if data is not None:
                        warehouse_stock.extend(data)
                    else:
                        failed_pages.append(page)
                except Exception as exc:
                    print(f"Page {page} generated an exception: {exc}")
                    failed_pages.append(page)

        # Retry failed pages
        if failed_pages:
            print(f"Retrying failed pages: {failed_pages}")
            for page in failed_pages:
                data = fetch_page(page)
                if data is not None:
                    warehouse_stock.extend(data)
                else:
                    print(f"Page {page}: Failed to fetch on retry.")

    except Exception as e:
        print(f"Failed to fetch total pages or initial data: {e}")
        return []

    print(f"Total warehouse stock items fetched: {len(warehouse_stock)}")

    # Filter and process warehouse stock data
    arr = []
    target_warehouse = "pupscribe enterprises private limited".strip().lower()
    for item in warehouse_stock:
        item_name = item.get("item_name", "").strip().lower()
        warehouses = item.get("warehouses", [])
        for w in warehouses:
            warehouse_name = w.get("warehouse_name", "").strip().lower()
            if warehouse_name == target_warehouse:
                try:
                    stock_quantity = int(w.get("quantity_available_for_sale", 0))
                    arr.append(
                        {
                            "name": item_name,
                            "stock": stock_quantity,
                        }
                    )
                    print(f"Added stock for '{item_name}': {stock_quantity}")
                except ValueError:
                    print(
                        f"Invalid stock quantity for item '{item_name}': {w.get('quantity_available_for_sale')}"
                    )
    print(f"Total stock items after filtering: {len(arr)}")
    print("Data fetching complete. Proceeding to update the database.")
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
        return

    # Fetch stock data from Zoho
    stock_data = get_zoho_stock()
    if not stock_data:
        print("No stock data fetched from Zoho.")
        return

    stock_dict = {item["name"]: item["stock"] for item in stock_data}
    print(f"Stock data contains {len(stock_dict)} items.")

    # Prepare bulk updates
    updates = []
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
            print(f"Prepared update for '{product_name}' with stock: {stock}")
        else:
            print(f"No stock data for product '{product_name}'")

    # Execute bulk updates
    if updates:
        try:
            result = collection.bulk_write(updates)
            print(f"Total products updated with stock: {result.modified_count}")
        except Exception as e:
            print(f"Failed to execute bulk updates: {e}")
    else:
        print("No updates required.")


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
        sales_admin = db.users.find_one({"email": "crmbarksales@gmail.com"})
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
            db.estimates.update_one(
                {"estimate_id": estimate_id},
                {"$set": {**estimate, "updated_at": datetime.datetime.now()}},
            )
            db.orders.update_one(
                {"estimate_id": estimate_id}, {"$set": {"status": estimate_status}}
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
        template = serialize_mongo_document(
            dict(db.templates.find_one({"name": "accepted_estimate"}))
        )

        to1 = serialize_mongo_document(
            dict(db.users.find_one({"email": "pupscribeinvoicee@gmail.com"}))
        )
        to2 = serialize_mongo_document(
            dict(db.users.find_one({"email": "crmbarksales@gmail.com"}))
        )

        for to in [to1, to2]:
            params = {"name": to.get("first_name"), "estimate_number": estimate_number}
            send_whatsapp(to.get("phone"), {**template}, {**params})
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
    else:
        print("Sales Order Does Not Exist. Webhook Received")


def handle_draft_invoice(data: dict):
    invoice = data.get("invoice")
    invoice_id = invoice.get("invoice_id", "")
    invoice_number = invoice.get("invoice_number", "")
    if invoice_id != "":
        member1 = serialize_mongo_document(
            dict(db.users.find_one({"email": "barkbutleracc@gmail.com"}))
        )
        member2 = serialize_mongo_document(
            dict(db.users.find_one({"designation": "Customer Care"}))
        )

        template = serialize_mongo_document(
            dict(db.templates.find_one({"name": "draft_invoice"}))
        )
        for person in [member1, member2]:
            params = {
                "name": person.get("first_name"),
                "invoice_number": invoice_number,
            }
            send_whatsapp(person.get("phone"), {**template}, {**params})
    else:
        print("Invoice Does Not Exist. Webhook Received")


def handle_shipment(data: dict):
    shipment = data.get("shipmentorder")
    invoices = shipment.get("invoices", [])
    invoice_number = invoices[-1].get("invoice_number", "") if len(invoices) > 0 else ""
    salesorder_number = (
        shipment.get("salesorder_number", "") if len(invoices) == 0 else ""
    )
    customer_name = shipment.get("customer_name", "")
    tracking_number = shipment.get("reference_number", "")
    tracking_partner = shipment.get("carrier", "")

    delivery_partner = serialize_mongo_document(
        dict(db["delivery_partners"].find_one({"name": tracking_partner}))
    )
    tracking_url = delivery_partner.get("tracking_url", "")

    if invoice_number != "":
        invoice = serialize_mongo_document(
            dict(db["invoices"].find_one({"invoice_number": invoice_number}))
        )
        invoice_sales_person = invoice.get("cf_sales_person", "")
        salesperson = invoice.get("salesperson_name", "")
        button_url = f"{invoice.get('_id')}"

    if salesorder_number != "":
        # Use regex to find invoice where reference_number contains salesorder_number
        invoice_query = {"reference_number": {"$regex": salesorder_number}}
        found_invoice = db["invoices"].find_one(invoice_query)

        if found_invoice:
            invoice = serialize_mongo_document(dict(found_invoice))
            salesorder_number = invoice.get("invoice_number", salesorder_number)

    if salesorder_number != "" or invoice_number != "":
        invoice_sales_person = invoice.get("cf_sales_person", "")
        salesperson = invoice.get("salesperson_name", "")
        button_url = f"{invoice.get('_id')}"
        print(invoice_number)

        sales_admin_1 = serialize_mongo_document(
            dict(db.users.find_one({"designation": "Customer Care"}))
        )
        sales_admin_2 = serialize_mongo_document(
            dict(db.users.find_one({"email": "pupscribeoffcoordinator@gmail.com"}))
        )
        sales_admin_3 = serialize_mongo_document(
            dict(db.users.find_one({"email": "events@barkbutler.in"}))
        )
        template = serialize_mongo_document(
            dict(db.templates.find_one({"name": "shipment_notification"}))
        )

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

        params = {
            "invoice_number": (
                invoice_number if invoice_number != "" else salesorder_number
            ),
            "customer_name": customer_name,
            "tracking_url": tracking_url,
            "tracking_number": tracking_number,
            "button_url": button_url,
        }
        valid_salespeople = [sales_admin_1, sales_admin_2]

        if any(is_forbidden(sp.strip()) for sp in all_salespeople):
            # Send to admin users
            for person in [sales_admin_1, sales_admin_2, sales_admin_3]:
                phone = int(person.get("phone"))
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
                phone = int(sp.get("phone"))
                print("else", phone)
                try:
                    send_whatsapp(phone, {**template}, {**params})
                except Exception as e:
                    print(f"Failed to send WhatsApp to {name}: {e}")


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
