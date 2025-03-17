from types import NoneType
from pymongo.collection import Collection
from datetime import datetime
from typing import List
from .helpers import get_access_token, send_email
from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
import time, os, httpx, requests, asyncio, random, json
from dotenv import load_dotenv
from fastapi.responses import Response
from backend.config.constants import terms, STATE_CODES  # type: ignore
from backend.config.whatsapp import send_whatsapp  # type:ignore
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pymongo import DESCENDING, ASCENDING
from pathlib import Path

load_dotenv()

org_id = os.getenv("ORG_ID")
ESTIMATE_URL = os.getenv("ESTIMATE_URL")
PDF_URL = os.getenv("PDF_URL")


# Connect to MongoDB
client, db = connect_to_mongo()
orders_collection = db["orders"]
customers_collection = db["customers"]
users_collection = db["users"]

router = APIRouter()

timeout = httpx.Timeout(30.0, connect=10.0, read=30.0, write=30.0)


# Create a new order
def create_order(order: dict, collection: Collection) -> str:
    # Explicitly convert customer_id and product_ids to ObjectId
    customer_id = order.get("customer_id", "")
    products = order.get("products", [])
    if customer_id:
        order["customer_id"] = ObjectId(order.get("customer_id"))

    if len(products) > 0:
        order["products"] = [
            {"product_id": ObjectId(item["product_id"]), "quantity": item["quantity"]}
            for item in products
        ]
    order["created_by"] = ObjectId(order.get("created_by", ""))
    order["created_at"] = datetime.utcnow()
    order["updated_at"] = datetime.utcnow()

    # Insert the document into MongoDB
    result = collection.insert_one(order)
    return str(result.inserted_id)


def check_if_order_exists(
    created_by: str, orders_collection: Collection
) -> dict | bool:
    order = orders_collection.find_one(
        {"created_by": ObjectId(created_by), "status": "draft"}
    )
    if order:
        return order
    else:
        return False


# Get an order by ID and populate customer and product details
def get_order(
    order_id: str,
    orders_collection: Collection,
):
    result = orders_collection.find_one({"_id": ObjectId(order_id)})
    if result:
        order = result
        order["status"] = str(order["status"]).capitalize()
        return serialize_mongo_document(order)
    return None


def get_all_orders(
    role: str,
    created_by: str,
    status: str,
    collection: Collection,
    users_collection: Collection,
):
    query = {}

    # Salesperson-specific query
    if role == "salesperson":
        if not created_by:
            raise ValueError("Salesperson role requires 'created_by'")
        query["created_by"] = ObjectId(created_by)
        query["is_deleted"] = {"$exists": False}
        query["$or"] = [{"total_amount": {"$gt": 0}}, {"spreadsheet_created": True}]
    if status:
        query["status"] = status

    # Fetch orders
    orders = collection.find(query).sort({"created_at": -1})

    # For admin, populate created_by_info with user information
    orders_with_user_info = []
    if "admin" in role:
        for order in orders:
            user_info = users_collection.find_one({"_id": order["created_by"]})
            if user_info:
                order["created_by_info"] = {
                    "id": str(user_info["_id"]),
                    "name": user_info.get("name"),
                    "email": user_info.get("email"),
                }
            orders_with_user_info.append(serialize_mongo_document(order))
    else:
        # For salesperson, no need to populate created_by_info
        orders_with_user_info = [serialize_mongo_document(order) for order in orders]

    return orders_with_user_info


# Update an order


def update_order(
    order_id: str,
    order_update: dict,
    order_collection: Collection,
    customer_collection: Collection,
):
    order_update["updated_at"] = datetime.utcnow()
    # Handle customer updates
    if "customer_id" in order_update:
        customer_id = order_update.get("customer_id")
        customer = customer_collection.find_one({"_id": ObjectId(customer_id)})

        if customer:
            order_update["customer_id"] = ObjectId(customer_id)
            order_update["customer_name"] = (
                customer.get("company_name")
                if customer.get("company_name") != ""
                else customer.get("contact_name")
            )
            order_update["gst_type"] = (
                customer.get("cf_in_ex")
                if type(customer.get("cf_in_ex")) is not NoneType
                else "Exclusive"
            )

    # Handle product updates (replace the entire product list)
    if "products" in order_update:
        updated_products = []
        for product in order_update.get("products", []):
            product_id = ObjectId(product["_id"])
            updated_products.append(
                {
                    "product_id": product_id,
                    "tax_percentage": (
                        product.get("item_tax_preferences", [{}])[0].get(
                            "tax_percentage", 0
                        )
                    ),
                    "brand": product.get("brand", ""),
                    "product_code": product.get("cf_sku_code", ""),
                    "quantity": product.get("quantity", 1),
                    "name": product.get("item_name", ""),
                    "image_url": product.get("image_url", ""),
                    "margin": product.get("margin", ""),
                    "price": product.get("rate", 0),
                    "added_by": product.get("added_by", ""),
                }
            )
        # Replace the product list in the update payload
        order_update["products"] = updated_products
    # Perform the update in MongoDB
    order_collection.update_one({"_id": ObjectId(order_id)}, {"$set": order_update})


# Delete an order
def delete_order(order_id: str, collection: Collection):
    order = collection.find_one({"_id": ObjectId(order_id)})
    if not order.get("estimate_created", False):
        collection.update_one(
            {"_id": order.get("_id")},
            {
                "$set": {
                    "status": "deleted",
                    "is_deleted": True,
                    "deleted_at": datetime.now(),
                }
            },
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Order With Estimate Created Cannot Be Marked As Deleted",
        )


def clear_empty_orders(user_id: str, collection: Collection):
    orders = collection.find({"created_by": ObjectId(user_id)})
    for order in orders:
        if not order.get("customer_id"):
            collection.delete_one(
                {"_id": order.get("_id")},
                {
                    "$set": {
                        "status": "deleted",
                        "is_deleted": True,
                        "deleted_at": datetime.now(),
                    }
                },
            )


async def email_estimate(
    status: str,
    order_id: str,
    estimate_id: str,
    estimate_number: str,
    estimate_url: str,
    message: str,
    headers: dict,
    timeout: any,
):
    async with httpx.AsyncClient(timeout=timeout) as client:
        if status in {"accepted", "declined"}:
            await client.post(
                url=f"https://books.zoho.com/api/v3/estimates/{estimate_id}/status/sent?organization_id={org_id}",
                headers=headers,
            )
            status_response = await client.post(
                url=f"https://books.zoho.com/api/v3/estimates/{estimate_id}/status/{status}?organization_id={org_id}",
                headers=headers,
            )
            status_response.raise_for_status()
            message += status_response.json()["message"]
            db.orders.update_one(
                {"_id": ObjectId(order_id)},
                {
                    "$set": {
                        "status": f"{status}",
                        "estimate_created": True,
                        "estimate_id": estimate_id,
                        "estimate_number": estimate_number,
                        "estimate_url": estimate_url,
                    }
                },
            )


def clear_cart(order_id: str, orders_collection: Collection):
    order = orders_collection.update_one(
        {"_id": ObjectId(order_id)}, {"$set": {"products": []}}
    )
    return order.did_upsert


def validate_order(order_id: str):
    order = db.orders.find_one({"_id": ObjectId(order_id)})

    if not order:
        raise HTTPException(status_code=400, detail="Order not found")
    # Check if shipping address is missing or invalid
    customer_id = order.get("customer_id", "")
    customer = customers_collection.find_one({"_id": ObjectId(customer_id)})
    if customer.get("status") == "inactive":
        raise HTTPException(
            status_code=400, detail="Cannot Proceed, Customer is Inactive"
        )
    shipping_address = order.get("shipping_address", {}).get("address")
    if not shipping_address:
        raise HTTPException(status_code=400, detail="Shipping address is missing")

    # Check if billing address is missing or invalid
    billing_address = order.get("billing_address", {}).get("address")
    if not billing_address:
        raise HTTPException(status_code=400, detail="Billing address is missing")

    # Check if place of supply is missing or invalid
    place_of_supply = order.get("shipping_address", {}).get("state_code")
    state_str = str(order.get("shipping_address", {}).get("state", ""))
    place_of_supply_backup = STATE_CODES.get(state_str.title())
    if not place_of_supply and not place_of_supply_backup:
        raise HTTPException(status_code=400, detail="Place of supply is missing")

    # Check if products are missing or invalid
    products = order.get("products", [])
    if not products:
        raise HTTPException(status_code=400, detail="Products are missing")
    for product in products:
        doc = dict(db.products.find_one({"_id": ObjectId(product.get("product_id"))}))
        if doc.get("status") == "inactive":
            raise HTTPException(
                status_code=400, detail=f"Cannot Proceed, {doc.get('name')} is inactive"
            )
    # Check if total amount is missing or invalid
    total_amount = order.get("total_amount")
    if total_amount is None:
        raise HTTPException(status_code=400, detail="Total amount is missing")

    return True


# API Endpoints


# Create a new order
@router.post("/")
def create_new_order(order: dict):
    """
    Create a new order with raw dictionary data.
    """
    try:
        order_id = create_order(order, orders_collection)
        order["_id"] = order_id  # Add the generated ID back to the response
        return serialize_mongo_document(order)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/check")
def check_order_status(order: dict):
    """
    Create a new order with raw dictionary data.
    """
    try:
        created_by = order.get("created_by", "")
        if not created_by:
            raise HTTPException(status_code=400, detail="created_by is required")
        order = check_if_order_exists(created_by, orders_collection)
        if order:
            return {
                **serialize_mongo_document(order),
                "message": "Existing Draft Order Found",
                "can_create": False,
            }
        else:
            return {"message": "Existing Draft Order Not Found", "can_create": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def get_active_products(sort: dict):
    products = list(
        db.products.aggregate(
            [
                {"$match": {"status": "active", "stock": {"$gt": 0}}},
                {"$sort": sort},
            ]
        )
    )
    return products


BASE_DIR = (
    Path(__file__).resolve().parent.parent
)  # Get the directory of the current script
SERVICE_ACCOUNT_FILE = (
    BASE_DIR / "creds.json"
)  # Adjust based on where the file is stored

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/script.projects",
]


# Authenticate and create a service client
def get_sheets_service():
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    return service


def get_drive_service():
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=credentials)
    return service


def get_apps_script_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,  # Update this path
        scopes=[
            "https://www.googleapis.com/auth/script.projects",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return build("script", "v1", credentials=creds)


# Helper function to implement retry logic for API requests
def execute_with_retry(request, retries=5, backoff=1):
    for attempt in range(retries):
        try:
            return request.execute()
        except HttpError as err:
            if err.resp.status == 429:  # Rate limit exceeded
                print(f"Rate limit exceeded, retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff
            else:
                raise  # If it's another error, raise it
    raise Exception("Exceeded maximum retry attempts")


async def execute_with_retry_async(request, max_retries=5, initial_delay=1.0):
    """
    Executes a Google API request with automatic exponential backoff retry.

    Parameters:
        - request: The Google API request object.
        - max_retries: Maximum retry attempts.
        - initial_delay: Initial wait time before retry (in seconds).

    Returns:
        - API response if successful.

    Raises:
        - HttpError if all retries fail.
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return await asyncio.to_thread(request.execute)  # Run in async mode
        except HttpError as err:
            if err.resp.status in [500, 502, 503, 504]:  # Retry on transient errors
                if attempt < max_retries - 1:
                    wait_time = delay + random.uniform(0, 0.5)  # Add jitter
                    print(
                        f"Retry {attempt + 1}/{max_retries} after {wait_time:.2f}s due to error: {err}"
                    )
                    await asyncio.sleep(wait_time)
                    delay *= 2  # Exponential backoff
                else:
                    raise HTTPException(
                        status_code=500, detail=f"Google API Error: {err}"
                    )
            else:
                raise  # If it's a permanent error, don't retry


@router.get("/download_order_form")
async def download_order_form(customer_id: str, order_id: str, sort: str = "default"):
    if not customer_id:
        raise HTTPException(status_code=400, detail="Customer is Required")
    if not order_id:
        raise HTTPException(status_code=400, detail="Order is Required")

    order = db.orders.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise HTTPException(status_code=400, detail="Order does not exist")

    spreadsheet_created = order.get("spreadsheet_created", False)
    if spreadsheet_created:
        return {"google_sheet_url": order.get("spreadsheet_url", "")}

    sort_stage = {"brand": 1, "rate": 1, "name": 1}
    if sort == "price_asc":
        sort_stage = {"rate": 1}
    elif sort == "price_desc":
        sort_stage = {"rate": -1}
    elif sort == "catalogue":
        sort_stage = {"catalogue_order": 1}

    products = get_active_products(sort_stage)
    brands = {}
    for product in products:
        brands.setdefault(product.get("brand", "Unknown"), []).append(product)

    # Enhanced retry function with exponential backoff for SSL errors
    async def execute_with_robust_retry(func, max_retries=5, initial_delay=1):
        retries = 0
        last_exception = None

        while retries < max_retries:
            try:
                return await asyncio.to_thread(func)
            except (HttpError, ConnectionError, TimeoutError) as e:
                last_exception = e
                wait_time = initial_delay * (2**retries)  # Exponential backoff
                print(
                    f"Connection error. Retry {retries + 1}/{max_retries} after {wait_time}s: {str(e)}"
                )
                await asyncio.sleep(wait_time)
                retries += 1

        # If we get here, all retries failed
        raise last_exception

    try:
        # Get services with robust retry
        try:
            service = await execute_with_robust_retry(get_sheets_service)
            drive_service = await execute_with_robust_retry(get_drive_service)
        except Exception as e:
            print(f"Failed to initialize services after retries: {e}")
            raise HTTPException(
                status_code=503, detail="Google services temporarily unavailable"
            )

        # Create spreadsheet
        try:
            spreadsheet = await execute_with_robust_retry(
                lambda: service.spreadsheets()
                .create(body={"properties": {"title": "Order Form"}})
                .execute()
            )
            spreadsheet_id = spreadsheet["spreadsheetId"]
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        except Exception as e:
            print(f"Failed to create spreadsheet: {e}")
            raise HTTPException(status_code=500, detail="Failed to create spreadsheet")

        # Update DB
        db.orders.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"spreadsheet_created": True, "spreadsheet_url": sheet_url}},
        )

        # Get customer data
        customer = db.customers.find_one({"_id": ObjectId(customer_id)})
        special_margins = {
            str(sm["product_id"]): sm["margin"]
            for sm in db.special_margins.find({"customer_id": ObjectId(customer_id)})
        }

        # Get spreadsheet details
        try:
            spreadsheet_details = await execute_with_robust_retry(
                lambda: service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id)
                .execute()
            )
            existing_sheets = {
                sheet["properties"]["title"]: sheet["properties"]["sheetId"]
                for sheet in spreadsheet_details["sheets"]
            }
            sheet1_id = existing_sheets.get("Sheet1")
        except Exception as e:
            print(f"Failed to get spreadsheet details: {e}")
            # Continue with the process, we can still try to add sheets

        # Sort brands alphabetically
        sorted_brands = sorted(brands.keys())

        # Add sheets in smaller batches to reduce connection load
        brand_sheets = {}
        for brand in sorted_brands:
            if brand in existing_sheets:
                brand_sheets[brand] = existing_sheets[brand]

        # Process brands in chunks to reduce API load
        chunk_size = 5  # Process 5 brands at a time
        for i in range(0, len(sorted_brands), chunk_size):
            chunk_brands = sorted_brands[i : i + chunk_size]
            chunk_requests = []

            for brand in chunk_brands:
                if brand not in brand_sheets:
                    chunk_requests.append(
                        {"addSheet": {"properties": {"title": brand}}}
                    )

            if chunk_requests:
                try:
                    batch_response = await execute_with_robust_retry(
                        lambda: service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={"requests": chunk_requests},
                        )
                        .execute()
                    )

                    for j, brand in enumerate(
                        [b for b in chunk_brands if b not in brand_sheets]
                    ):
                        brand_sheets[brand] = batch_response["replies"][j]["addSheet"][
                            "properties"
                        ]["sheetId"]
                except Exception as e:
                    print(f"Error adding sheets for chunk {i//chunk_size + 1}: {e}")
                    # Continue with sheets we successfully created

        # Process data in chunks
        for i in range(0, len(sorted_brands), chunk_size):
            chunk_brands = sorted_brands[i : i + chunk_size]
            data_updates = []
            format_requests = []

            for brand in chunk_brands:
                if brand not in brand_sheets:
                    continue  # Skip brands without sheets

                sheet_id = brand_sheets[brand]
                products_in_brand = brands[brand]
                rows = [
                    [
                        "Image",
                        "Name",
                        "Sub Category",
                        "Series",
                        "SKU",
                        "Price",
                        "Margin",
                        "Selling Price",
                        "Quantity",
                        "Total",
                    ]
                ]

                for idx, product in enumerate(products_in_brand, start=2):
                    image_url = product.get("image_url", "")
                    margin = special_margins.get(
                        str(product.get("_id")), customer.get("cf_margin", "40%")
                    )
                    margin_value = int(margin.replace("%", "")) / 100

                    rows.append(
                        [
                            f'=IMAGE("{image_url}", 1)',
                            product.get("name", ""),
                            product.get("sub_category", ""),
                            product.get("series", ""),
                            product.get("cf_sku_code", ""),
                            product.get("rate", 0),
                            margin,
                            product.get("rate", 0) * margin_value,
                            "",
                            f"=I{idx}*H{idx}",
                        ]
                    )

                data_updates.append(
                    {"range": f"{brand}!A1:J{len(rows)}", "values": rows}
                )
                rows_count = len(products_in_brand) + 1

                # Add formatting requests for this brand
                format_requests.extend(
                    [
                        # Bold headers
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 10,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "horizontalAlignment": "CENTER",
                                        "verticalAlignment": "MIDDLE",
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment)",
                            }
                        },
                        # Table borders
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": rows_count,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 10,
                                },
                                "top": {"style": "SOLID"},
                                "bottom": {"style": "SOLID"},
                                "left": {"style": "SOLID"},
                                "right": {"style": "SOLID"},
                                "innerHorizontal": {"style": "SOLID"},
                                "innerVertical": {"style": "SOLID"},
                            }
                        },
                        # Column widths
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 0,
                                    "endIndex": 1,
                                },
                                "properties": {"pixelSize": 120},
                                "fields": "pixelSize",
                            }
                        },
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 1,
                                    "endIndex": 2,
                                },
                                "properties": {"pixelSize": 250},
                                "fields": "pixelSize",
                            }
                        },
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 2,
                                    "endIndex": 4,
                                },
                                "properties": {"pixelSize": 150},
                                "fields": "pixelSize",
                            }
                        },
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 4,
                                    "endIndex": 10,
                                },
                                "properties": {"pixelSize": 120},
                                "fields": "pixelSize",
                            }
                        },
                        # Row height
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "ROWS",
                                    "startIndex": 0,
                                    "endIndex": rows_count,
                                },
                                "properties": {"pixelSize": 100},
                                "fields": "pixelSize",
                            }
                        },
                    ]
                )

            # Update data for this chunk if we have any
            if data_updates:
                try:
                    await execute_with_robust_retry(
                        lambda: service.spreadsheets()
                        .values()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={
                                "valueInputOption": "USER_ENTERED",
                                "data": data_updates,
                            },
                        )
                        .execute()
                    )
                except Exception as e:
                    print(f"Error updating data for chunk {i//chunk_size + 1}: {e}")

            # Apply formatting for this chunk if we have any
            if format_requests:
                try:
                    await execute_with_robust_retry(
                        lambda: service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={"requests": format_requests},
                        )
                        .execute()
                    )
                except Exception as e:
                    print(
                        f"Error applying formatting for chunk {i//chunk_size + 1}: {e}"
                    )

        # Delete Sheet1 if it exists - attempt multiple times
        if sheet1_id is not None:
            for attempt in range(3):  # Try 3 times to delete Sheet1
                try:
                    await execute_with_robust_retry(
                        lambda: service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={
                                "requests": [{"deleteSheet": {"sheetId": sheet1_id}}]
                            },
                        )
                        .execute()
                    )
                    break  # If successful, break out of the retry loop
                except Exception as e:
                    print(f"Error deleting Sheet1 (attempt {attempt+1}/3): {e}")
                    if attempt == 2:  # If this was the last attempt
                        print("Failed to delete Sheet1 after multiple attempts")

        # Set permissions
        try:
            await execute_with_robust_retry(
                lambda: drive_service.permissions()
                .create(
                    fileId=spreadsheet_id, body={"type": "anyone", "role": "writer"}
                )
                .execute()
            )
        except Exception as e:
            print(f"Error setting permissions: {e}")
            # We'll return the URL anyway since the sheet is created

        return {"google_sheet_url": sheet_url}

    except Exception as err:
        print(f"Unexpected error: {err}")
        raise HTTPException(
            status_code=500, detail=f"Error creating Google Sheet: {str(err)}"
        )


@router.get("/update_cart")
async def update_order_from_sheet(order_id: str):
    # Fetch order details from DB
    order = db.orders.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    spreadsheet_url = order.get("spreadsheet_url", "")
    if not spreadsheet_url:
        raise HTTPException(
            status_code=400, detail="No spreadsheet associated with order"
        )

    try:
        spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
    except IndexError:
        raise HTTPException(status_code=400, detail="Invalid spreadsheet URL")

    try:
        service = get_sheets_service()

        # Get all sheets in the spreadsheet
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_titles = [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]

        updated_products = []

        for sheet_title in sheet_titles:
            sheet_data = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=f"{sheet_title}!A1:J")
                .execute()
            )

            values = sheet_data.get("values", [])
            if not values or len(values) < 2:
                continue  # Skip empty sheets

            headers = values[0]
            col_indices = {
                "image_url": headers.index("Image") if "Image" in headers else -1,
                "name": headers.index("Name") if "Name" in headers else -1,
                "sub_category": (
                    headers.index("Sub Category") if "Sub Category" in headers else -1
                ),
                "series": headers.index("Series") if "Series" in headers else -1,
                "sku": headers.index("SKU") if "SKU" in headers else -1,
                "price": headers.index("Price") if "Price" in headers else -1,
                "margin": headers.index("Margin") if "Margin" in headers else -1,
                "selling_price": (
                    headers.index("Selling Price") if "Selling Price" in headers else -1
                ),
                "quantity": headers.index("Quantity") if "Quantity" in headers else -1,
                "total": headers.index("Total") if "Total" in headers else -1,
            }

            # Ensure required fields exist
            if col_indices["sku"] == -1 or col_indices["quantity"] == -1:
                continue

            for row in values[1:]:
                if (
                    len(row) > col_indices["quantity"]
                    and row[col_indices["quantity"]].strip()
                ):
                    try:
                        quantity = int(row[col_indices["quantity"]].strip())
                        if quantity > 0:
                            product_sku = row[col_indices["sku"]]
                            product = db.products.find_one(
                                {"cf_sku_code": product_sku, "status": "active"}
                            )

                            if product:
                                updated_products.append(
                                    {
                                        "product_id": ObjectId(product["_id"]),
                                        "tax_percentage": 18,  # Adjust based on product data if available
                                        "brand": product.get("brand", "Unknown"),
                                        "product_code": product_sku,
                                        "quantity": quantity,
                                        "name": row[col_indices["name"]],
                                        "image_url": product.get("image_url"),
                                        "margin": row[col_indices["margin"]],
                                        "price": float(product.get("rate", 0)),
                                        "added_by": "sales_person",
                                    }
                                )
                    except ValueError:
                        continue  # Ignore invalid quantity values
        if not updated_products:
            raise HTTPException(
                status_code=400, detail="No products with quantities found"
            )
        print(json.dumps(serialize_mongo_document(updated_products), indent=4))

        # Update the order in the database
        db.orders.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"products": updated_products, "updated_from_sheet": True}},
        )

        return {
            "message": "Order updated successfully",
            "updated_products": serialize_mongo_document(updated_products),
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(
            status_code=500, detail=f"Error reading spreadsheet: {str(e)}"
        )


# Get an order by ID
@router.get("/{order_id}")
def read_order(order_id: str):
    """
    Retrieve an order by its ID.
    """
    order = get_order(order_id, orders_collection)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


# Get all orders
@router.get("")
def read_all_orders(role: str = "salesperson", created_by: str = "", status: str = ""):
    """
    Retrieve all orders.
    If role is 'admin', return all orders.
    If role is 'salesperson', return only orders created by the specified user.
    """
    orders = get_all_orders(
        role, created_by, status, orders_collection, users_collection
    )
    return orders


# Update an order
@router.put("/{order_id}")
def update_existing_order(order_id: str, order_update: dict):
    """
    Update an existing order with raw dictionary data.
    """
    update_order(order_id, order_update, orders_collection, customers_collection)
    updated_order = get_order(order_id, orders_collection)
    if not updated_order:
        raise HTTPException(status_code=404, detail="Order not found")
    return updated_order


# Delete an order
@router.delete("/clear/{user_id}")
def clear_existing_order(user_id: str):
    """
    Deletes all orders by a given user who has created it if there is no customer information
    """
    clear_empty_orders(user_id, orders_collection)
    return {"detail": "Orders deleted successfully"}


@router.delete("/{order_id}")
def delete_existing_order(order_id: str):
    """
    Deletes all orders by a given user who has created it if there is no customer information
    """
    try:
        delete_order(order_id, orders_collection)
        return {"detail": "Orders deleted successfully"}
    except Exception as e:
        raise e


# Update an order
@router.put("/clear/{order_id}")
def clear_order_cart(order_id: str):
    """
    Update an existing order with raw dictionary data.
    """
    updated_order = clear_cart(order_id, orders_collection)
    return updated_order


# Finalise an order (Create Estimate)
@router.post("/finalise")
async def finalise(order_dict: dict):
    """
    finalise an existing order
    """
    order_id = order_dict.get("order_id")
    status = str(order_dict.get("status")).lower()
    try:
        # Perform order validation
        validate_order(order_id)
    except HTTPException as e:
        # Return validation error message if validation fails
        return {"status": "error", "message": e.detail}
    order = db.orders.find_one({"_id": ObjectId(order_id)})
    estimate_created = order.get("estimate_created", False)
    estimate_id = order.get("estimate_id", "")
    shipping_address_id = order.get("shipping_address", {}).get("address_id", "")
    billing_address_id = order.get("billing_address", {}).get("address_id", "")
    customer = db.customers.find_one({"_id": ObjectId(order.get("customer_id"))})
    state_str = str(order.get("shipping_address", {}).get("state", ""))
    place_of_supply = STATE_CODES.get(state_str.title())
    gst_type = order.get("gst_type", "")
    products = order.get("products", [])
    total_amount = order.get("total_amount")
    created_by = order.get("created_by")
    user = users_collection.find_one({"_id": ObjectId(created_by)})
    reference_number = order.get("reference_number", "")
    # Fetch SPecial Margins
    customer_id = order.get("customer_id")
    special_margins_cursor = db.special_margins.find(
        {"customer_id": ObjectId(customer_id)}
    )
    special_margin_dict = {
        str(sm["product_id"]): sm["margin"] for sm in special_margins_cursor
    }

    line_items = []
    for idx, product in enumerate(products):
        item = db.products.find_one({"_id": ObjectId(product.get("product_id"))})
        product_id_str = str(
            product.get("product_id")
        )  # Convert to string for dictionary lookup
        # Retrieve the special margin if it exists; otherwise, use the product's default margin
        special_margin = special_margin_dict.get(
            product_id_str, customer.get("cf_margin", "40%")
        )
        discount_value = special_margin
        if not discount_value.endswith("%"):
            discount_value = f"{discount_value}%"
        obj = {
            "item_order": idx + 1,
            "item_id": item.get("item_id"),
            "rate": item.get("rate"),
            "name": item.get("name"),
            "description": f"SOH:{item.get('stock')}",
            "quantity": product.get("quantity"),
            "discount": discount_value,
            "tax_id": (
                item.get("item_tax_preferences", [{}])[1].get("tax_id", 0)
                if place_of_supply == "MH" or place_of_supply == ""
                else item.get("item_tax_preferences", [{}])[0].get("tax_id", 0)
            ),
            "tags": [],
            "tax_exemption_code": "",
            "item_custom_fields": [
                {"label": "Manufacturer Code", "value": item.get("cf_item_code")},
                {"label": "SKU Code", "value": item.get("cf_sku_code")},
            ],
            "hsn_or_sac": item.get("hsn_or_sac"),
            "gst_treatment_code": "",
            "unit": "pcs",
            "unit_conversion_id": "",
        }
        line_items.append(obj)

    headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
    message = ""
    estimate_data = {}

    if not estimate_created:
        async with httpx.AsyncClient(timeout=timeout) as client:
            y = await client.get(
                url=ESTIMATE_URL.format(org_id=org_id)
                + "&filter_by=Status.All&per_page=200&sort_column=estimate_number&sort_order=D",
                headers=headers,
            )
            y.raise_for_status()
            last_estimate_number = str(
                y.json()["estimates"][0]["estimate_number"]
            ).split("/")
            new_estimate_number = f"{last_estimate_number[0]}/{last_estimate_number[1]}/{int(last_estimate_number[-1]) + 1}"
            # Prepare the request payload
            payload = {
                "estimate_number": new_estimate_number,
                "location_id": "3220178000143298047",
                "contact_persons": [],
                "customer_id": customer.get("contact_id"),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "expiry_date": "",
                "notes": "Looking forward for your business.",
                "terms": terms,
                "line_items": line_items,
                "custom_fields": [],
                "is_inclusive_tax": False if gst_type == "Exclusive" else True,
                "is_discount_before_tax": "",
                "discount": 0,
                "discount_type": "item_level",
                "adjustment": "",
                "adjustment_description": "Adjustment",
                "tax_exemption_code": "",
                "tax_authority_name": "",
                "pricebook_id": "",
                "salesperson_id": user.get("salesperson_id", ""),
                # "template_id": "3220178000000075080",
                "payment_options": {"payment_gateways": []},
                "documents": [],
                "mail_attachments": [],
                "billing_address_id": billing_address_id,
                "shipping_address_id": shipping_address_id,
                "dispatch_from_address_id": "3220178000177830244",
                "project_id": "",
                "gst_treatment": customer.get("gst_treatment"),
                "gst_no": customer.get("gst_no", ""),
                "place_of_supply": place_of_supply,
                "is_tcs_amount_in_percent": True,
                "client_computation": {"total": total_amount},
                "reference_number": reference_number,
            }
            estimate_response = await client.post(
                url=ESTIMATE_URL.format(org_id=org_id)
                + "&ignore_auto_number_generation=true",
                headers=headers,
                json=payload,
            )
            estimate_response.raise_for_status()

            estimate_data = estimate_response.json()["estimate"]
            estimate_id = estimate_data.get("estimate_id")
            estimate_number = estimate_data.get("estimate_number")
            estimate_url = estimate_data.get("estimate_url")
            db.estimates.insert_one(
                {
                    **estimate_data,
                    "order_id": ObjectId(order_id),
                }
            )
            db.orders.update_one(
                {"_id": ObjectId(order_id)},
                {
                    "$set": {
                        "status": status,
                        "estimate_created": True,
                        "estimate_id": estimate_id,
                        "estimate_number": estimate_number,
                        "estimate_url": estimate_url,
                    }
                },
            )
            message = f"Estimate has been created - {estimate_data['estimate_number']} with Status : {str(status).capitalize()}\n"
    else:
        async with httpx.AsyncClient(timeout=timeout) as client:
            payload = {
                "location_id": "3220178000143298047",
                "contact_persons": [],
                "customer_id": customer.get("contact_id"),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "expiry_date": "",
                "notes": "Looking forward for your business.",
                "terms": terms,
                "line_items": line_items,
                "custom_fields": [],
                "is_inclusive_tax": False if gst_type == "Exclusive" else True,
                "is_discount_before_tax": "",
                "discount": 0,
                "discount_type": "item_level",
                "adjustment": "",
                "adjustment_description": "Adjustment",
                "tax_exemption_code": "",
                "tax_authority_name": "",
                "pricebook_id": "",
                "salesperson_id": user.get("salesperson_id", ""),
                # "template_id": "3220178000000075080",
                "payment_options": {"payment_gateways": []},
                "documents": [],
                "mail_attachments": [],
                "billing_address_id": billing_address_id,
                "shipping_address_id": shipping_address_id,
                "dispatch_from_address_id": "3220178000177830244",
                "project_id": "",
                "gst_treatment": customer.get("gst_treatment"),
                "gst_no": customer.get("gst_no", ""),
                "place_of_supply": place_of_supply,
                "is_tcs_amount_in_percent": True,
                "client_computation": {"total": total_amount},
                "reference_number": reference_number,
            }

            y = await client.put(
                url=f"https://books.zoho.com/api/v3/estimates/{estimate_id}?organization_id={org_id}",
                headers=headers,
                json=payload,
            )
            y.raise_for_status()
            estimate_data = y.json()["estimate"]
            estimate_id = estimate_data.get("estimate_id")
            estimate_number = estimate_data.get("estimate_number")
            estimate_url = estimate_data.get("estimate_url")
            message = f"Estimate has been updated - {estimate_number} with Status : {str(status).capitalize()}\n"
            update_fields = {
                "status": f"{str(status).capitalize()}",
                "estimate_created": True,
                "estimate_id": estimate_id,
                "estimate_number": estimate_number,
                "estimate_url": estimate_url,
            }

            db.orders.update_one(
                {"_id": ObjectId(order_id)},
                {"$set": update_fields},
            )
    await email_estimate(
        status,
        order_id,
        estimate_data["estimate_id"],
        estimate_data["estimate_number"],
        estimate_data["estimate_url"],
        message,
        headers,
        timeout,
    )
    return {"status": "success", "message": message}


@router.get("/download_pdf/{order_id}")
async def download_pdf(order_id: str = ""):
    try:
        # Check if the order exists in the database
        order = db.orders.find_one(
            {"_id": ObjectId(order_id), "estimate_created": True}
        )
        if order is None:
            raise HTTPException(status_code=404, detail="Draft Estimate Not Created")

        # Get the estimate_id and make the request to Zoho
        estimate_id = order.get("estimate_id", "")
        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        response = requests.get(
            url=PDF_URL.format(org_id=org_id, estimate_id=estimate_id),
            headers=headers,
            allow_redirects=False,  # Prevent automatic redirects
        )

        # Check if the response from Zoho is successful (200)
        if response.status_code == 200:
            # Return the PDF content
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename=order_{order_id}.pdf"
                },
            )
        elif response.status_code == 307:
            raise HTTPException(
                status_code=307,
                detail="Redirect encountered. Check Zoho endpoint or token.",
            )
        else:
            # Raise an exception if Zoho's API returns an error
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch PDF: {response.text}",
            )

    except HTTPException as e:
        print(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/notify")
async def notify(order_dict: dict):
    try:
        order_id = order_dict.get("order_id", "")
        if not order_id:
            raise HTTPException(status_code=404, detail="Order Id is neccesary")
        order = db.orders.find_one({"_id": ObjectId(order_id)})
        customer_name = order.get("customer_name")
        estimate_created = order.get("estimate_created", False)
        estimate_number = order.get("estimate_number", False)
        created_by = order.get("created_by", "")
        sales_person = db.users.find_one({"_id": ObjectId(created_by)})
        sales_person_phone = sales_person.get("phone")
        salesperson_name = sales_person.get("name")
        template = db.templates.find_one({"name": "customer_order_edit"})
        template_doc = {**template}
        params = {
            "salesperson_name": salesperson_name,
            "customer_name": customer_name,
            "estimate_number": estimate_number if estimate_created else order_id[-6:],
            "button_url": f"{order_id}",
        }
        for item in [
            {"name": salesperson_name, "phone": sales_person_phone},
            {
                "name": os.getenv("NOTIFY_NUMBER_TO_CC4_NAME"),
                "phone": os.getenv("NOTIFY_NUMBER_TO_CC4"),
            },
            {
                "name": os.getenv("NOTIFY_NUMBER_TO_CC5_NAME"),
                "phone": os.getenv("NOTIFY_NUMBER_TO_CC5"),
            },
        ]:
            params["salesperson_name"] = item["name"]
            send_whatsapp(to=item["phone"], template_doc=template_doc, params=params)
        return
    except Exception as e:
        raise e


@router.post("/duplicate_order")
async def duplicate_order(order_dict: dict):
    try:
        order_id = order_dict.get("order_id", "")
        if not order_id:
            raise HTTPException(status_code=404, detail="Order Id is neccesary")
        order = db.orders.find_one({"_id": ObjectId(order_id)})
        order["created_at"] = datetime.now()
        order["updated_at"] = datetime.now()
        order["status"] = "draft"
        order.pop("_id")
        if "estimate_created" in order.keys():
            order.pop("estimate_created")
            order.pop("estimate_number")
            order.pop("estimate_id")
            order.pop("estimate_url")
        result = db.orders.insert_one(order)
        return str(result.inserted_id)
    except Exception as e:
        raise e
