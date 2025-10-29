from types import NoneType
from pymongo.collection import Collection
from datetime import datetime
from typing import List, Dict, Tuple
from .helpers import get_access_token
from fastapi import APIRouter, HTTPException
from config.root import connect_to_mongo, serialize_mongo_document
from bson.objectid import ObjectId
import time, os, httpx, requests, asyncio, ssl, socket
from dotenv import load_dotenv
from fastapi.responses import Response
from config.constants import terms, STATE_CODES 
from config.whatsapp import send_whatsapp 
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pymongo import DESCENDING, ASCENDING
from pathlib import Path
from collections import defaultdict

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
# Rate limiting to prevent API quota exhaustion
class APIRateLimiter:
    def __init__(self, max_calls_per_minute=100):
        self.max_calls = max_calls_per_minute
        self.calls = defaultdict(list)
    
    async def wait_if_needed(self, key="default"):
        """Wait if rate limit would be exceeded"""
        now = time.time()
        minute_ago = now - 60
        
        # Clean old calls
        self.calls[key] = [call_time for call_time in self.calls[key] if call_time > minute_ago]
        
        # Check if we need to wait
        if len(self.calls[key]) >= self.max_calls:
            sleep_time = 60 - (now - self.calls[key][0])
            if sleep_time > 0:
                print(f"⏱️ Rate limiting: waiting {sleep_time:.1f} seconds")
                await asyncio.sleep(sleep_time)
        
        # Record this call
        self.calls[key].append(now)

# Global rate limiter
rate_limiter = APIRateLimiter(max_calls_per_minute=100)

def create_fresh_sheets_service():
    """Create a new service instance to avoid threading/memory issues"""
    credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)
async def robust_api_call(operation_name: str, func, max_retries: int = 3):
    """
    Execute Google API calls with robust error handling for SSL and connection issues
    """
    # Apply rate limiting
    await rate_limiter.wait_if_needed()
    
    for attempt in range(max_retries):
        try:
            result = await asyncio.to_thread(func)
            print(f"✓ {operation_name} succeeded")
            return result, None
        except (ssl.SSLError, socket.error, ConnectionError) as e:
            if attempt < max_retries - 1:
                wait_time = min((attempt + 1) * 2, 10)  # Cap at 10 seconds
                print(f"🔄 {operation_name} SSL/Connection error (attempt {attempt + 1}), retrying in {wait_time}s: {type(e).__name__}")
                await asyncio.sleep(wait_time)
                continue
            else:
                error_msg = f"{operation_name} failed after {max_retries} attempts: {str(e)}"
                print(f"✗ {error_msg}")
                return None, error_msg
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit
                wait_time = 60
                print(f"⏱️ {operation_name} rate limited, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                if attempt < max_retries - 1:
                    continue
            error_msg = f"{operation_name} HTTP error: {e.resp.status} - {str(e)}"
            print(f"✗ {error_msg}")
            return None, error_msg
        except Exception as e:
            error_msg = f"{operation_name} unexpected error: {str(e)}"
            print(f"✗ {error_msg}")
            return None, error_msg
    
    return None, f"{operation_name} failed after all retries"

async def safe_execute(operation_name: str, func, critical: bool = True):
    """
    Execute an operation with proper error handling
    critical=True: Will raise HTTPException if fails
    critical=False: Will log error but continue
    """
    result, error = await robust_api_call(operation_name, func)
    
    if error and critical:
        raise HTTPException(status_code=500, detail=error)
    
    return result, error
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
        query["$or"] = [
            {"total_amount": {"$gte": 0}},
            {"spreadsheet_created": True}
        ]
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
    if "created_by" in order_update:
        order_update["created_by"] = ObjectId(order_update.get("created_by"))
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


import asyncio
import json
import traceback
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

from fastapi import HTTPException
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from bson import ObjectId


BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT_FILE = BASE_DIR / "creds.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Global service instances
_sheets_service = None
_drive_service = None

def get_sheets_service():
    global _sheets_service
    if _sheets_service is None:
        credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        _sheets_service = build("sheets", "v4", credentials=credentials)
    return _sheets_service

def get_drive_service():
    global _drive_service
    if _drive_service is None:
        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        _drive_service = build("drive", "v3", credentials=credentials)
    return _drive_service

async def safe_execute(operation_name: str, func, critical: bool = True):
    """
    Execute an operation with proper error handling
    critical=True: Will raise HTTPException if fails
    critical=False: Will log error but continue
    """
    try:
        result = await asyncio.to_thread(func)
        print(f"✓ {operation_name} succeeded")
        return result, None
    except HttpError as e:
        error_msg = f"{operation_name} failed: HTTP {e.resp.status} - {str(e)}"
        print(f"✗ {error_msg}")
        if critical:
            raise HTTPException(status_code=500, detail=error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"{operation_name} failed: {str(e)}"
        print(f"✗ {error_msg}")
        if critical:
            raise HTTPException(status_code=500, detail=error_msg)
        return None, error_msg

def prepare_brand_data(brands: Dict, customer: Dict, special_margins: Dict) -> Dict[str, List]:
    """Prepare all data upfront"""
    brand_data = {}
    
    for brand_name, products in brands.items():
        rows = [[
            "Image", "Name", "Sub Category", "Series", "SKU", 
            "Stock", "UPC/EAN Code", "Price", "Margin", "Selling Price", 
            "Quantity", "Total"
        ]]
        
        for idx, product in enumerate(products, start=2):
            margin = special_margins.get(
                str(product.get("_id")), 
                customer.get("cf_margin", "40%")
            )
            try:
                margin_value = int(margin.replace("%", "")) / 100
            except:
                margin_value = 0.4
                
            rate = product.get("rate", 0)
            
            rows.append([
                f'=IMAGE("{product.get("image_url", "")}", 1)',
                product.get("name", ""),
                product.get("sub_category", ""),
                product.get("series", ""),
                product.get("cf_sku_code", ""),
                product.get("stock", ""),
                product.get("upc_code", ""),
                rate,
                margin,
                rate * margin_value,
                "",
                f"=K{idx}*H{idx}"
            ])
        
        brand_data[brand_name] = rows
    
    return brand_data

def create_format_requests(sheet_id: int, rows_count: int) -> List[Dict]:
    """Create formatting requests for a sheet"""
    return [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 12,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                    }
                },
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor)",
            }
        },
        {
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": rows_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": 12,
                },
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1,
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
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": rows_count,
                },
                "properties": {"pixelSize": 80},
                "fields": "pixelSize",
            }
        },
    ]

async def try_set_permissions(drive_service, spreadsheet_id: str, user_email: Optional[str] = None) -> Dict[str, str]:
    """Try different permission methods and return status"""
    permission_status = {}
    
    # Method 1: Anyone with link (most important)
    result, error = await safe_execute(
        "Setting 'anyone with link' permissions",
        lambda: drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={"type": "anyone", "role": "writer"},
            sendNotificationEmail=False
        ).execute(),
        critical=False
    )
    permission_status["anyone_with_link"] = "success" if result else f"failed: {error}"
    
    # Method 2: Specific user if provided
    if user_email:
        result, error = await safe_execute(
            f"Setting user permissions for {user_email}",
            lambda: drive_service.permissions().create(
                fileId=spreadsheet_id,
                body={"type": "user", "role": "writer", "emailAddress": user_email},
                sendNotificationEmail=False
            ).execute(),
            critical=False
        )
        permission_status["user_specific"] = "success" if result else f"failed: {error}"
    
    # Method 3: Try to make it publicly viewable as fallback
    if permission_status["anyone_with_link"].startswith("failed"):
        result, error = await safe_execute(
            "Setting public view permissions",
            lambda: drive_service.permissions().create(
                fileId=spreadsheet_id,
                body={"type": "anyone", "role": "reader"},
                sendNotificationEmail=False
            ).execute(),
            critical=False
        )
        permission_status["public_readonly"] = "success" if result else f"failed: {error}"
    
    return permission_status

@router.get("/download_order_form")
async def download_order_form(customer_id: str, order_id: str, sort: str = "default", user_email: Optional[str] = None):
    """
    Create order form spreadsheet with robust error handling
    user_email: Optional email to grant specific access to
    """
    
    if not customer_id or not order_id:
        raise HTTPException(status_code=400, detail="Customer and Order IDs are required")

    # Check existing order
    order = db.orders.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order.get("spreadsheet_created", False):
        return {"google_sheet_url": order.get("spreadsheet_url", "")}

    # Track operations for detailed response
    operations_status = {}
    sheet_url = None
    spreadsheet_id = None
    
    try:
        print(f"Starting order form creation for order {order_id}")
        
        # Step 1: Get all required data (CRITICAL)
        print("📊 Fetching data...")
        
        customer_task = asyncio.create_task(
            asyncio.to_thread(db.customers.find_one, {"_id": ObjectId(customer_id)})
        )
        
        sort_stage = {"brand": 1, "rate": 1, "name": 1}
        if sort == "price_asc":
            sort_stage = {"rate": 1}
        elif sort == "price_desc":
            sort_stage = {"rate": -1}
        elif sort == "catalogue":
            sort_stage = {"catalogue_order": 1}
            
        products_task = asyncio.create_task(
            asyncio.to_thread(get_active_products, sort_stage)
        )
        
        special_margins_task = asyncio.create_task(
            asyncio.to_thread(
                lambda: {
                    str(sm["product_id"]): sm["margin"]
                    for sm in db.special_margins.find({"customer_id": ObjectId(customer_id)})
                }
            )
        )
        
        customer, products, special_margins = await asyncio.gather(
            customer_task, products_task, special_margins_task
        )
        
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
            
        brands = {}
        for product in products:
            brand = product.get("brand", "Unknown")
            brands.setdefault(brand, []).append(product)
            
        if not brands:
            raise HTTPException(status_code=404, detail="No active products found")

        operations_status["data_fetch"] = f"✓ Loaded {len(products)} products in {len(brands)} brands"
        
        # Step 2: Create spreadsheet (CRITICAL)
        print("📝 Creating spreadsheet...")
        
        sheets_service = get_sheets_service()
        drive_service = get_drive_service()
        
        spreadsheet_body = {
            "properties": {
                "title": f"Order Form - {customer.get('display_name', 'Customer')} - {order_id[:8]}"
            }
        }
        
        spreadsheet, error = await safe_execute(
            "Creating spreadsheet",
            lambda: sheets_service.spreadsheets().create(body=spreadsheet_body).execute(),
            critical=True
        )
        
        spreadsheet_id = spreadsheet["spreadsheetId"]
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        operations_status["spreadsheet_creation"] = "✓ Spreadsheet created successfully"
        
        # Update DB immediately after successful creation
        db.orders.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"spreadsheet_created": True, "spreadsheet_url": sheet_url}},
        )
        operations_status["database_update"] = "✓ Order updated in database"
        
        # Step 3: Create sheets and add data (SEMI-CRITICAL)
        print("📋 Adding sheets and data...")
        
        brand_data = prepare_brand_data(brands, customer, special_margins)
        sorted_brands = sorted(brand_data.keys())
        
        # Create sheets
        sheet_requests = [
            {"addSheet": {"properties": {"title": brand}}}
            for brand in sorted_brands
        ]
        sheet_requests.append({"deleteSheet": {"sheetId": 0}})  # Remove Sheet1
        
        batch_response, error = await safe_execute(
            "Creating brand sheets",
            lambda: sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": sheet_requests}
            ).execute(),
            critical=False
        )
        
        if batch_response:
            operations_status["sheets_creation"] = f"✓ Created {len(sorted_brands)} brand sheets"
            
            # Map brand names to sheet IDs
            brand_sheets = {}
            for i, brand in enumerate(sorted_brands):
                brand_sheets[brand] = batch_response["replies"][i]["addSheet"]["properties"]["sheetId"]
            
            # Add data to sheets
            data_updates = []
            all_format_requests = []
            
            for brand_name in sorted_brands:
                sheet_id = brand_sheets[brand_name]
                rows = brand_data[brand_name]
                
                data_updates.append({
                    "range": f"{brand_name}!A1:L{len(rows)}",
                    "values": rows
                })
                
                format_requests = create_format_requests(sheet_id, len(rows))
                all_format_requests.extend(format_requests)
            
            # Add data
            data_result, error = await safe_execute(
                "Adding data to sheets",
                lambda: sheets_service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "valueInputOption": "USER_ENTERED",
                        "data": data_updates,
                    },
                ).execute(),
                critical=False
            )
            
            if data_result:
                operations_status["data_population"] = "✓ Data added to all sheets"
            else:
                operations_status["data_population"] = f"✗ Data population failed: {error}"
            
            # Format sheets
            format_result, error = await safe_execute(
                "Formatting sheets",
                lambda: sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": all_format_requests},
                ).execute(),
                critical=False
            )
            
            if format_result:
                operations_status["formatting"] = "✓ Sheets formatted successfully"
            else:
                operations_status["formatting"] = f"✗ Formatting failed: {error}"
        else:
            operations_status["sheets_creation"] = f"✗ Failed to create sheets: {error}"
        
        # Step 4: Set permissions (NON-CRITICAL)
        print("🔐 Setting permissions...")
        
        permission_status = await try_set_permissions(drive_service, spreadsheet_id, user_email)
        operations_status["permissions"] = permission_status
        
        # Determine if we have any working access method
        has_access = any(status.startswith("success") for status in permission_status.values())
        
        response_data = {
            "google_sheet_url": sheet_url,
            "spreadsheet_id": spreadsheet_id,
            "status": "success",
            "operations": operations_status,
            "access_methods": permission_status,
            "message": f"Order form created with {len(sorted_brands)} brand sheets"
        }
        
        if not has_access:
            response_data["warning"] = "Sheet created but permission setting failed. You may need to manually share the sheet."
            response_data["manual_access_instructions"] = [
                f"1. Open {sheet_url}",
                "2. Click 'Share' button in top right",
                "3. Change 'Restricted' to 'Anyone with the link'",
                "4. Set permission to 'Editor'",
                "5. Click 'Done'"
            ]
        
        print("✅ Order form creation completed")
        return response_data
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Log the full error for debugging
        print(f"❌ Unexpected error: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        
        # If we have a sheet URL, return it with error details
        if sheet_url:
            return {
                "google_sheet_url": sheet_url,
                "spreadsheet_id": spreadsheet_id,
                "status": "partial_success",
                "error": str(e),
                "operations": operations_status,
                "message": "Sheet was created but some operations failed. Check the sheet manually."
            }
        else:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to create order form: {str(e)}"
            )
async def read_sheet_with_retry(service, spreadsheet_id: str, sheet_title: str, max_retries: int = 3, 
                               initial_delay: float = 1.0, backoff_factor: float = 2.0) -> Tuple[Optional[dict], Optional[str]]:
    """
    Read a single sheet with exponential backoff retry logic
    """
    last_error = None
    delay = initial_delay
    
    for attempt in range(max_retries):
        try:
            print(f"Reading sheet '{sheet_title}' (attempt {attempt + 1}/{max_retries})")
            
            result = await safe_execute(
                f"Reading sheet {sheet_title}",
                lambda: service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id, 
                    range=f"{sheet_title}!A1:L"
                ).execute(),
                critical=False
            )
            
            data, error = result
            if not error:
                print(f"✓ Successfully read sheet '{sheet_title}'")
                return data, None
            
            last_error = error
            print(f"✗ Attempt {attempt + 1} failed for sheet '{sheet_title}': {error}")
            
        except Exception as e:
            last_error = str(e)
            print(f"✗ Attempt {attempt + 1} failed for sheet '{sheet_title}': {e}")
        
        # Wait before retry (except on last attempt)
        if attempt < max_retries - 1:
            print(f"Waiting {delay:.1f}s before retry...")
            await asyncio.sleep(delay)
            delay *= backoff_factor
    
    return None, f"Failed after {max_retries} attempts: {last_error}"

async def read_sheets_batch(service, spreadsheet_id: str, sheet_titles: List[str], 
                           batch_size: int = 3, max_concurrent: int = 2) -> List[Tuple[str, Optional[dict], Optional[str]]]:
    """
    Read sheets in smaller batches to avoid overwhelming the API and network
    """
    results = []
    
    # Process sheets in batches
    for i in range(0, len(sheet_titles), batch_size):
        batch = sheet_titles[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {batch}")
        
        # Limit concurrent requests within each batch
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def read_with_semaphore(title):
            async with semaphore:
                data, error = await read_sheet_with_retry(service, spreadsheet_id, title)
                return title, data, error
        
        batch_tasks = [read_with_semaphore(title) for title in batch]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for result in batch_results:
            if isinstance(result, Exception):
                # Handle any unexpected exceptions
                results.append((f"unknown_sheet", None, str(result)))
            else:
                results.append(result)
        
        # Small delay between batches to be gentle on the API
        if i + batch_size < len(sheet_titles):
            await asyncio.sleep(0.5)
    
    return results
def process_sheet_data_fast(values: List[List], sheet_name: str, products_cache: Dict) -> tuple[List[Dict], List[str]]:
    """
    Optimized sheet data processing with pre-cached products
    """
    updated_products = []
    processing_errors = []
    
    if len(values) < 2:
        return updated_products, [f"Sheet {sheet_name}: No data rows found"]
        
    headers = values[0]
    
    # Find required columns
    try:
        sku_idx = headers.index("SKU")
        quantity_idx = headers.index("Quantity")
        name_idx = headers.index("Name") if "Name" in headers else -1
        margin_idx = headers.index("Margin") if "Margin" in headers else -1
    except ValueError as e:
        return updated_products, [f"Sheet {sheet_name}: Missing required columns - {str(e)}"]
    
    # Process rows in batches for better performance
    valid_rows = []
    for row_num, row in enumerate(values[1:], start=2):
        # Quick validation
        if (len(row) <= max(quantity_idx, sku_idx) or 
            not row[quantity_idx].strip() or 
            not row[sku_idx].strip()):
            continue
        
        try:
            quantity = int(float(row[quantity_idx].strip()))
            if quantity <= 0:
                continue
                
            valid_rows.append((row_num, row, quantity))
        except ValueError:
            processing_errors.append(f"Sheet {sheet_name} row {row_num}: Invalid quantity")
    
    # Process valid rows with cached products
    for row_num, row, quantity in valid_rows:
        try:
            product_sku = row[sku_idx].strip()
            
            # Use cached product lookup instead of database query
            product = products_cache.get(product_sku)
            if not product:
                processing_errors.append(f"Sheet {sheet_name} row {row_num}: Product not found for SKU '{product_sku}'")
                continue
            
            # Extract additional data safely
            product_name = row[name_idx] if name_idx >= 0 and len(row) > name_idx else ""
            margin = row[margin_idx] if margin_idx >= 0 and len(row) > margin_idx else ""
            
            # Create product entry
            updated_products.append({
                "product_id": ObjectId(product["_id"]),
                "tax_percentage": product.get("item_tax_preferences", [{}])[0].get("tax_percentage", 0),
                "brand": product.get("brand", "Unknown"),
                "product_code": product_sku,
                "quantity": quantity,
                "name": product_name or product.get("name", ""),
                "image_url": product.get("image_url"),
                "margin": margin,
                "price": float(product.get("rate", 0)),
                "added_by": "sales_person",
            })
            
        except Exception as e:
            processing_errors.append(f"Sheet {sheet_name} row {row_num}: Processing error - {str(e)}")
            continue
    
    return updated_products, processing_errors

async def fallback_individual_read(order_id: str, spreadsheet_id: str, sheet_titles: List[str], products_cache: Dict, service):
    """
    Fallback method: Read sheets individually if batch read fails
    """
    print("🔄 Using fallback individual read method")
    start_time = time.time()
    
    all_updated_products = []
    all_processing_errors = []
    successful_sheets = 0
    
    for sheet_title in sheet_titles:
        try:
            # Read individual sheet
            escaped_title = f"'{sheet_title}'" if " " in sheet_title or "'" in sheet_title else sheet_title
            sheet_data, error = await robust_api_call(
                f"Reading {sheet_title}",
                lambda: service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{escaped_title}!A1:L1000"
                ).execute()
            )
            
            if not sheet_data:
                all_processing_errors.append(f"Sheet {sheet_title}: {error}")
                continue
            
            values = sheet_data.get("values", [])
            if values:
                sheet_products, sheet_errors = process_sheet_data_fast(
                    values, sheet_title, products_cache
                )
                
                all_updated_products.extend(sheet_products)
                all_processing_errors.extend(sheet_errors)
                
                if sheet_products:
                    successful_sheets += 1
                    print(f"✅ {sheet_title}: {len(sheet_products)} products")
            
        except Exception as e:
            all_processing_errors.append(f"Sheet {sheet_title}: Exception - {str(e)}")
    
    if not all_updated_products:
        error_summary = "; ".join(all_processing_errors[:5])
        raise HTTPException(
            status_code=400, 
            detail=f"Fallback method failed - No valid products found. Errors: {error_summary}"
        )
    
    # Update database
    db.orders.update_one(
        {"_id": ObjectId(order_id)},
        {
            "$set": {
                "products": all_updated_products, 
                "updated_from_sheet": True,
                "last_sheet_update": datetime.now()
            }
        },
    )
    
    processing_time = round(time.time() - start_time, 2)
    
    return {
        "message": "Order updated successfully (fallback method)",
        "products_count": len(all_updated_products),
        "sheets_processed": len(sheet_titles),
        "successful_sheets": successful_sheets,
        "processing_time_seconds": processing_time,
        "method": "fallback_individual",
        "updated_products": serialize_mongo_document(all_updated_products),
        "warnings": all_processing_errors[:5] if all_processing_errors else []
    }

@router.get("/update_cart")
async def update_order_from_sheet(order_id: str):
    """
    Fast and reliable update order from spreadsheet using proper batch operations
    """
    
    # Validate order
    order = db.orders.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    spreadsheet_url = order.get("spreadsheet_url", "")
    if not spreadsheet_url:
        raise HTTPException(status_code=400, detail="No spreadsheet associated with order")

    # Extract spreadsheet ID
    try:
        spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
    except IndexError:
        raise HTTPException(status_code=400, detail="Invalid spreadsheet URL")

    print(f"🚀 Starting fast cart update for order {order_id}")
    start_time = time.time()
    
    try:
        service = create_fresh_sheets_service()
        
        # Step 1: Get spreadsheet info and product cache in parallel
        spreadsheet_task = robust_api_call(
            "Reading spreadsheet info",
            lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        )
        
        # Pre-load all active products into memory
        products_task = asyncio.create_task(
            asyncio.to_thread(
                lambda: {
                    product["cf_sku_code"]: product 
                    for product in db.products.find({"status": "active"})
                    if product.get("cf_sku_code")
                }
            )
        )
        
        spreadsheet_result, products_cache = await asyncio.gather(
            spreadsheet_task, products_task
        )
        
        spreadsheet_info, error = spreadsheet_result
        if not spreadsheet_info:
            raise HTTPException(status_code=500, detail=f"Cannot access spreadsheet: {error}")
        
        sheet_titles = [sheet["properties"]["title"] for sheet in spreadsheet_info["sheets"]]
        print(f"📊 Found {len(sheet_titles)} sheets, {len(products_cache)} products cached")
        
        # Step 2: Create proper ranges for batch reading
        ranges = []
        for title in sheet_titles:
            # Properly escape sheet names with spaces or special characters
            escaped_title = f"'{title}'" if " " in title or "'" in title else title
            ranges.append(f"{escaped_title}!A1:L1000")
        
        print(f"📋 Reading ranges: {ranges}")
        
        # Step 3: Batch read all sheets
        batch_data, error = await robust_api_call(
            "Batch reading all sheets",
            lambda: service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                majorDimension="ROWS"
            ).execute()
        )
        
        if not batch_data:
            raise HTTPException(status_code=500, detail=f"Failed to read sheets: {error}")
        
        print(f"📊 Batch data keys: {list(batch_data.keys())}")
        
        # Debug: Check what we actually got
        value_ranges = batch_data.get("valueRanges", [])
        print(f"📋 Got {len(value_ranges)} value ranges")
        
        if not value_ranges:
            # Fallback: Try reading sheets individually if batch failed
            print("⚠️ Batch read returned no data, falling back to individual reads")
            return await fallback_individual_read(order_id, spreadsheet_id, sheet_titles, products_cache, service)
        
        # Step 4: Process all sheets data efficiently
        all_updated_products = []
        all_processing_errors = []
        successful_sheets = 0
        
        for i, sheet_data in enumerate(value_ranges):
            sheet_title = sheet_titles[i] if i < len(sheet_titles) else f"Sheet_{i+1}"
            values = sheet_data.get("values", [])
            
            print(f"📖 Processing {sheet_title}: {len(values)} rows")
            
            if not values:
                all_processing_errors.append(f"Sheet {sheet_title}: No data found")
                continue
            
            # Process this sheet's data with pre-cached products
            sheet_products, sheet_errors = process_sheet_data_fast(
                values, sheet_title, products_cache
            )
            
            all_updated_products.extend(sheet_products)
            all_processing_errors.extend(sheet_errors)
            
            if sheet_products:
                successful_sheets += 1
                print(f"✅ {sheet_title}: Found {len(sheet_products)} products")
            else:
                print(f"⚠️ {sheet_title}: No valid products found")
        
        # Step 5: Validate results
        if not all_updated_products:
            error_summary = "; ".join(all_processing_errors[:3])
            print(f"❌ No products found. Errors: {error_summary}")
            print(f"📊 Debug info - Sheets processed: {len(value_ranges)}, Products cache size: {len(products_cache)}")
            
            # Additional debugging
            debug_info = {
                "sheets_found": len(sheet_titles),
                "value_ranges_returned": len(value_ranges),
                "products_cache_size": len(products_cache),
                "sample_sheet_data": {}
            }
            
            # Show sample data from first sheet for debugging
            if value_ranges and len(value_ranges) > 0:
                sample_values = value_ranges[0].get("values", [])
                debug_info["sample_sheet_data"] = {
                    "first_sheet_rows": len(sample_values),
                    "first_few_rows": sample_values[:3] if sample_values else []
                }
            
            raise HTTPException(
                status_code=400, 
                detail=f"No valid products found. Errors: {error_summary}. Debug: {debug_info}"
            )
        
        processing_time = round(time.time() - start_time, 2)
        print(f"⚡ Processed {len(all_updated_products)} products in {processing_time}s")
        
        # Step 6: Update database
        db.orders.update_one(
            {"_id": ObjectId(order_id)},
            {
                "$set": {
                    "products": all_updated_products, 
                    "updated_from_sheet": True,
                    "last_sheet_update": datetime.now()
                }
            },
        )
        
        # Step 7: Prepare response
        response = {
            "message": "Order updated successfully",
            "products_count": len(all_updated_products),
            "sheets_processed": len(sheet_titles),
            "successful_sheets": successful_sheets,
            "processing_time_seconds": processing_time,
            "updated_products": serialize_mongo_document(all_updated_products),
        }
        
        if all_processing_errors:
            response["warnings"] = all_processing_errors[:5]  # Limit warnings
            response["total_warnings"] = len(all_processing_errors)
        
        print(f"🎉 Cart update completed in {processing_time}s")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"💥 Error in update_order_from_sheet: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading spreadsheet: {str(e)}")

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
            if y.status_code != 200:
                return {"status": "error", "message": f"{y.json().get('message','')}"}
            last_estimate_number = str(
                y.json()["estimates"][0]["estimate_number"]
            ).split("/")
            new_last_part = str(int(last_estimate_number[-1]) + 1).zfill(
                len(last_estimate_number[-1])
            )
            # Reconstruct the estimate number
            new_estimate_number = (
                f"{last_estimate_number[0]}/{last_estimate_number[1]}/{new_last_part}"
            )
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
            print(estimate_response.json())

            # Check if the response contains an error
            response_json = estimate_response.json()
            # Zoho returns code: 0 for success, non-zero for errors
            if estimate_response.status_code != 201 or (response_json.get("code", 0) != 0):
                error_message = response_json.get("message", "Unknown error occurred")
                error_code = response_json.get("code", "")
                return {
                    "status": "error",
                    "message": error_message,
                    "error_code": error_code
                }

            estimate_data = response_json["estimate"]
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

            # Check if the response contains an error
            response_json = y.json()
            # Zoho returns code: 0 for success, non-zero for errors
            if y.status_code != 200 or (response_json.get("code", 0) != 0):
                error_message = response_json.get("message", "Unknown error occurred")
                error_code = response_json.get("code", "")
                return {
                    "status": "error",
                    "message": error_message,
                    "error_code": error_code
                }

            estimate_data = response_json["estimate"]
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
            return {"status": "error", "message": "Draft Estimate Not Created"}
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
