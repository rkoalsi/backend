from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    Query,
    File,
    UploadFile,
    Form,
    Depends,
    Request,
)
from fastapi.responses import JSONResponse, Response, StreamingResponse
from ..config.root import get_client, get_database, serialize_mongo_document
from bson.objectid import ObjectId
from .helpers import get_access_token, fetch_overdue_invoices, fetch_associated_credit_notes
from typing import Optional, List
import re, requests, os, json, time, boto3, io, csv, openpyxl
from dotenv import load_dotenv
from pytz import timezone as tz
from botocore.exceptions import BotoCoreError, NoCredentialsError
from datetime import date, timedelta, datetime, timezone
from .admin_trainings import router as admin_trainings_router
from .admin_catalogues import router as admin_catalogues_router
from .admin_salespeople import router as admin_salespeople_router
from .admin_special_margins import router as admin_special_margins_router
from .admin_inventory_aging import router as admin_inventory_aging_router
from .admin_announcements import router as admin_announcements_router
from .admin_daily_visits import router as admin_daily_visits_router
from .admin_hooks_categories import router as admin_hooks_categories_router
from .admin_hooks import router as admin_hooks_router
from .admin_potential_customers import router as admin_potential_customers_router
from .admin_expected_reorders import router as admin_expected_reorders_router
from .admin_targeted_customers import router as admin_targeted_customers_router
from .webhooks import update_stock_lock, run_update_stock
from .admin_return_orders import router as admin_return_orders_router
from .admin_sales_by_customer import router as admin_sales_by_customer_router
from .admin_external_links import router as admin_external_links_router
from .admin_linktree import router as admin_linktree_router
from .admin_business_cards import router as admin_business_cards_router
from .admin_customer_analytics import router as admin_customer_analytics_router
from .admin_order_analytics import router as admin_order_analytics_router
from .admin_catalogue_leads import router as admin_catalogue_leads_router
from .admin_brand_leads import router as admin_brand_leads_router
from .admin_b2b_registrations import router as admin_b2b_registrations_router
from .admin_attendance import router as admin_attendance_router
from .admin_users import router as admin_users_router
from .admin_careers import router as admin_careers_router
from .admin_career_applications import router as admin_career_applications_router
from .admin_contact_leads import router as admin_contact_submissions_router
from .admin_chats import router as admin_chats_router
from .admin_chats import contacts_router as admin_chatbot_customers_router
from .admin_templates import router as admin_templates_router
from .admin_segments import router as admin_segments_router
from .admin_campaigns import router as admin_campaigns_router
from ..config.auth import JWTBearer
import pandas as pd
from io import BytesIO
from pymongo.errors import OperationFailure
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, date
from typing import  List
import pytz
from bson import ObjectId


load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")

# Use shared client and database instances
client = get_client()
db = get_database()

# Module-level cache for /stats — avoids hammering the DB on every page load
_stats_cache: dict = {"data": None, "ts": 0.0}
_STATS_TTL = 60  # seconds

# Cache for the /admin/orders total count. Counting the (unindexed) "non-empty
# orders" predicate is expensive on a large collection, so we cache per filter
# signature and guard the query with maxTimeMS so a slow count can never hang
# the request past the socket timeout.
_orders_count_cache: dict = {}  # signature -> {"count": int, "ts": float}
_ORDERS_COUNT_TTL = 30  # seconds
_ORDERS_COUNT_MAX_MS = 8000  # give up counting well before the socket timeout


def _orders_count_signature(match: dict) -> str:
    import json

    return json.dumps(match, default=str, sort_keys=True)


def _cached_orders_count(match: dict) -> int:
    """Total count for the orders list, cached by filter signature. On a slow
    count we serve the last cached value (or a cheap estimate) rather than let
    the request time out."""
    sig = _orders_count_signature(match)
    now = time.time()
    entry = _orders_count_cache.get(sig)
    if entry and (now - entry["ts"]) < _ORDERS_COUNT_TTL:
        return entry["count"]
    try:
        count = orders_collection.count_documents(
            match, maxTimeMS=_ORDERS_COUNT_MAX_MS
        )
    except Exception as e:
        print(f"orders count fell back (slow/failed): {e}")
        if entry:
            return entry["count"]
        try:
            count = orders_collection.estimated_document_count()
        except Exception:
            count = 0
    _orders_count_cache[sig] = {"count": count, "ts": now}
    return count

products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]

def get_upcoming_stock_for_products(pre_order_products: list) -> dict:
    """
    For a list of pre-order products, return a dict mapping product _id (str)
    to a dict {"upcoming_stock", "inward_date", "eta_port_date"} sourced from
    the latest open/non-cancelled purchase order whose vendor matches the
    product's brand. The inward / ETA-at-port dates come from the matching
    brand_orders document (joined on purchaseorder_number).
    """
    if not pre_order_products:
        return {}

    brand_names = list({p.get("brand") for p in pre_order_products if p.get("brand")})
    item_id_to_product_id = {
        p.get("item_id"): str(p["_id"])
        for p in pre_order_products
        if p.get("item_id")
    }

    if not brand_names or not item_id_to_product_id:
        return {}

    # brand name → vendor_id
    brand_docs = list(db.brands.find(
        {"name": {"$in": brand_names}},
        {"name": 1, "vendor_id": 1}
    ))
    brand_to_vendor = {b["name"]: b.get("vendor_id") for b in brand_docs if b.get("vendor_id")}

    vendor_ids = list(set(brand_to_vendor.values()))
    if not vendor_ids:
        return {}

    # product item_id → vendor_id expected for that product
    item_id_to_vendor = {}
    for p in pre_order_products:
        iid = p.get("item_id")
        brand = p.get("brand")
        if iid and brand and brand in brand_to_vendor:
            item_id_to_vendor[iid] = brand_to_vendor[brand]

    # Fetch open/non-cancelled POs for these vendors that contain these items
    open_item_ids = list(item_id_to_vendor.keys())
    pos = list(db.purchase_orders.find(
        {
            "vendor_id": {"$in": vendor_ids},
            "status": {"$nin": ["cancelled"]},
            "line_items": {"$elemMatch": {"item_id": {"$in": open_item_ids}}},
        },
        {"vendor_id": 1, "line_items": 1, "date": 1, "purchaseorder_number": 1}
    ).sort("date", -1))

    # Walk POs newest-first; take the first match per item_id
    upcoming_by_item: dict = {}
    po_number_by_item: dict = {}
    seen: set = set()
    for po in pos:
        po_vendor = po.get("vendor_id")
        for li in po.get("line_items", []):
            iid = li.get("item_id")
            if not iid or iid in seen:
                continue
            if item_id_to_vendor.get(iid) != po_vendor:
                continue
            qty = float(li.get("quantity") or 0)
            qty_received = float(li.get("quantity_received") or 0)
            upcoming_by_item[iid] = max(0, int(qty - qty_received))
            if po.get("purchaseorder_number"):
                po_number_by_item[iid] = po["purchaseorder_number"]
            seen.add(iid)

    # Join to brand_orders for inward / ETA-at-port dates (keyed by PO number)
    dates_by_po: dict = {}
    po_numbers = list(set(po_number_by_item.values()))
    if po_numbers:
        for bo in db.brand_orders.find(
            {"purchaseorder_number": {"$in": po_numbers}},
            {"purchaseorder_number": 1, "inward_date": 1, "eta_port_date": 1, "_id": 0}
        ):
            dates_by_po[bo["purchaseorder_number"]] = {
                "inward_date": bo.get("inward_date"),
                "eta_port_date": bo.get("eta_port_date"),
            }

    # Map back to product _id
    result = {}
    for iid, pid in item_id_to_product_id.items():
        dates = dates_by_po.get(po_number_by_item.get(iid)) or {}
        result[pid] = {
            "upcoming_stock": upcoming_by_item.get(iid, 0),
            "inward_date": dates.get("inward_date"),
            "eta_port_date": dates.get("eta_port_date"),
        }
    return result


# Connect to attendance database
attendance_db = client.get_database("attendance")
employees_collection = attendance_db.get_collection("employees")
attendance_collection = attendance_db.get_collection("attendance")
device_collection = attendance_db.get_collection("devices")

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")  # Default to ap-south-1
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

MAX_FILE_SIZE_MB = 10


# Cache for device locations
DEVICE_CACHE = {}


def serialize_objectid_document(doc):
    """Helper function to recursively convert ObjectIds to strings in MongoDB documents"""
    if isinstance(doc, dict):
        return {key: serialize_objectid_document(value) for key, value in doc.items()}
    elif isinstance(doc, list):
        return [serialize_objectid_document(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc


def convert_utc_to_ist(utc_time):
    """Convert UTC time to IST"""
    if utc_time.tzinfo is None:
        utc_time = pytz.UTC.localize(utc_time)
    ist = pytz.timezone('Asia/Kolkata')
    return utc_time.astimezone(ist)


def parse_swipe_datetime(swipe_datetime):
    """Parse swipe_datetime which can be either a string or datetime object"""
    if isinstance(swipe_datetime, datetime):
        return swipe_datetime
    elif isinstance(swipe_datetime, str):
        try:
            # Try parsing format like "19-03-2025 09:57:00"
            return datetime.strptime(swipe_datetime, "%d-%m-%Y %H:%M:%S")
        except ValueError:
            try:
                # Try ISO format
                return datetime.fromisoformat(swipe_datetime.replace("Z", "+00:00"))
            except ValueError:
                return None
    elif isinstance(swipe_datetime, dict) and "$date" in swipe_datetime:
        # Handle MongoDB date format
        try:
            return datetime.fromisoformat(swipe_datetime["$date"].replace("Z", "+00:00"))
        except:
            return None
    return None


def get_all_devices_cached():
    """Cache all devices on first call to avoid repeated queries"""
    global DEVICE_CACHE
    
    if not DEVICE_CACHE:
        devices = list(device_collection.find({}))
        for device in devices:
            serialized_device = serialize_objectid_document(device)
            device_id = serialized_device["_id"]
            device_name = serialized_device.get("name", "Unknown Location")
            DEVICE_CACHE[device_id] = device_name
            if "name" in serialized_device:
                DEVICE_CACHE[serialized_device["name"]] = device_name
    
    return DEVICE_CACHE


def get_attendance_stats(start_of_today_ist, now_ist):
    """Get attendance statistics for today"""
    try:
        # Get today's date in string format for comparison
        today_str = start_of_today_ist.strftime("%Y-%m-%d")
        
        # Cache devices
        device_cache = get_all_devices_cached()
        
        # Get today's attendance records
        today_attendance_pipeline = [
            {
                "$match": {
                    "$or": [
                        {"created_at": {"$gte": start_of_today_ist}},
                        {"swipe_datetime": {"$gte": start_of_today_ist}}
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "employees",
                    "localField": "employee_id",
                    "foreignField": "_id",
                    "as": "employee_info"
                }
            },
            {
                "$unwind": {
                    "path": "$employee_info",
                    "preserveNullAndEmptyArrays": True
                }
            }
        ]
        
        attendance_records = list(attendance_collection.aggregate(today_attendance_pipeline))
        
        # Process attendance data
        total_records_today = len(attendance_records)
        unique_employees_today = set()
        check_ins_today = 0
        check_outs_today = 0
        total_work_hours = 0
        employee_attendance_summary = {}
        
        for record in attendance_records:
            employee_id = str(record.get("employee_id", ""))
            employee_name = record.get("employee_info", {}).get("name", "Unknown")
            
            # Parse swipe time
            swipe_time = parse_swipe_datetime(record.get("swipe_datetime"))
            if not swipe_time:
                swipe_time = parse_swipe_datetime(record.get("created_at"))
            
            if swipe_time:
                # Convert to IST
                ist_time = convert_utc_to_ist(swipe_time)
                date_key = ist_time.strftime("%Y-%m-%d")
                
                # Only process today's records
                if date_key == today_str:
                    unique_employees_today.add(employee_id)
                    
                    # Track check-ins and check-outs
                    is_check_in = record.get("is_check_in", True)
                    if is_check_in:
                        check_ins_today += 1
                    else:
                        check_outs_today += 1
                    
                    # Group by employee for work hours calculation
                    if employee_id not in employee_attendance_summary:
                        employee_attendance_summary[employee_id] = {
                            "name": employee_name,
                            "check_ins": [],
                            "check_outs": []
                        }
                    
                    if is_check_in:
                        employee_attendance_summary[employee_id]["check_ins"].append(ist_time)
                    else:
                        employee_attendance_summary[employee_id]["check_outs"].append(ist_time)
        
        # Calculate total work hours
        employees_with_complete_attendance = 0
        total_work_minutes = 0
        
        for emp_id, emp_data in employee_attendance_summary.items():
            check_ins = sorted(emp_data["check_ins"])
            check_outs = sorted(emp_data["check_outs"])
            
            # Calculate work hours for employees with both check-in and check-out
            if check_ins and check_outs:
                # Use first check-in and last check-out
                work_duration = check_outs[-1] - check_ins[0]
                work_minutes = work_duration.total_seconds() / 60
                total_work_minutes += work_minutes
                employees_with_complete_attendance += 1
        
        # Calculate average work hours
        average_work_hours = 0
        if employees_with_complete_attendance > 0:
            average_work_hours = total_work_minutes / employees_with_complete_attendance / 60
        
        # Get total employees count
        total_employees = employees_collection.count_documents({})
        
        # Calculate attendance rate
        attendance_rate = 0
        if total_employees > 0:
            attendance_rate = (len(unique_employees_today) / total_employees) * 100
        
        return {
            "total_attendance_records_today": total_records_today,
            "unique_employees_present_today": len(unique_employees_today),
            "total_employees": total_employees,
            "attendance_rate_percentage": round(attendance_rate, 2),
            "check_ins_today": check_ins_today,
            "check_outs_today": check_outs_today,
            "employees_with_complete_attendance": employees_with_complete_attendance,
            "average_work_hours_today": round(average_work_hours, 2),
            "total_devices": len(device_cache),
            "attendance_date": today_str,
            "last_updated_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
        }
        
    except Exception as e:
        print(f"Error getting attendance stats: {e}")
        return {
            "total_attendance_records_today": 0,
            "unique_employees_present_today": 0,
            "total_employees": 0,
            "attendance_rate_percentage": 0,
            "check_ins_today": 0,
            "check_outs_today": 0,
            "employees_with_complete_attendance": 0,
            "average_work_hours_today": 0,
            "total_devices": 0,
            "attendance_date": start_of_today_ist.strftime("%Y-%m-%d"),
            "last_updated_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
            "error": str(e)
        }


@router.get("/stats")
async def get_stats():
    if _stats_cache["data"] and (time.time() - _stats_cache["ts"]) < _STATS_TTL:
        return _stats_cache["data"]

    try:
        # Pre-calculate common date values
        ist = tz("Asia/Kolkata")
        now_ist = datetime.now(ist)
        six_months_ago = now_ist - timedelta(days=180)
        start_of_today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        today = date.today()
        day_before_yesterday = today - timedelta(days=2)
        day_before_yesterday_str = day_before_yesterday.isoformat()
        today_str = today.isoformat()

        # Create thread pool for concurrent database operations
        with ThreadPoolExecutor(max_workers=10) as executor:  # Increased workers for attendance
            # Group 1: Product statistics (can be combined into one aggregation)
            products_stats_future = executor.submit(get_products_stats)
            
            # Group 2: Customer statistics (can be combined)
            customers_stats_future = executor.submit(get_customers_stats)
            
            # Group 3: Billing statistics (complex, keep separate but optimize)
            billing_stats_future = executor.submit(get_billing_stats, six_months_ago)
            
            # Group 4: Sales people statistics
            sales_people_stats_future = executor.submit(get_sales_people_stats)
            
            # Group 5: Orders statistics (can be combined)
            orders_stats_future = executor.submit(get_orders_stats, start_of_today_ist)
            
            # Group 6: Content statistics (catalogues, trainings, announcements)
            content_stats_future = executor.submit(get_content_stats)
            
            # Group 7: Payment and visit statistics
            payments_visits_future = executor.submit(get_payments_visits_stats, 
                                                   today_str, day_before_yesterday_str, start_of_today_ist, today, day_before_yesterday)
            
            # Group 8: Miscellaneous counts
            misc_stats_future = executor.submit(get_misc_stats, start_of_today_ist)
            
            # Group 9: Customer analytics (most complex, keep separate)
            customer_analytics_future = executor.submit(get_customer_analytics_count)
            
            # Group 10: Attendance statistics (NEW)
            attendance_stats_future = executor.submit(get_attendance_stats, start_of_today_ist, now_ist)

            # Group 11: Product additions by type (SP vs Customer)
            product_additions_future = executor.submit(get_product_additions_by_type_stats)

            # Wait for all futures to complete
            products_stats = products_stats_future.result()
            customers_stats = customers_stats_future.result()
            billing_stats = billing_stats_future.result()
            sales_people_stats = sales_people_stats_future.result()
            orders_stats = orders_stats_future.result()
            content_stats = content_stats_future.result()
            payments_visits_stats = payments_visits_future.result()
            misc_stats = misc_stats_future.result()
            customer_analytics = customer_analytics_future.result()
            attendance_stats = attendance_stats_future.result()
            product_additions_stats = product_additions_future.result()

        # Combine all results
        result = {
            **products_stats,
            **customers_stats,
            **billing_stats,
            **sales_people_stats,
            **orders_stats,
            **content_stats,
            **payments_visits_stats,
            **misc_stats,
            **customer_analytics,
            **attendance_stats,
            **product_additions_stats
        }

        _stats_cache["data"] = result
        _stats_cache["ts"] = time.time()

        return result

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


# All existing helper functions remain the same
def get_products_stats():
    """Combine all product-related counts into a single aggregation"""
    pipeline = [
        {
            "$facet": {
                "active_stock": [{"$match": {"stock": {"$gt": 0}}}, {"$count": "count"}],
                "inactive": [{"$match": {"status": "inactive"}}, {"$count": "count"}],
                "total": [{"$count": "count"}],
                "active": [{"$match": {"status": "active"}}, {"$count": "count"}],
                "out_of_stock": [{"$match": {"stock": {"$lte": 0}}}, {"$count": "count"}]
            }
        }
    ]
    
    result = list(db["products"].aggregate(pipeline))[0]
    
    return {
        "active_stock_products": result["active_stock"][0]["count"] if result["active_stock"] else 0,
        "inactive_products": result["inactive"][0]["count"] if result["inactive"] else 0,
        "total_products": result["total"][0]["count"] if result["total"] else 0,
        "active_products": result["active"][0]["count"] if result["active"] else 0,
        "out_of_stock_products": result["out_of_stock"][0]["count"] if result["out_of_stock"] else 0,
    }


def get_customers_stats():
    """Combine all customer-related counts into a single aggregation"""
    pipeline = [
        {
            "$facet": {
                "assigned": [
                    {"$match": {"cf_sales_person": {"$exists": True, "$ne": "", "$ne": None}}},
                    {"$count": "count"}
                ],
                "unassigned": [
                    {
                        "$match": {
                            "$or": [
                                {"cf_sales_person": {"$exists": False}},
                                {"cf_sales_person": ""},
                                {"cf_sales_person": None},
                            ]
                        }
                    },
                    {"$count": "count"}
                ],
                "active": [{"$match": {"status": "active"}}, {"$count": "count"}],
                "inactive": [{"$match": {"status": "inactive"}}, {"$count": "count"}]
            }
        }
    ]
    
    result = list(db["customers"].aggregate(pipeline))[0]
    
    return {
        "assigned_customers": result["assigned"][0]["count"] if result["assigned"] else 0,
        "unassigned_customers": result["unassigned"][0]["count"] if result["unassigned"] else 0,
        "active_customers": result["active"][0]["count"] if result["active"] else 0,
        "inactive_customers": result["inactive"][0]["count"] if result["inactive"] else 0,
    }


def get_billing_stats(six_months_ago):
    """Optimized billing statistics with single pipeline for billed customers"""
    # Combined pipeline for billed customers and getting their IDs
    billed_customers_pipeline = [
        {
            "$match": {
                "status": {"$nin": ["void", "draft"]},
                "created_time": {"$exists": True},
            }
        },
        {
            "$addFields": {
                "parsed_date": {
                    "$dateFromString": {
                        "dateString": {"$substr": ["$created_time", 0, 19]}
                    }
                }
            }
        },
        {"$match": {"parsed_date": {"$gte": six_months_ago}}},
        {
            "$group": {
                "_id": "$customer_id"
            }
        },
        {
            "$facet": {
                "count": [{"$count": "total"}],
                "customer_ids": [{"$project": {"_id": 1}}]
            }
        }
    ]

    billed_result = list(db["invoices"].aggregate(billed_customers_pipeline))[0]
    total_billed_customers_6_months = billed_result["count"][0]["total"] if billed_result["count"] else 0
    billed_customer_ids = [doc["_id"] for doc in billed_result["customer_ids"]]

    # Count unbilled customers
    unbilled_customers_query = {
        "status": "active",
        "contact_id": {"$nin": billed_customer_ids},
    }
    total_unbilled_customers_6_months = db["customers"].count_documents(unbilled_customers_query)

    return {
        "total_billed_customers_6_months": total_billed_customers_6_months,
        "total_unbilled_customers_6_months": total_unbilled_customers_6_months,
    }


def get_sales_people_stats():
    """Combine sales people statistics"""
    pipeline = [
        {
            "$match": {"role": "sales_person"}
        },
        {
            "$facet": {
                "active": [{"$match": {"status": "active"}}, {"$count": "count"}],
                "inactive": [{"$match": {"status": "inactive"}}, {"$count": "count"}]
            }
        }
    ]
    
    result = list(db["users"].aggregate(pipeline))[0]
    active_sales_people = result["active"][0]["count"] if result["active"] else 0
    inactive_sales_people = result["inactive"][0]["count"] if result["inactive"] else 0
    
    return {
        "active_sales_people": active_sales_people,
        "inactive_sales_people": inactive_sales_people,
        "total_sales_people": active_sales_people + inactive_sales_people,
    }


def get_orders_stats(start_of_today_ist):
    """Combine all order statistics"""
    pipeline = [
        {
            "$match": {"created_at": {"$gte": start_of_today_ist}}
        },
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "draft": [{"$match": {"status": "draft"}}, {"$count": "count"}],
                "accepted": [{"$match": {"status": "accepted"}}, {"$count": "count"}],
                "declined": [{"$match": {"status": "declined"}}, {"$count": "count"}],
                "invoiced": [{"$match": {"status": "invoiced"}}, {"$count": "count"}]
            }
        }
    ]
    
    result = list(db["orders"].aggregate(pipeline))[0]
    
    return {
        "recent_orders": result["total"][0]["count"] if result["total"] else 0,
        "orders_draft": result["draft"][0]["count"] if result["draft"] else 0,
        "orders_accepted": result["accepted"][0]["count"] if result["accepted"] else 0,
        "orders_declined": result["declined"][0]["count"] if result["declined"] else 0,
        "orders_invoiced": result["invoiced"][0]["count"] if result["invoiced"] else 0,
    }


def get_content_stats():
    """Combine catalogues, trainings, and announcements statistics"""
    # Use concurrent execution for these independent collections
    with ThreadPoolExecutor(max_workers=3) as executor:
        catalogues_future = executor.submit(lambda: {
            "active": db["catalogues"].count_documents({"is_active": True}),
            "inactive": db["catalogues"].count_documents({"is_active": False})
        })
        
        trainings_future = executor.submit(lambda: {
            "active": db["trainings"].count_documents({"is_active": True}),
            "inactive": db["trainings"].count_documents({"is_active": False})
        })
        
        announcements_future = executor.submit(lambda: {
            "active": db["announcements"].count_documents({"is_active": True}),
            "inactive": db["announcements"].count_documents({"is_active": False})
        })
        
        catalogues = catalogues_future.result()
        trainings = trainings_future.result()
        announcements = announcements_future.result()
    
    return {
        "active_catalogues": catalogues["active"],
        "inactive_catalogues": catalogues["inactive"],
        "active_trainings": trainings["active"],
        "inactive_trainings": trainings["inactive"],
        "active_announcements": announcements["active"],
        "inactive_announcements": announcements["inactive"],
    }


def get_product_additions_by_type_stats():
    """Get breakdown of products added to cart by SP vs customer"""
    pipeline = [
        # Step 1: Filter out orders with status "declined" and "draft"
        {
            "$match": {
                "status": {
                    "$nin": ["declined", "draft"]
                }
            }
        },
        # Step 2: Unwind the products array to work with individual products
        {
            "$unwind": "$products"
        },
        # Step 2b: Normalize added_by so null/""/unknown all become "sales_person"
        {
            "$addFields": {
                "products.added_by": {
                    "$cond": {
                        "if": {"$in": ["$products.added_by", ["customer", "admin"]]},
                        "then": "$products.added_by",
                        "else": "sales_person"
                    }
                }
            }
        },
        # Step 3: Group by added_by to count products by type
        {
            "$group": {
                "_id": "$products.added_by",
                "count": {
                    "$sum": 1
                }
            }
        },
        # Step 4: Group all results together to calculate totals and percentages
        {
            "$group": {
                "_id": None,
                "results": {
                    "$push": {
                        "added_by": "$_id",
                        "count": "$count"
                    }
                },
                "total_products": {
                    "$sum": "$count"
                }
            }
        },
        # Step 5: Calculate percentages
        {
            "$project": {
                "_id": 0,
                "total_products": 1,
                "breakdown": {
                    "$map": {
                        "input": "$results",
                        "as": "result",
                        "in": {
                            "added_by": "$$result.added_by",
                            "count": "$$result.count",
                            "percentage": {
                                "$round": [
                                    {
                                        "$multiply": [
                                            {
                                                "$divide": [
                                                    "$$result.count",
                                                    "$total_products"
                                                ]
                                            },
                                            100
                                        ]
                                    },
                                    2
                                ]
                            }
                        }
                    }
                }
            }
        }
    ]

    result = list(db["orders"].aggregate(pipeline))

    if result and len(result) > 0:
        return {
            "product_additions_by_type": result[0]
        }
    else:
        return {
            "product_additions_by_type": {
                "total_products": 0,
                "breakdown": []
            }
        }



def get_payments_visits_stats(today_str, day_before_yesterday_str, start_of_today_ist, today_date, day_before_yesterday_date):
    """Combine payment and visit statistics with mixed due_date support"""
    # Convert date objects to datetime objects for MongoDB comparison
    today_datetime = datetime.combine(today_date, datetime.min.time())
    day_before_yesterday_datetime = datetime.combine(day_before_yesterday_date, datetime.min.time())
    
    # Payments with mixed due_date format support
    payments_pipeline = [
        {
            "$facet": {
                "overdue": [
                    {
                        "$match": {
                            "$or": [
                                # Case 1: due_date is a string
                                {
                                    "due_date": {"$type": "string", "$lt": today_str}
                                },
                                # Case 2: due_date is a date
                                {
                                    "due_date": {"$type": "date", "$lt": today_datetime}
                                }
                            ],
                            "status": {"$nin": ["paid", "void"]},
                        }
                    },
                    {"$count": "count"}
                ],
                "due_today": [
                    {
                        "$match": {
                            "$or": [
                                # Case 1: due_date is a string
                                {
                                    "due_date": {
                                        "$type": "string", 
                                        "$gt": day_before_yesterday_str, 
                                        "$lt": today_str
                                    }
                                },
                                # Case 2: due_date is a date
                                {
                                    "due_date": {
                                        "$type": "date", 
                                        "$gt": day_before_yesterday_datetime, 
                                        "$lt": today_datetime
                                    }
                                }
                            ],
                            "status": {"$nin": ["paid", "void"]},
                        }
                    },
                    {"$count": "count"}
                ]
            }
        }
    ]
    
    payments_result = list(db["invoices"].aggregate(payments_pipeline))[0]
    
    # Visits
    visits_pipeline = [
        {
            "$match": {"created_at": {"$gte": start_of_today_ist}}
        },
        {
            "$facet": {
                "submitted": [{"$count": "count"}],
                "updated": [
                    {"$match": {"updates": {"$exists": True}}},
                    {"$count": "count"}
                ]
            }
        }
    ]
    
    visits_result = list(db["daily_visits"].aggregate(visits_pipeline))[0]
    
    return {
        "total_due_payments": payments_result["overdue"][0]["count"] if payments_result["overdue"] else 0,
        "total_due_payments_today": payments_result["due_today"][0]["count"] if payments_result["due_today"] else 0,
        "submitted_daily_visits": visits_result["submitted"][0]["count"] if visits_result["submitted"] else 0,
        "updated_daily_visits": visits_result["updated"][0]["count"] if visits_result["updated"] else 0,
    }

def get_misc_stats(start_of_today_ist):
    """Get miscellaneous statistics concurrently"""
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Hook categories
        hook_categories_future = executor.submit(lambda: {
            "active": db["hooks_category"].count_documents({"is_active": True}),
            "inactive": db["hooks_category"].count_documents({"is_active": False})
        })
        
        # Today's submissions
        today_filter = {"created_at": {"$gte": start_of_today_ist}}
        submissions_future = executor.submit(lambda: {
            "shop_hooks": db["shop_hooks"].count_documents(today_filter),
            "potential_customers": db["potential_customers"].count_documents(today_filter),
            "targeted_customers": db["targeted_customers"].count_documents(today_filter),
            "expected_reorders": db["expected_reorders"].count_documents(today_filter),
        })
        
        # General counts
        general_counts_future = executor.submit(lambda: {
"return_orders": db["return_orders"].count_documents({}),
            "brands": db["brands"].count_documents({}),
            "external_links": db["external_links"].count_documents({}),
            "permissions":db["permissions"].count_documents({"is_active":True}),
        })
        
        
        hook_categories = hook_categories_future.result()
        submissions = submissions_future.result()
        general_counts = general_counts_future.result()
    
    return {
        "active_hook_categories": hook_categories["active"],
        "inactive_hook_categories": hook_categories["inactive"],
        "submitted_shop_hooks": submissions["shop_hooks"],
        "submitted_potential_customers": submissions["potential_customers"],
        "submitted_targeted_customers": submissions["targeted_customers"],
        "submitted_expected_reorders": submissions["expected_reorders"],
        **general_counts
    }


def get_customer_analytics_count():
    """Optimized customer analytics count with better indexing hints"""
    customer_analytics_count_pipeline = [
        # Add hint for better performance if you have the right index
        {
            "$match": {
                "date": {"$gte": "2023-04-01"},
                "status": {"$nin": ["void", "draft"]},
                "$and": [
                {
                    "customer_name": {
                        "$not": {
                            "$regex": r"\b(EC|NA|PUPEV|RS|MKT|SPUR|SSAM|OSAMP)\b",
                            "$options": "i",
                        }
                    }
                },
                {
                    "customer_name": {
                        "$not": {
                            "$regex": r"(amzb2b|amz2b2|Blinkit|Flipkart)",
                            "$options": "i",
                        }
                    }
                },
            ],
            }
        },
        {
            "$addFields": {
                "normalizedCity": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(bangalore|bengaluru)$",
                                        "options": "i",
                                    }
                                },
                                "then": "bengaluru",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(mumbai|bombay)$",
                                        "options": "i",
                                    }
                                },
                                "then": "mumbai",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(delhi|new delhi)$",
                                        "options": "i",
                                    }
                                },
                                "then": "delhi",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(kolkata|calcutta)$",
                                        "options": "i",
                                    }
                                },
                                "then": "kolkata",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(chennai|madras)$",
                                        "options": "i",
                                    }
                                },
                                "then": "chennai",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(hyderabad|secunderabad)$",
                                        "options": "i",
                                    }
                                },
                                "then": "hyderabad",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$billing_address.city",
                                        "regex": "^(pune|poona)$",
                                        "options": "i",
                                    }
                                },
                                "then": "pune",
                            },
                        ],
                        "default": {"$toLower": "$billing_address.city"},
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "customerId": "$customer_id",
                    "city": "$normalizedCity",
                    "state": "$billing_address.state", 
                    "zip": "$billing_address.zip",
                    "country": "$billing_address.country",
                }
            }
        },
        {
            "$count": "total_customer_analytics"
        }
    ]

    customer_analytics_result = list(db["invoices"].aggregate(customer_analytics_count_pipeline))
    total_customer_analytics = customer_analytics_result[0]["total_customer_analytics"] if customer_analytics_result else 0
    
    return {"total_customer_analytics": total_customer_analytics}

@router.get("/brands")
def get_all_brands():
    """
    Retrieve a list of all distinct brands.
    """
    try:
        brands = products_collection.distinct(
            "brand", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        brands = [brand for brand in brands if brand]  # Remove empty or null brands
        return {"brands": brands}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.get("/categories")
def get_all_categories():
    """
    Retrieve a list of all distinct categories.
    """
    try:
        categories = products_collection.distinct(
            "category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        categories = [
            category for category in categories if category
        ]  # Remove empty or null categories
        return {"categories": categories}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch categories.")


@router.get("/sub_categories")
def get_all_sub_categories():
    """
    Retrieve a list of all distinct sub_categories.
    """
    try:
        sub_categories = products_collection.distinct(
            "sub_category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        sub_categories = [
            sub_category for sub_category in sub_categories if sub_category
        ]  # Remove empty or null sub_categories
        return {"sub_categories": sub_categories}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch sub_categories.")


@router.get("/products")
def get_products(
    page: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    # New query params
    status: Optional[str] = None,  # e.g. 'active' or 'inactive'
    stock: Optional[str] = None,  # e.g. 'zero' or 'gt_zero'
    new_arrivals: Optional[bool] = None,
    missing_info_products: Optional[bool] = None,
    pre_order: Optional[bool] = None,
    clearance: Optional[bool] = None,
    sort_by: Optional[str] = None,
):
    """
    Retrieve products with optional search, brand, status, stock, and new_arrivals filtering.
    """

    try:
        query = {}

        # 1) Status Filter
        if status == "active":
            query["status"] = "active"
        elif status == "inactive":
            query["status"] = "inactive"

        # 2) Stock Filter
        if stock == "zero":
            # products where stock = 0
            query["stock"] = {"$lte": 0}
        elif stock == "gt_zero":
            # products where stock > 0
            query["stock"] = {"$gt": 0}

        # 3) New Arrivals (depending on how you define "new")
        if new_arrivals:
            from datetime import datetime, timedelta

            ninty_days_ago = datetime.now() - timedelta(days=90)
            query["created_at"] = {"$gte": ninty_days_ago}

        if pre_order:
            query["pre_order"] = True

        if clearance:
            query["clearance"] = True

        if missing_info_products:
            query["$and"] = [
                {"$or": [{"series": {"$exists": False}}, {"series": ""}]},
                {"$or": [{"category": {"$exists": False}}, {"category": ""}]},
                {"$or": [{"sub_category": {"$exists": False}}, {"sub_category": ""}]},
            ]

        # 4) Search Filter
        if search and search.strip() != "":
            regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # 5) Brand Filter
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}

        if category and category.lower() != "all":
            query["category"] = {"$regex": f"^{category}$", "$options": "i"}

        if sub_category and sub_category.lower() != "all":
            query["sub_category"] = {"$regex": f"^{sub_category}$", "$options": "i"}

        # Pagination
        skip = page * limit

        if sort_by == "catalogue":
            sort_spec = [("catalogue_order", 1)]
        elif sort_by == "latest":
            sort_spec = [("created_at", -1)]
        else:
            sort_spec = [("status", 1), ("name", 1)]

        docs_cursor = (
            products_collection.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(limit)
        )
        print(json.dumps(query, indent=4, default=str))
        total_count = products_collection.count_documents(query)
        products = [serialize_mongo_document(doc) for doc in docs_cursor]

        # Enrich pre-order products with upcoming stock from purchase orders
        pre_order_prods = [p for p in products if p.get("pre_order")]
        if pre_order_prods:
            upcoming_map = get_upcoming_stock_for_products(pre_order_prods)
            for p in products:
                if p.get("pre_order"):
                    info = upcoming_map.get(p["_id"]) or {}
                    p["upcoming_stock"] = info.get("upcoming_stock", 0)
                    p["inward_date"] = info.get("inward_date")
                    p["eta_port_date"] = info.get("eta_port_date")

        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return JSONResponse(
            {
                "products": products,
                "total_count": total_count,
                "page": page,
                "per_page": limit,
                "total_pages": total_pages,
            }
        )

    except Exception as e:
        print(e)
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.post("/products/update-stock")
def admin_update_stock(background_tasks: BackgroundTasks):
    """
    Schedules the stock update to run in the background and returns immediately.
    """
    if update_stock_lock.locked():
        raise HTTPException(status_code=409, detail="Stock update is already running.")
    background_tasks.add_task(run_update_stock)
    return {"message": "Stock update started in the background."}


@router.get("/products/download")
def download_products(
    search: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    status: Optional[str] = None,  # e.g. 'active' or 'inactive'
    stock: Optional[str] = None,  # e.g. 'zero' or 'gt_zero'
    new_arrivals: Optional[bool] = None,
    missing_info_products: Optional[bool] = None,
):
    """
    Download products in XLSX format using the same filters.
    """
    try:
        query = {}

        # 1) Status Filter
        if status == "active":
            query["status"] = "active"
        elif status == "inactive":
            query["status"] = "inactive"

        # 2) Stock Filter
        if stock == "zero":
            query["stock"] = {"$lte": 0}
        elif stock == "gt_zero":
            query["stock"] = {"$gt": 0}

        # 3) New Arrivals
        if new_arrivals:
            ninety_days_ago = datetime.now() - timedelta(days=90)
            query["created_at"] = {"$gte": ninety_days_ago}

        # 4) Missing Info Filter for series, category, sub_category
        if missing_info_products:
            query["$and"] = [
                {"$or": [{"series": {"$exists": False}}, {"series": ""}]},
                {"$or": [{"category": {"$exists": False}}, {"category": ""}]},
                {"$or": [{"sub_category": {"$exists": False}}, {"sub_category": ""}]},
            ]

        # 5) Search Filter
        if search and search.strip() != "":
            regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

        # 6) Brand, Category, Sub Category Filters
        if brand and brand.lower() != "all":
            query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}
        if category and category.lower() != "all":
            query["category"] = {"$regex": f"^{category}$", "$options": "i"}
        if sub_category and sub_category.lower() != "all":
            query["sub_category"] = {"$regex": f"^{sub_category}$", "$options": "i"}

        docs_cursor = products_collection.find(query).sort([("status", 1), ("name", 1)])
        products = [serialize_mongo_document(doc) for doc in docs_cursor]

        # Create a new workbook and worksheet using openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Products"

        # Define headers (adjust these fields as needed)
        headers = [
            "Name",
            "Brand",
            "Category",
            "Sub Category",
            "Series",
            "SKU",
            "Price",
            "Stock",
            "Status",
            "Created At",
        ]
        ws.append(headers)

        for product in products:
            # Convert created_at to a formatted string if present
            created_at = ""
            if product.get("created_at"):
                if isinstance(product["created_at"], datetime):
                    created_at = product["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    created_at = str(product["created_at"])

            row = [
                product.get("name", ""),
                product.get("brand", ""),
                product.get("category", ""),
                product.get("sub_category", ""),
                product.get("series", ""),
                product.get("cf_sku_code", ""),
                product.get("rate", ""),
                product.get("stock", ""),
                product.get("status", ""),
                created_at,
            ]
            ws.append(row)

        # Save workbook to a BytesIO stream
        stream = io.BytesIO()
        wb.save(stream)
        stream.seek(0)

        filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        headers_response = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers_response,
        )

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/customers")
def get_customers(
    name: Optional[str] = None,
    page: int = Query(1, ge=1, description="1-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sort: Optional[str] = None,
    status: Optional[str] = Query(
        None, description="Filter by customer status: active or inactive"
    ),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    unassigned: Optional[bool] = Query(
        None, description="Filter for unassigned customers"
    ),
    gst_type: Optional[str] = Query(
        None, description="Filter by customer type: exclusive or inclusive"
    ),
):
    """
    Returns a paginated list of customers, optionally filtered by name.
    Sorting is optional:
      - Default sort is ascending by 'status'
      - If ?sort=desc, it will sort descending by 'status'
    """
    try:
        query = {}
        # Filter by name if provided
        if name:
            query["contact_name"] = re.compile(re.escape(name), re.IGNORECASE)
        # Sort logic
        sort_order = [("status", 1)]  # default ascending by status
        if sort and sort.lower() == "desc":
            sort_order = [("status", -1)]

        # Filter by status if provided
        if status:
            if status.lower() not in ["active", "inactive"]:
                raise HTTPException(
                    status_code=400, detail="Invalid status filter value"
                )
            query["status"] = status.lower()

        # Filter by sales_person if provided
        if sales_person:
            escaped_sales_person = re.escape(sales_person)
            query["$or"] = [
                {
                    "cf_sales_person": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
                {
                    "salesperson_name": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
            ]

        # Filter for unassigned customers if true
        if unassigned:
            query["$or"] = [
                {"cf_sales_person": {"$exists": False}},
                {"cf_sales_person": ""},
                {"cf_sales_person": None},
            ]
        if gst_type:
            if str(gst_type).capitalize() == "Inclusive":
                query["$and"] = [
                    {"cf_in_ex": {"$exists": True}},
                    {"cf_in_ex": "Inclusive"},
                ]
            else:
                query["$or"] = [
                    {"cf_in_ex": {"$exists": False}},
                    {"cf_in_ex": "Exclusive"},
                ]

        # print(json.dumps(query, indent=4))
        # Calculate skip based on 1-based indexing
        skip = (page - 1) * limit
        cursor = (
            customers_collection.find(query).sort(sort_order).skip(skip).limit(limit)
        )
        # Count total matching documents for pagination
        total_count = customers_collection.count_documents(query)
        customers = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        if page > total_pages and total_pages != 0:
            customers = []

        return {
            "customers": customers,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@router.get("/customers/report")
def get_customers_report(
    name: Optional[str] = None,
    sort: Optional[str] = None,
    status: Optional[str] = Query(
        None, description="Filter by customer status: active or inactive"
    ),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    unassigned: Optional[bool] = Query(
        None, description="Filter for unassigned customers"
    ),
    gst_type: Optional[str] = Query(
        None, description="Filter by customer type: exclusive or inclusive"
    ),
):
    # Build the query similar to your /customers endpoint.
    query = {}
    if name:
        query["contact_name"] = re.compile(re.escape(name), re.IGNORECASE)

    sort_order = [("status", 1)]
    if sort and sort.lower() == "desc":
        sort_order = [("status", -1)]

    if status:
        if status.lower() not in ["active", "inactive"]:
            raise HTTPException(status_code=400, detail="Invalid status filter value")
        query["status"] = status.lower()

    if sales_person:
        escaped_sales_person = re.escape(sales_person)
        query["$or"] = [
            {
                "cf_sales_person": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
            {
                "salesperson_name": {
                    "$regex": f"^{escaped_sales_person}$",
                    "$options": "i",
                }
            },
        ]

    if unassigned:
        query["$or"] = [
            {"cf_sales_person": {"$exists": False}},
            {"cf_sales_person": ""},
            {"cf_sales_person": None},
        ]

    if gst_type:
        if str(gst_type).capitalize() == "Inclusive":
            query["$and"] = [
                {"cf_in_ex": {"$exists": True}},
                {"cf_in_ex": "Inclusive"},
            ]
        else:
            query["$or"] = [
                {"cf_in_ex": {"$exists": False}},
                {"cf_in_ex": "Exclusive"},
            ]

    # Fetch matching customers (adjust as necessary for your setup)
    customers_cursor = customers_collection.find(query).sort(sort_order)
    customers = [serialize_mongo_document(doc) for doc in customers_cursor]

    # Create an Excel workbook using openpyxl.
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Customers Report"

    # Define the header row.
    headers = [
        "Customer Name",
        "Sales Person",
        "GST Number",
        "Status",
        "Whatsapp group",
        "Place Of Supply",
    ]
    ws.append(headers)

    for cust in customers:
        # Extract state codes from each address
        addresses = cust.get("addresses", [])
        state_codes = set()
        for addr in addresses:
            state_value = addr.get("state", "")
            if state_value:
                state_codes.add(state_value.title())
        place_of_supply = ", ".join(state_codes)

        # Handle sales person conversion if it's a list.
        sales_person_val = cust.get("cf_sales_person", "") or cust.get(
            "salesperson_name", ""
        )
        if isinstance(sales_person_val, list):
            sales_person_val = ", ".join(sales_person_val)

        row = [
            cust.get("contact_name", ""),
            sales_person_val,
            cust.get("gst_no", "-"),
            cust.get("status", ""),
            cust.get("cf_whatsapp_group", "-"),
            place_of_supply,
        ]
        ws.append(row)

    # Save the workbook to a binary stream.
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=customers_report.xlsx"},
    )


@router.delete("/customers/{customer_id}")
def delete_customer(customer_id: str):
    try:
        result = customers_collection.delete_one({"_id": ObjectId(customer_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Customer not found")
        return {"message": "Customer deleted successfully"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.delete("/customers/{customer_id}/address/{address_id}")
def delete_customer_address(customer_id: str, address_id: str):
    try:
        result = customers_collection.update_one(
            {"_id": ObjectId(customer_id)},
            {"$pull": {"addresses": {"address_id": address_id}}},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Customer not found")
        db["customer_address_details"].delete_one(
            {"customer_id": customer_id, "address_id": address_id}
        )
        return {"message": "Address deleted successfully"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/orders")
def read_all_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(25, ge=1, description="Number of items per page"),
    sales_person: Optional[str] = Query(
        None, description="Filter by sales person name"
    ),
    status: Optional[str] = Query(None, description="Filter by order status"),
    estimate_created: Optional[bool] = Query(
        None, description="Filter by whether estimate was created"
    ),
    spreadsheet_created: Optional[bool] = Query(
        None, description="Filter by whether the spreadsheet was created"
    ),
    amount: Optional[str] = Query(None, description="Filter by amount"),
    estimate_number: Optional[str] = Query(
        None, description="Search by estimate number"
    ),
    search: Optional[str] = Query(
        None,
        description=(
            "Free-text search across invoice number, customer name, estimate "
            "number, sales person (name/code) and order id (_id)."
        ),
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    has_pre_order: Optional[bool] = Query(None, description="Filter orders containing pre-order items"),
    hide_empty: bool = Query(
        True,
        description=(
            "When true (default), hide orders with no customer or nothing added "
            "to the cart. Set false to include those empty orders."
        ),
    ),
    order_id: Optional[str] = Query(None, description="Fetch a single order by its Mongo _id"),
):
    """
    Retrieve all orders for admin, with pagination and optional filters,
    converting created_at to IST in MongoDB.
    """
    # Initialize the match stage for the main pipeline
    initial_match_conditions = {}

    # Deep-link support: fetch one specific order by _id (ignores other filters).
    if order_id:
        try:
            initial_match_conditions["_id"] = ObjectId(order_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid order_id")

    date_filter = {}
    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        date_filter["$gte"] = start_date_obj
    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
        date_filter["$lte"] = end_date_obj

    if date_filter:
        initial_match_conditions["created_at"] = date_filter

    if estimate_number:
        initial_match_conditions["estimate_number"] = {
            "$regex": f"^{re.escape(estimate_number.strip())}",
            "$options": "i",
        }
    if status:
        initial_match_conditions["status"] = status.lower()

    if estimate_created is not None:
        initial_match_conditions["estimate_created"] = estimate_created

    if spreadsheet_created is not None:
        initial_match_conditions["spreadsheet_created"] = spreadsheet_created

    if amount:
        # Assuming you want to filter for total_amount > 0 when 'amount' is provided
        initial_match_conditions["total_amount"] = {"$gt": 0}

    if has_pre_order:
        initial_match_conditions["products"] = {"$elemMatch": {"pre_order": True}}

    # By default, hide "empty" orders — those with no customer attached or with
    # nothing actually added to the cart. Toggleable via `hide_empty`. A
    # deep-link (order_id) or an explicit search bypasses this so any order can
    # still be located. Applied in the initial match (top-level fields only) so
    # it runs before the lookups.
    if hide_empty and not order_id and not search:
        initial_match_conditions.setdefault("$and", [])
        initial_match_conditions["$and"].append(
            {"customer_id": {"$exists": True, "$nin": [None, ""]}}
        )
        initial_match_conditions["$and"].append(
            {
                "$or": [
                    {"products": {"$elemMatch": {"quantity": {"$gt": 0}}}},
                    {"products": {"$elemMatch": {"pre_order_quantity": {"$gt": 0}}}},
                ]
            }
        )

    # Resolve join-based filters (sales person, free-text search) to
    # order-native conditions so they can be applied in the initial match. This
    # keeps the fast "paginate then enrich" path for every request and avoids
    # $lookup-based filtering across the whole collection.
    extra_and = []

    if sales_person:
        sp_ids = [
            u["_id"]
            for u in users_collection.find({"code": sales_person}, {"_id": 1})
        ]
        # Unknown salesperson → match nothing rather than everything.
        extra_and.append({"created_by": {"$in": sp_ids}})

    if search:
        term = search.strip()
        rx = {"$regex": re.escape(term), "$options": "i"}
        or_search = [
            {"customer_name": rx},
            {"estimate_number": rx},
            {"pre_order_estimate_number": rx},
            {"reference_number": rx},
            {"zoho_flow.invoice_number": rx},
        ]
        if ObjectId.is_valid(term):
            or_search.append({"_id": ObjectId(term)})
        # Sales person by name/code → created_by ids.
        _user_ids = [
            u["_id"]
            for u in users_collection.find(
                {"$or": [{"name": rx}, {"code": rx}]}, {"_id": 1}
            )
        ]
        if _user_ids:
            or_search.append({"created_by": {"$in": _user_ids}})
        # Invoice number → its estimate(s) → the order carrying that estimate_id.
        # Anchored prefix match so this can use the invoice_number index instead
        # of scanning the whole invoices collection; capped as a safety net.
        _inv_rx = {"$regex": f"^{re.escape(term)}", "$options": "i"}
        _inv_ids = [
            i["invoice_id"]
            for i in db.invoices.find(
                {"invoice_number": _inv_rx}, {"invoice_id": 1}
            ).limit(200)
        ]
        if _inv_ids:
            _est_ids = [
                e["estimate_id"]
                for e in db.estimates.find(
                    {"invoice_ids": {"$in": _inv_ids}}, {"estimate_id": 1}
                )
            ]
            if _est_ids:
                or_search.append({"estimate_id": {"$in": _est_ids}})
        extra_and.append({"$or": or_search})

    if extra_and:
        initial_match_conditions.setdefault("$and", [])
        initial_match_conditions["$and"].extend(extra_and)

    # Enrich only the current page's rows. The users join is cheap and indexed;
    # estimate status and invoice payment status are batched in Python below
    # (a $lookup on invoices timed out — its array localField can't use the
    # index).
    users_lookup = [
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
    ]

    sort_stage = {"$sort": {"created_at": -1}}
    paginate_stages = [{"$skip": page * limit}, {"$limit": limit}]

    total_count = _cached_orders_count(initial_match_conditions)
    pipeline = [{"$match": initial_match_conditions}, sort_stage]
    pipeline += paginate_stages
    pipeline += users_lookup

    # Only the unfiltered/date list benefits from streaming the created_at index;
    # for a targeted search/salesperson/deep-link the planner's own index choice
    # (with a blocking sort over the small result set) is better.
    agg_hint = None
    if not (sales_person or search or order_id):
        agg_hint = "created_at_1"

    # Project stage to format fields (including date conversion)
    pipeline.append(
        {
            "$project": {
                "created_by": 1,
                "total_amount": 1,
                "total_gst": 1,
                "gst_type": 1,
                "status": 1,
                "products": 1,
                "shipping_address": 1,
                "billing_address": 1,
                "customer_id": 1,
                "customer_name": 1,
                "estimate_url": 1,
                "estimate_created": 1,
                "estimate_number": 1,
                "estimate_id": 1,
                "pre_order_estimate_url": 1,
                "pre_order_estimate_created": 1,
                "pre_order_estimate_number": 1,
                "pre_order_estimate_id": 1,
                "reference_number": 1,
                "spreadsheet_url": 1,
                "spreadsheet_created": 1,
                "payment": 1,
                "payment_mode": 1,
                "zoho_flow": 1,
                "created_at": {
                    "$dateToString": {
                        "date": "$created_at",
                        "format": "%Y-%m-%d %H:%M:%S",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "updated_at": {
                    "$dateToString": {
                        "date": "$updated_at",
                        "format": "%Y-%m-%d %H:%M:%S",
                        "timezone": "Asia/Kolkata",
                    }
                },
                "created_by_info.id": {"$toString": "$created_by_info._id"},
                "created_by_info.name": "$created_by_info.name",
                "created_by_info.first_name": "$created_by_info.first_name",
                "created_by_info.last_name": "$created_by_info.last_name",
                "created_by_info.email": "$created_by_info.email",
                "created_by_info.code": "$created_by_info.code",
                "created_by_info.self_registered": "$created_by_info.self_registered",
                "created_by_info.role": "$created_by_info.role",
            }
        }
    )

    # Execute the pipeline for orders data
    _agg_kwargs = {"allowDiskUse": True}
    if agg_hint:
        _agg_kwargs["hint"] = agg_hint
    orders_cursor = orders_collection.aggregate(pipeline, **_agg_kwargs)
    orders_with_user_info = [serialize_mongo_document(doc) for doc in orders_cursor]

    # Server stores datetimes in UTC — render payment/zoho_flow timestamps as
    # IST strings so the admin UI can display them directly.
    _ist = pytz.timezone("Asia/Kolkata")

    def _to_ist_str(value):
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return value
        if not isinstance(value, datetime):
            return value
        if value.tzinfo is None:
            value = pytz.utc.localize(value)
        return value.astimezone(_ist).strftime("%Y-%m-%d %H:%M:%S")

    for _o in orders_with_user_info:
        for _sub in ("payment", "zoho_flow"):
            _obj = _o.get(_sub)
            if isinstance(_obj, dict):
                for _k, _v in list(_obj.items()):
                    if _k.endswith("_at") or _k in ("paid_at",):
                        _obj[_k] = _to_ist_str(_v)

    # Batch-enrich the page's rows with live estimate status (point of truth,
    # kept current by the Zoho webhook) and, for invoiced estimates, an
    # invoice-level payment status. Done with simple indexed $in queries over
    # only the current page instead of a $lookup (an invoices $lookup timed out
    # because its array localField can't use the invoice_id index).
    _now = datetime.now()

    def _parse_due(value):
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).replace(tzinfo=None)
            except ValueError:
                return None
        return None

    _est_ids = {
        _o.get("estimate_id")
        for _o in orders_with_user_info
        if _o.get("estimate_id")
    }
    _est_map = {}
    _inv_ids_all = set()
    if _est_ids:
        for _e in db.estimates.find(
            {"estimate_id": {"$in": list(_est_ids)}},
            {"estimate_id": 1, "status": 1, "invoice_ids": 1},
        ):
            _est_map[_e["estimate_id"]] = _e
            if str(_e.get("status", "")).lower() == "invoiced":
                _inv_ids_all.update(_e.get("invoice_ids") or [])

    _inv_map = {}
    if _inv_ids_all:
        for _i in db.invoices.find(
            {"invoice_id": {"$in": list(_inv_ids_all)}},
            {"invoice_id": 1, "status": 1, "due_date": 1, "balance": 1},
        ):
            _inv_map[_i["invoice_id"]] = _i

    # Derive an invoice-level payment status so the client can distinguish a
    # paid/overdue Zoho invoice from a gateway (Razorpay) payment. Mirrors the
    # paid/overdue logic used by /admin/payments_due: overdue = a non-void,
    # unpaid invoice whose due_date is in the past.
    for _o in orders_with_user_info:
        _est = _est_map.get(_o.get("estimate_id"))
        if not _est:
            continue
        _o["estimate_status"] = _est.get("status")
        if str(_est.get("status", "")).lower() != "invoiced":
            continue
        _active = [
            _inv_map[_iid]
            for _iid in (_est.get("invoice_ids") or [])
            if _iid in _inv_map
            and str(_inv_map[_iid].get("status", "")).lower() != "void"
        ]
        if not _active:
            continue
        _statuses = [str(i.get("status", "")).lower() for i in _active]
        _overdue = any(
            st != "paid" and (_parse_due(i.get("due_date")) or _now) < _now
            for i, st in zip(_active, _statuses)
        )
        if _overdue:
            _o["invoice_payment_status"] = "overdue"
        elif all(s == "paid" for s in _statuses):
            _o["invoice_payment_status"] = "paid"
        else:
            _o["invoice_payment_status"] = _statuses[0]

    # Order lines snapshot `image_url` at save time, but many products have it
    # empty/None while still carrying a valid `images` array. Backfill from the
    # products collection so the admin drawer doesn't render placeholders.
    try:
        _missing_pids = {
            str(p.get("product_id"))
            for _o in orders_with_user_info
            for p in (_o.get("products") or [])
            if p.get("product_id") and not p.get("image_url")
        }
        if _missing_pids:
            _img_by_pid = {}
            for _d in db.products.find(
                {"_id": {"$in": [ObjectId(x) for x in _missing_pids]}},
                {"image_url": 1, "images": 1},
            ):
                _imgs = _d.get("images") or []
                _url = _d.get("image_url") or (_imgs[0] if _imgs else None)
                if _url:
                    _img_by_pid[str(_d["_id"])] = _url
            for _o in orders_with_user_info:
                for p in _o.get("products") or []:
                    if not p.get("image_url"):
                        _url = _img_by_pid.get(str(p.get("product_id")))
                        if _url:
                            p["image_url"] = _url
    except Exception as e:
        # Images are cosmetic — never fail the orders list over them
        print(f"Error backfilling order product images: {e}")

    if total_count == -1:
        # Count was too slow and fell back to "unknown"; don't range-check the
        # page (the rows for this page were still fetched).
        total_pages = -1
    else:
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number - this check should use the *actual* total pages
        if total_pages > 0 and page >= total_pages:
            raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "orders": orders_with_user_info,
        "total_count": total_count,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
    }


@router.post("/orders/{order_id}/retry_payment_chain")
async def retry_payment_chain(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Re-run the post-payment Zoho chain (accepted estimate -> draft sales order
    -> sent invoice -> customer payment) for a PAID order. Idempotent: steps
    already completed are skipped, so this finishes whatever failed partway
    (e.g. a customer-payment number conflict).
    """
    from .payments import _get_order_or_404, _run_post_payment_zoho_chain

    order = _get_order_or_404(order_id)
    if (order.get("payment") or {}).get("status") != "paid":
        raise HTTPException(status_code=400, detail="Order has no successful online payment")

    try:
        result = await _run_post_payment_zoho_chain(order_id, request, background_tasks)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    return result


@router.get("/pg_payments")
def read_pg_payments(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(20, ge=1, description="Number of items per page"),
    action: Optional[str] = Query(None, description="Filter by transaction action"),
    search: Optional[str] = Query(
        None, description="Search by order id / razorpay payment id / link id"
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """
    Payment-gateway audit log: every Razorpay interaction (create-link
    request/response, checkout verify, webhook callbacks, errors) persisted to
    the `razorpay_transactions` collection, enriched with the linked order's
    customer/estimate info. Intended for finance reconciliation and debugging
    of failed / orphaned payments.
    """
    razorpay_transactions = db["razorpay_transactions"]

    match_conditions: dict = {}

    if action:
        match_conditions["action"] = action

    date_filter = {}
    if start_date:
        date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    if end_date:
        date_filter["$lte"] = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
    if date_filter:
        match_conditions["created_at"] = date_filter

    if search:
        term = search.strip()
        or_conditions = [
            {"razorpay_payment_id": {"$regex": re.escape(term), "$options": "i"}},
            {"payment_link_id": {"$regex": re.escape(term), "$options": "i"}},
        ]
        # order_id is stored as an ObjectId when valid; match both forms.
        if ObjectId.is_valid(term):
            or_conditions.append({"order_id": ObjectId(term)})
        or_conditions.append({"order_id": term})
        match_conditions["$or"] = or_conditions

    pipeline = [
        {"$match": match_conditions},
        {"$sort": {"created_at": -1}},
        # Join the order to surface customer / estimate context for the payment.
        {
            "$lookup": {
                "from": "orders",
                "localField": "order_id",
                "foreignField": "_id",
                "as": "order_info",
            }
        },
        {"$unwind": {"path": "$order_info", "preserveNullAndEmptyArrays": True}},
    ]

    count_pipeline = list(pipeline)
    count_pipeline.append({"$count": "total"})
    total_count_result = list(razorpay_transactions.aggregate(count_pipeline))
    total_count = total_count_result[0]["total"] if total_count_result else 0

    pipeline.append({"$skip": page * limit})
    pipeline.append({"$limit": limit})
    pipeline.append(
        {
            "$project": {
                "action": 1,
                "order_id": {"$toString": "$order_id"},
                "razorpay_payment_id": 1,
                "razorpay_order_id": 1,
                "payment_link_id": 1,
                "status": 1,
                "status_code": 1,
                "reason": 1,
                "error": 1,
                "customer_name": "$order_info.customer_name",
                "estimate_number": "$order_info.estimate_number",
                "order_total": "$order_info.total_amount",
                "payment_status": "$order_info.payment.status",
                "created_at": {
                    "$dateToString": {
                        "date": "$created_at",
                        "format": "%Y-%m-%d %H:%M:%S",
                        "timezone": "Asia/Kolkata",
                    }
                },
            }
        }
    )

    txns = [serialize_mongo_document(doc) for doc in razorpay_transactions.aggregate(pipeline)]

    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

    # Distinct action list for the frontend filter dropdown.
    actions = razorpay_transactions.distinct("action")

    return {
        "payments": txns,
        "total_count": total_count,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
        "actions": sorted(a for a in actions if a),
    }


@router.get("/orders/export")
async def export_orders(
    response: Response,
    sales_person: Optional[str] = Query(None),  # Fix 1: Match parameter type
    status: Optional[str] = Query(None),
    estimate_created: Optional[bool] = Query(None),
    spreadsheet_created: Optional[bool] = Query(None),
    amount: Optional[str] = Query(None),  # Fix 2: Change to Optional[str]
    estimate_number: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    hide_empty: bool = Query(True),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    # Build the match stage based on filters
    match_stage = {"$match": {}}
    second_match_stage = {"$match": {}}
    date_filter = {}

    # Hide "empty" orders (no customer / nothing in the cart) by default, mirroring
    # the /admin/orders table. A search bypasses it so any order can be exported.
    if hide_empty and not search:
        match_stage["$match"]["$and"] = [
            {"customer_id": {"$exists": True, "$nin": [None, ""]}},
            {
                "$or": [
                    {"products": {"$elemMatch": {"quantity": {"$gt": 0}}}},
                    {"products": {"$elemMatch": {"pre_order_quantity": {"$gt": 0}}}},
                ]
            },
        ]

    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        date_filter["$gte"] = start_date
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
        date_filter["$lte"] = end_date

    if date_filter:
        match_stage["$match"]["created_at"] = date_filter
    # Estimate number search
    if estimate_number:
        match_stage["$match"]["estimate_number"] = {
            "$regex": f"^{re.escape(estimate_number.strip())}",
            "$options": "i",
        }
    if status:
        match_stage["$match"]["status"] = status.lower()

    if estimate_created is not None:
        match_stage["$match"]["estimate_created"] = estimate_created

    if spreadsheet_created is not None:
        match_stage["$match"]["spreadsheet_created"] = spreadsheet_created

    if amount:
        match_stage["$match"]["total_amount"] = {"$gt": 0}
    print(sales_person)
    if sales_person:
        # Assuming 'created_by_info.name' is the field to filter
        second_match_stage["$match"]["Sales Person Code"] = sales_person

    # Free-text search — mirrors the /admin/orders table search. Order-level
    # fields are matched pre-lookup; sales person (name/code) is matched
    # post-lookup on the projected fields (see second_match_stage below).
    if search:
        term = search.strip()
        rx = {"$regex": re.escape(term), "$options": "i"}
        or_search = [
            {"customer_name": rx},
            {"estimate_number": rx},
            {"pre_order_estimate_number": rx},
            {"reference_number": rx},
        ]
        if ObjectId.is_valid(term):
            or_search.append({"_id": ObjectId(term)})
        match_stage["$match"]["$or"] = or_search
    # Now build our aggregation pipeline
    pipeline = [
        match_stage,
        {"$sort": {"created_at": -1}},  # sort descending by created_at
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        # Unwind the created_by_info array so it's a single object
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
        # Convert created_at (UTC) to a string in IST
        {
            "$project": {
                # Keep the original fields (except created_by_info is now an object)
                "Sales Person Name": "$created_by_info.name",
                "Sales Person Code": "$created_by_info.code",
                "Customer Name": "$customer_name",
                "Total Amount": "$total_amount",
                "Total GST": "$total_gst",
                "GST Type": "$gst_type",
                "Status": "$status",
                "Products": {"$size": {"$ifNull": ["$products", []]}},
                "Estimate Url": "$estimate_url",
                "Estimate Number": "$estimate_number",
                "Pre-Order Estimate Url": "$pre_order_estimate_url",
                "Pre-Order Estimate Number": "$pre_order_estimate_number",
                "Reference Number": "$reference_number",
                # ... include any other fields you want
                # Convert the "created_at" date to a string in IST
                "Created At": {
                    "$dateToString": {
                        "date": "$created_at",
                        "format": "%Y-%m-%d %H:%M:%S",  # date/time format
                        "timezone": "Asia/Kolkata",
                    }
                },
                "Updated At": {
                    "$dateToString": {
                        "date": "$updated_at",
                        "format": "%Y-%m-%d %H:%M:%S",  # date/time format
                        "timezone": "Asia/Kolkata",
                    }
                },
                "Shipping Address Address": "$shipping_address.address",
                "Shipping Address State": "$shipping_address.state",
                "Shipping Address City": "$shipping_address.city",
                "Billing Address Address": "$billing_address.address",
                "Billing Address State": "$billing_address.state",
                "Billing Address City": "$billing_address.city",
            }
        },
        second_match_stage,
    ]

    try:
        # Execute pipeline
        cursor = orders_collection.aggregate(pipeline)
        pipeline.append({"$count": "total"})
        total_count = list(orders_collection.aggregate(pipeline))
        total = total_count[0] if total_count else None
        total_count = total.get("total", 0)
        data = [serialize_mongo_document(doc) for doc in cursor]
    except OperationFailure as e:
        print(f"MongoDB aggregation failed: {e}")
        return Response(content="Export failed", status_code=500)

    if not data:
        return Response(content="No data found", status_code=404)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Orders")

        # Get worksheet and apply formatting
        worksheet = writer.sheets["Orders"]

        # Set column widths
        column_widths = {
            "A": 20,  # Estimate Number
            "B": 20,  # Created At
            "C": 25,  # Customer Name
            "D": 15,  # Status
            "E": 15,  # Sales Person
            "F": 15,  # Total Amount
            "G": 15,  # GST Amount
            "H": 15,  # Grand Total
            "I": 20,  # Reference Number
            "J": 15,  # Products Count
        }

        for col, width in column_widths.items():
            worksheet.column_dimensions[col].width = width

    # Prepare response
    output.seek(0)
    filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    return Response(content=output.getvalue())

@router.get("/payments_due")
def read_all_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    sales_person: str = Query(None, description="Filter by sales person"),
    invoice_number: str = Query(None, description="Filter by Invoice number"),
):
    """
    Retrieve all invoices past their due_date with pagination.
    page:  0-based page index
    limit: number of invoices per page
    """
    additional_conditions = []

    if sales_person:
        escaped_sales_person = re.escape(sales_person)
        additional_conditions.append({
            "$or": [
                {
                    "cf_sales_person": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
                {
                    "salesperson_name": {
                        "$regex": f"^{escaped_sales_person}$",
                        "$options": "i",
                    }
                },
            ]
        })

    if invoice_number:
        additional_conditions.append({
            "invoice_number": {
                "$regex": f"^{invoice_number.strip()}$",
                "$options": "i",
            }
        })

    extra_query = {"$and": additional_conditions} if additional_conditions else None

    # Fetch + sort happens in Python (see fetch_overdue_invoices docstring for why:
    # the mixed string/date due_date type makes Mongo's planner pick a slow index
    # for a combined $or query).
    matched = fetch_overdue_invoices(db, extra_query)
    total_count = len(matched)
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    page_docs = matched[page * limit : page * limit + limit]

    # Enrich only the current page's rows (not the whole matched set)
    invoice_numbers = [d.get("invoice_number") for d in page_docs]
    customer_ids = [d.get("customer_id") for d in page_docs]
    page_ids = [d.get("_id") for d in page_docs]

    associated_cns = fetch_associated_credit_notes(db, page_docs)

    notes_by_invoice = {
        n["invoice_number"]: n
        for n in db.invoice_notes.find({"invoice_number": {"$in": invoice_numbers}})
    }
    note_creator_ids = [
        n["created_by"] for n in notes_by_invoice.values() if n.get("created_by")
    ]
    users_by_id = {
        u["_id"]: u for u in db.users.find({"_id": {"$in": note_creator_ids}})
    }
    # line_items were excluded from the listing projection (they can be huge);
    # fetch them only for this page's invoices, for the admin drawer.
    line_items_by_id = {
        d["_id"]: d.get("line_items")
        for d in db.invoices.find(
            {"_id": {"$in": page_ids}}, {"line_items": 1}
        )
    }
    credit_note_totals = {}
    for row in db.credit_notes.aggregate([
        {"$match": {"customer_id": {"$in": customer_ids}, "status": {"$nin": ["void", "closed"]}}},
        {"$group": {"_id": "$customer_id", "total": {"$sum": {"$toDouble": {"$ifNull": ["$balance", 0]}}}}},
    ]):
        credit_note_totals[row["_id"]] = row["total"]

    inv = []
    for doc in page_docs:
        note = notes_by_invoice.get(doc.get("invoice_number"))
        creator = users_by_id.get(note.get("created_by")) if note else None
        due_date = doc.get("due_date")
        if isinstance(due_date, str):
            try:
                due_date = datetime.fromisoformat(due_date)
            except ValueError:
                pass
        item = {
            "_id": doc.get("_id"),
            "created_at": doc.get("created_at"),
            "total": doc.get("total"),
            "due_date": due_date,
            "balance": doc.get("balance"),
            "status": "overdue",
            "cf_sales_person": doc.get("cf_sales_person"),
            "salesperson_name": doc.get("salesperson_name"),
            "customer_id": doc.get("customer_id"),
            "customer_name": doc.get("customer_name"),
            "invoice_url": doc.get("invoice_url"),
            "invoice_number": doc.get("invoice_number"),
            "invoice_id": doc.get("invoice_id"),
            "line_items": line_items_by_id.get(doc.get("_id")),
            "created_by_name": doc.get("created_by_name"),
            "overdue_by_days": doc.get("overdue_by_days"),
            "invoice_notes": note,
            "open_credit_note_amt": credit_note_totals.get(doc.get("customer_id"), 0),
            "associated_credit_notes": associated_cns.get(doc.get("invoice_id"), []),
            "note_created_by_name": creator.get("first_name") if creator else None,
        }
        inv.append(serialize_mongo_document(item))

    return {
        "invoices": inv,
        "total_count": total_count,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
    }


@router.get("/payments_due/aging_stats")
def get_payments_due_aging_stats(
    sales_person: str = Query(None, description="Filter by sales person"),
):
    """
    Return aggregate aging bucket stats (counts + balances) across ALL overdue invoices.
    Buckets: 0-30, 31-60, 61+ days overdue.
    """
    extra_query = None
    if sales_person:
        escaped = re.escape(sales_person)
        extra_query = {"$or": [
            {"cf_sales_person": {"$regex": f"^{escaped}$", "$options": "i"}},
            {"salesperson_name": {"$regex": f"^{escaped}$", "$options": "i"}},
        ]}

    matched = fetch_overdue_invoices(db, extra_query)
    result = {
        "current": {"count": 0, "balance": 0},
        "overdue30": {"count": 0, "balance": 0},
        "overdue60": {"count": 0, "balance": 0},
    }
    for doc in matched:
        days = doc["overdue_by_days"]
        key = "current" if days <= 30 else "overdue30" if days <= 60 else "overdue60"
        result[key]["count"] += 1
        result[key]["balance"] += float(doc.get("balance") or 0)

    for key in result:
        result[key]["balance"] = round(result[key]["balance"], 2)
    return result


@router.get("/payments_due/download_xlsx")
def download_payments_due_xlsx(sales_person: str):
    """
    Download all invoices past their due_date (and not paid) as an XLSX file,
    matching the "Payment Due Sheet" sample format.
    """
    extra_query = None
    if sales_person:
        extra_query = {"$or": [
            {"cf_sales_person": sales_person},
            {"salesperson_name": sales_person},
        ]}

    matched = fetch_overdue_invoices(db, extra_query)

    invoice_numbers = [d.get("invoice_number") for d in matched]
    customer_ids = [d.get("customer_id") for d in matched]

    associated_cns = fetch_associated_credit_notes(db, matched)

    notes_by_invoice = {
        n["invoice_number"]: n
        for n in db.invoice_notes.find({"invoice_number": {"$in": invoice_numbers}})
    }
    credit_note_totals = {}
    for row in db.credit_notes.aggregate([
        {"$match": {"customer_id": {"$in": customer_ids}, "status": {"$nin": ["void", "closed"]}}},
        {"$group": {"_id": "$customer_id", "total": {"$sum": {"$toDouble": {"$ifNull": ["$balance", 0]}}}}},
    ]):
        credit_note_totals[row["_id"]] = row["total"]

    invoices = []
    for doc in matched:
        due_date = doc.get("due_date")
        if isinstance(due_date, str):
            try:
                due_date = datetime.fromisoformat(due_date)
            except ValueError:
                pass
        invoices.append(serialize_mongo_document({
            "created_at": doc.get("created_at"),
            "total": doc.get("total"),
            "due_date": due_date,
            "balance": doc.get("balance"),
            "status": "overdue",
            "cf_sales_person": doc.get("cf_sales_person"),
            "created_by_name": doc.get("created_by_name"),
            "salesperson_name": doc.get("salesperson_name"),
            "customer_id": doc.get("customer_id"),
            "customer_name": doc.get("customer_name"),
            "invoice_url": doc.get("invoice_url"),
            "invoice_number": doc.get("invoice_number"),
            "invoice_id": doc.get("invoice_id"),
            "overdue_by_days": doc.get("overdue_by_days"),
            "invoice_notes": notes_by_invoice.get(doc.get("invoice_number")),
            "open_credit_note_amt": credit_note_totals.get(doc.get("customer_id"), 0),
            "associated_credit_notes": associated_cns.get(doc.get("invoice_id"), []),
        }))

    # Build the XLSX workbook matching the sample "Payment Due Sheet" format
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Payments Due"

    headers = [
        "Created At",
        "Due Date",
        "Invoice Number",
        "Overdue by Days",
        "Customer Name",
        "Status",
        "CF Sales Person",
        "Invoice Sales Person",
        "Created By",
        "Total",
        "Balance",
        "SP Remarks",
        "Payment cleared - Details",
        "Expected Payment date",
        "Remarks Office Team",
        "Open Credit Note Amt.",
        "Associated Credit Notes",
        "Additional Information",
        "Images",
    ]
    ws.append(headers)

    def _fmt_date(value):
        if not value:
            return ""
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-%d")
        return str(value)

    def _fmt_credit_notes(cns):
        # e.g. "CN-00257 - ₹542.64 (closed); CN-00301 - ₹100 (open)"
        parts = []
        for cn in cns or []:
            num = cn.get("creditnote_number", "")
            total = cn.get("balance") if cn.get("balance") is not None else cn.get("total", 0)
            status = cn.get("status", "")
            parts.append(f"{num} - ₹{total} ({status})")
        return "; ".join(parts)

    for invoice in invoices:
        notes = invoice.get("invoice_notes") or {}
        ws.append(
            [
                _fmt_date(invoice.get("created_at")),
                _fmt_date(invoice.get("due_date")),
                invoice.get("invoice_number"),
                invoice.get("overdue_by_days"),
                invoice.get("customer_name"),
                invoice.get("status"),
                invoice.get("cf_sales_person") or invoice.get("salesperson_name", "-"),
                invoice.get("salesperson_name"),
                invoice.get("created_by_name"),
                invoice.get("total"),
                invoice.get("balance"),
                notes.get("sp_remarks", ""),
                notes.get("payment_cleared_details", ""),
                _fmt_date(notes.get("expected_payment_date")),
                notes.get("office_team_remarks", ""),
                invoice.get("open_credit_note_amt", 0),
                _fmt_credit_notes(invoice.get("associated_credit_notes")),
                notes.get("additional_info", ""),
                ", ".join(notes.get("images", []) or []),
            ]
        )

    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    filename = f"payments_due_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/sales-people")
def get_sales_people():
    """
    Retrieve a list of sales people from the users collection.
    Assuming users have a role or designation that identifies them as sales people.
    """
    # Replace 'sales' with the actual role identifier
    sales_people_cursor = users_collection.find(
        {"role": "sales_person"}, {"code": 1, "_id": 1, "name": 1}
    )
    sales_people = [serialize_mongo_document(user) for user in sales_people_cursor]
    return {"sales_people": sales_people}


@router.put("/customers/bulk-update")
async def bulk_update_customers(payload: dict):
    """
    Bulk update multiple customers in one request.
    Expects a JSON body:
    {
      "updates": [
        {
          "_id": "someCustomerId",
          "cf_sales_person": "SP1, SP2",
          ...
        },
        ...
      ]
    }
    """
    updates = payload.get("updates", [])
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    updated_count = 0
    results = []

    for item in updates:
        customer_id = item.get("_id")
        if not customer_id:
            # Skip if no _id
            results.append(
                {
                    "status": "skipped - missing _id",
                }
            )
            continue

        # ------------------------------------------------------------
        # 1) Prepare the update_data (exclude _id, skip None)
        # ------------------------------------------------------------
        update_data = {k: v for k, v in item.items() if k != "_id" and v is not None}
        if not update_data:
            results.append(
                {"customer_id": customer_id, "status": "skipped - no valid fields"}
            )
            continue

        # Convert cf_sales_person from comma-string to a list, if present
        if "cf_sales_person" in update_data:
            if isinstance(update_data["cf_sales_person"], str):
                update_data["cf_sales_person"] = [
                    s.strip() for s in update_data["cf_sales_person"].split(",")
                ]

        # ------------------------------------------------------------
        # 2) Check existing record & compare old vs new cf_sales_person
        # ------------------------------------------------------------
        existing_customer = db.customers.find_one({"_id": ObjectId(customer_id)})
        if not existing_customer:
            results.append({"customer_id": customer_id, "status": "not found"})
            continue

        old_cf_sales_person = existing_customer.get("cf_sales_person", [])
        if not isinstance(old_cf_sales_person, list):
            # If old was a string, convert it for consistent comparison
            old_cf_sales_person = [
                s.strip() for s in str(old_cf_sales_person).split(",") if s.strip()
            ]

        new_cf_sales_person = update_data.get("cf_sales_person", old_cf_sales_person)

        # ------------------------------------------------------------
        # 3) Perform the MongoDB update
        # ------------------------------------------------------------
        result = db.customers.update_one(
            {"_id": ObjectId(customer_id)},
            {"$set": update_data},
        )
        if result.matched_count == 0:
            results.append({"customer_id": customer_id, "status": "not found"})
            continue

        updated_count += 1
        results.append(
            {
                "customer_id": customer_id,
                "status": "updated",
                "update_data": update_data,
            }
        )

        # ------------------------------------------------------------
        # 4) Only if new cf_sales_person differs from old, call Zoho
        # ------------------------------------------------------------
        if old_cf_sales_person != new_cf_sales_person:
            payload_zoho = {
                "custom_fields": [
                    {
                        "value": (
                            new_cf_sales_person
                            if new_cf_sales_person and new_cf_sales_person[0] != ""
                            else []
                        ),
                        "customfield_id": "3220178000221198007",
                        "label": "Sales person",
                        "index": 11,
                    }
                ]
            }
            zoho_response = requests.put(
                url=f"https://www.zohoapis.com/books/v3/contacts/{existing_customer.get('contact_id')}?organization_id={org_id}",
                headers={
                    "Authorization": f"Zoho-oauthtoken {get_access_token('books')}"
                },
                json=payload_zoho,
            )
            print(payload_zoho)
            print(zoho_response.json().get("message"))

    return {
        "message": f"Bulk update complete. {updated_count} customers updated.",
        "results": results,
    }


@router.post("/upload-image")
async def upload_image(file: UploadFile = File(...), product_id: str = Form(...)):
    # Validate file type
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    # Validate file size
    file.file.seek(0, 2)  # Move the cursor to the end of the file
    file_size = file.file.tell()  # Get the current position (file size in bytes)
    file.file.seek(0)  # Reset the cursor to the beginning of the file

    if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"File is too large. Maximum allowed size is {MAX_FILE_SIZE_MB} MB.",
        )

    try:
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        # Generate a unique filename with timestamp to avoid conflicts
        file_extension = os.path.splitext(file.filename)[1]
        timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
        unique_filename = (
            f"product_images/{product.get('item_id')}_{timestamp}{file_extension}"
        )

        # Upload the file to S3
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            unique_filename,
            ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
        )

        # Construct the S3 URL
        s3_url = f"{AWS_S3_URL}/{unique_filename}"

        if s3_url:
            # Get current images array or initialize empty array
            current_images = product.get("images", [])

            # Add new image to the end of the array
            updated_images = current_images + [s3_url]

            # Keep image_url in sync with the first image in the array
            primary_image = updated_images[0]

            # Update the product with the new images array
            products_collection.update_one(
                {"_id": ObjectId(product_id)},
                {"$set": {"images": updated_images, "image_url": primary_image}},
            )

            return {
                "image_url": s3_url,
                "images": updated_images,
                "message": "Image uploaded successfully",
            }

    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading file to S3.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    finally:
        file.file.close()


@router.post("/reorder-images")
async def reorder_images(product_id: str = Form(...), images: str = Form(...)):
    """
    Reorder images for a product.
    images should be a JSON string array of image URLs in the desired order.
    """
    try:
        # Parse the images JSON string
        import json

        images_list = json.loads(images)

        if not isinstance(images_list, list):
            raise HTTPException(status_code=400, detail="Images must be an array.")

        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        # Update the product with reordered images
        result = products_collection.update_one(
            {"_id": ObjectId(product_id)}, {"$set": {"images": images_list}}
        )

        if result.modified_count > 0:
            return {"images": images_list, "message": "Images reordered successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to reorder images.")

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format for images.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.delete("/delete-image")
async def delete_image(product_id: str = Form(...), image_url: str = Form(...)):
    """
    Delete a specific image from a product and remove it from S3.
    """
    try:
        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        current_images = product.get("images", [])

        if image_url not in current_images:
            raise HTTPException(status_code=404, detail="Image not found in product.")

        # Remove image from array
        updated_images = [img for img in current_images if img != image_url]
        new_primary = updated_images[0] if updated_images else None

        # Update the product
        update_fields = {"images": updated_images, "image_url": new_primary}
        result = products_collection.update_one(
            {"_id": ObjectId(product_id)}, {"$set": update_fields}
        )

        if result.modified_count > 0:
            # Try to delete from S3 (optional - you might want to keep files for backup)
            try:
                # Extract S3 key from URL
                s3_key = image_url.replace(f"{AWS_S3_URL}/", "")
                s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=s3_key)
            except Exception as s3_error:
                # Log the error but don't fail the request
                print(f"Warning: Could not delete file from S3: {s3_error}")

            return {"images": updated_images, "message": "Image deleted successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to delete image.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/make-primary-image")
async def make_primary_image(product_id: str = Form(...), image_url: str = Form(...)):
    """
    Make a specific image the primary image (move it to first position).
    """
    try:
        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        current_images = product.get("images", [])

        if image_url not in current_images:
            raise HTTPException(status_code=404, detail="Image not found in product.")

        # Remove the image from its current position
        updated_images = [img for img in current_images if img != image_url]
        # Insert it at the beginning (primary position)
        updated_images.insert(0, image_url)

        # Update the product
        result = products_collection.update_one(
            {"_id": ObjectId(product_id)},
            {"$set": {"images": updated_images, "image_url": image_url}},
        )

        if result.modified_count > 0:
            return {
                "images": updated_images,
                "primary_image": image_url,
                "message": "Primary image updated successfully",
            }
        else:
            raise HTTPException(
                status_code=400, detail="Failed to update primary image."
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# Optional: Bulk upload endpoint for multiple images at once
@router.post("/upload-multiple-images")
async def upload_multiple_images(
    files: List[UploadFile] = File(...), product_id: str = Form(...)
):
    """
    Upload multiple images at once for a product.
    """
    if len(files) > 10:  # Limit to prevent abuse
        raise HTTPException(
            status_code=400, detail="Maximum 10 files allowed per upload."
        )

    try:
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        uploaded_urls = []
        current_images = product.get("images", [])

        for file in files:
            # Validate each file
            if not file.content_type.startswith("image/"):
                continue  # Skip non-image files

            # Check file size
            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)

            if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                continue  # Skip oversized files

            # Generate unique filename
            file_extension = os.path.splitext(file.filename)[1]
            timestamp = int(time.time() * 1000)
            unique_filename = (
                f"product_images/{product.get('item_id')}_{timestamp}_{file.filename}"
            )

            # Upload to S3
            s3_client.upload_fileobj(
                file.file,
                AWS_S3_BUCKET_NAME,
                unique_filename,
                ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
            )

            # Construct S3 URL
            s3_url = f"{AWS_S3_URL}/{unique_filename}"
            uploaded_urls.append(s3_url)

            file.file.close()

        if uploaded_urls:
            # Add all new images to the existing array
            updated_images = current_images + uploaded_urls

            # Update the product
            products_collection.update_one(
                {"_id": ObjectId(product_id)}, {"$set": {"images": updated_images}}
            )

            return {
                "uploaded_images": uploaded_urls,
                "total_images": len(updated_images),
                "images": updated_images,
                "message": f"Successfully uploaded {len(uploaded_urls)} images",
            }
        else:
            raise HTTPException(
                status_code=400, detail="No valid images were uploaded."
            )

    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading files to S3.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    finally:
        # Ensure all files are closed
        for file in files:
            if hasattr(file, "file") and not file.file.closed:
                file.file.close()


# ===== VIDEO MANAGEMENT ENDPOINTS =====

@router.post("/upload-video")
async def upload_video(file: UploadFile = File(...), product_id: str = Form(...)):
    """
    Upload a video to S3 and add it to the product's videos array.
    """
    # Validate file type
    if not file.content_type.startswith("video/"):
        raise HTTPException(status_code=400, detail="Invalid file type. Must be a video.")

    # Validate file size (50MB max for videos)
    MAX_VIDEO_SIZE_MB = 50
    file.file.seek(0, 2)
    file_size = file.file.tell()
    file.file.seek(0)

    if file_size > MAX_VIDEO_SIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"File is too large. Maximum allowed size is {MAX_VIDEO_SIZE_MB} MB.",
        )

    try:
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        # Generate a unique filename with timestamp
        file_extension = os.path.splitext(file.filename)[1]
        timestamp = int(time.time() * 1000)
        unique_filename = (
            f"product_videos/{product.get('item_id')}_{timestamp}{file_extension}"
        )

        # Upload the file to S3
        s3_client.upload_fileobj(
            file.file,
            AWS_S3_BUCKET_NAME,
            unique_filename,
            ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
        )

        # Construct the S3 URL
        s3_url = f"{AWS_S3_URL}/{unique_filename}"

        if s3_url:
            # Get current videos array or initialize empty array
            current_videos = product.get("videos", [])

            # Add new video to the end of the array
            updated_videos = current_videos + [s3_url]

            # Update the product with the new videos array
            products_collection.update_one(
                {"_id": ObjectId(product_id)}, {"$set": {"videos": updated_videos}}
            )

            return {
                "video_url": s3_url,
                "videos": updated_videos,
                "message": "Video uploaded successfully",
            }

    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading file to S3.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    finally:
        file.file.close()


@router.post("/reorder-videos")
async def reorder_videos(product_id: str = Form(...), videos: str = Form(...)):
    """
    Reorder videos for a product.
    videos should be a JSON string array of video URLs in the desired order.
    """
    try:
        import json

        videos_list = json.loads(videos)

        if not isinstance(videos_list, list):
            raise HTTPException(status_code=400, detail="Videos must be an array.")

        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        # Update the product with reordered videos
        result = products_collection.update_one(
            {"_id": ObjectId(product_id)}, {"$set": {"videos": videos_list}}
        )

        if result.modified_count > 0:
            return {"videos": videos_list, "message": "Videos reordered successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to reorder videos.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.delete("/delete-video")
async def delete_video(product_id: str = Form(...), video_url: str = Form(...)):
    """
    Delete a specific video from a product and remove it from S3.
    """
    try:
        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        current_videos = product.get("videos", [])

        if video_url not in current_videos:
            raise HTTPException(status_code=404, detail="Video not found in product.")

        # Remove video from array
        updated_videos = [vid for vid in current_videos if vid != video_url]

        # Update the product
        result = products_collection.update_one(
            {"_id": ObjectId(product_id)}, {"$set": {"videos": updated_videos}}
        )

        if result.modified_count > 0:
            # Try to delete from S3
            try:
                s3_key = video_url.replace(f"{AWS_S3_URL}/", "")
                s3_client.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=s3_key)
            except Exception as s3_error:
                print(f"Warning: Could not delete file from S3: {s3_error}")

            return {"videos": updated_videos, "message": "Video deleted successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to delete video.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/upload-multiple-videos")
async def upload_multiple_videos(
    files: List[UploadFile] = File(...), product_id: str = Form(...)
):
    """
    Upload multiple videos at once to a product.
    """
    MAX_VIDEO_SIZE_MB = 50

    try:
        # Validate that product exists
        product = products_collection.find_one({"_id": ObjectId(product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")

        uploaded_urls = []
        current_videos = product.get("videos", [])

        for file in files:
            # Validate file type
            if not file.content_type.startswith("video/"):
                continue  # Skip non-video files

            # Validate file size
            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)

            if file_size > MAX_VIDEO_SIZE_MB * 1024 * 1024:
                continue  # Skip files that are too large

            # Generate unique filename
            file_extension = os.path.splitext(file.filename)[1]
            timestamp = int(time.time() * 1000)
            unique_filename = (
                f"product_videos/{product.get('item_id')}_{timestamp}{file_extension}"
            )

            try:
                # Upload to S3
                s3_client.upload_fileobj(
                    file.file,
                    AWS_S3_BUCKET_NAME,
                    unique_filename,
                    ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
                )

                s3_url = f"{AWS_S3_URL}/{unique_filename}"
                uploaded_urls.append(s3_url)

            except Exception as upload_error:
                print(f"Error uploading {file.filename}: {upload_error}")
                continue

        if uploaded_urls:
            # Add all new videos to the existing array
            updated_videos = current_videos + uploaded_urls

            # Update the product
            products_collection.update_one(
                {"_id": ObjectId(product_id)}, {"$set": {"videos": updated_videos}}
            )

            return {
                "total_videos": len(updated_videos),
                "videos": updated_videos,
                "message": f"Successfully uploaded {len(uploaded_urls)} videos",
            }
        else:
            raise HTTPException(
                status_code=400, detail="No valid videos were uploaded."
            )

    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading files to S3.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    finally:
        for file in files:
            if hasattr(file, "file") and not file.file.closed:
                file.file.close()


def slugify(brand: str):
    return brand.lower().replace(" ", "_") if len(brand.split()) >= 2 else brand.lower()


@router.get("/brands_with_images")
def get_all_brands(search: Optional[str] = Query(None)):  # Make search optional
    """
    Retrieve a list of all distinct brands with associated image URLs.
    """
    try:
        condition = {}
        if search:
            # Case-insensitive regex search
            condition["name"] = {"$regex": search, "$options": "i"}

        brands = list(db.brands.find(condition))
        print(serialize_mongo_document(brands))
        return {"brands": serialize_mongo_document(brands)}
    except Exception as e:
        print("Failed to fetch brands from MongoDB.")
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.get("/brands/refresh")
def refresh_brands():  # Make search optional
    try:
        brands = products_collection.distinct(
            "brand",
            {"stock": {"$gt": 0}, "status": "active", "is_deleted": {"$exists": False}},
        )
        product_brands = [brand for brand in brands if brand]

        for brand in product_brands:
            exists = db.brands.find_one({"name": brand})
            if not exists:
                db.brands.insert_one({"name": brands, "image_url": ""})

        return "Updated Brands Collection"
    except Exception as e:
        print("Failed to fetch brands from MongoDB.")
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.put("/brands/image")
async def update_brand_image(file: UploadFile = File(...), brand_name: str = Form(...)):
    # Input validation
    if not brand_name or not brand_name.strip():
        raise HTTPException(status_code=400, detail="Brand name is required")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    brand_name = brand_name.strip()
    print(f"Processing brand: {brand_name}")

    # Environment variables
    S3_URL_BASE = os.getenv("S3_URL")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    if not s3_client or not S3_BUCKET_NAME or not S3_URL_BASE:
        raise HTTPException(
            status_code=500, detail="S3 configuration missing or failed to initialize."
        )

    # File extension validation
    file_extension = (
        file.filename.split(".")[-1].lower() if "." in file.filename else "svg"
    )
    ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png", "svg", "webp", "gif"}

    if file_extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    # Read and validate file content
    try:
        file_content = await file.read()

        # File size validation
        MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB limit
        if len(file_content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")

        if len(file_content) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error reading file: {e}")
        raise HTTPException(status_code=400, detail="Failed to read uploaded file")

    # S3 upload
    image_s3_key = f"brands/{slugify(brand_name.lower())}.{file_extension}"

    try:
        file_obj = io.BytesIO(file_content)

        s3_client.upload_fileobj(
            file_obj,  # Now this is a file-like object
            S3_BUCKET_NAME,
            image_s3_key,
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": file.content_type,
            },
        )
        print(f"Successfully uploaded {image_s3_key} to S3.")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        print(f"S3 upload failed: {error_code} - {e}")
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {error_code}")
    except Exception as e:
        print(f"Unexpected error during S3 upload: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload image to S3")

    # Database update
    new_image_url = (
        f"{S3_URL_BASE}/brands/{slugify(brand_name.lower())}.{file_extension}"
    )
    try:
        update_result = db.brands.update_one(
            {"name": brand_name},
            {"$set": {"image_url": new_image_url}},
        )

        print(
            f"MongoDB update for brand '{brand_name}': Matched {update_result.matched_count}, Modified {update_result.modified_count}."
        )

        if update_result.matched_count == 0:
            print(f"No brand found for '{brand_name}' to update image for.")
            return {
                "message": f"Image uploaded for '{brand_name}', but no brand was found to update.",
                "image_url": new_image_url,
            }

        return {
            "message": f"Brand image updated successfully for '{brand_name}'.",
            "image_url": new_image_url,
            "matched_count": update_result.matched_count,
            "modified_count": update_result.modified_count,
        }

    except Exception as e:
        print(f"Database update error for brand '{brand_name}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update database: {e}")


@router.put("/brands/secondary_image")
async def update_brand_secondary_image(file: UploadFile = File(...), brand_name: str = Form(...)):
    if not brand_name or not brand_name.strip():
        raise HTTPException(status_code=400, detail="Brand name is required")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    brand_name = brand_name.strip()

    S3_URL_BASE = os.getenv("S3_URL")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    if not s3_client or not S3_BUCKET_NAME or not S3_URL_BASE:
        raise HTTPException(status_code=500, detail="S3 configuration missing or failed to initialize.")

    file_extension = file.filename.split(".")[-1].lower() if "." in file.filename else "jpg"
    ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png", "svg", "webp", "gif"}

    if file_extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Invalid file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}")

    try:
        file_content = await file.read()
        if len(file_content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")
        if len(file_content) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail="Failed to read uploaded file")

    image_s3_key = f"brands/{slugify(brand_name.lower())}_secondary.{file_extension}"

    try:
        s3_client.upload_fileobj(
            io.BytesIO(file_content),
            S3_BUCKET_NAME,
            image_s3_key,
            ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e.response['Error']['Code']}")
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to upload image to S3")

    new_image_url = f"{S3_URL_BASE}/brands/{slugify(brand_name.lower())}_secondary.{file_extension}"

    try:
        db.brands.update_one(
            {"name": brand_name},
            {"$set": {"secondary_image_url": new_image_url}},
        )
        return {"message": f"Brand secondary image updated for '{brand_name}'.", "secondary_image_url": new_image_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update database: {e}")


@router.put("/brands/{brand_id}")
async def update_brand(brand_id: str, payload: dict):
    from bson import ObjectId

    allowed_fields = {"description", "status", "hidden"}
    update_data = {k: v for k, v in payload.items() if k in allowed_fields}

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    try:
        result = db.brands.update_one(
            {"_id": ObjectId(brand_id)},
            {"$set": update_data},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Brand not found")
        return {"message": "Brand updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update brand: {e}")


@router.put("/products/{product_id}")
async def update_product(
    product_id: str,
    name: Optional[str] = Form(None),
    brand: Optional[str] = Form(None),
    category: Optional[str] = Form(None),
    sub_category: Optional[str] = Form(None),
    series: Optional[str] = Form(None),
    cf_sku_code: Optional[str] = Form(None),
    upc_code: Optional[str] = Form(None),  # Added this missing parameter
    catalogue_page: Optional[str] = Form(None),  # Also added this one if needed
    rate: Optional[float] = Form(None),
    stock: Optional[int] = Form(None),
    status: Optional[str] = Form(None),
    catalogue_order: Optional[int] = Form(None),
    pre_order: Optional[str] = Form(None),
    clearance: Optional[str] = Form(None),
    clearance_margin: Optional[float] = Form(None),
    files: Optional[List[UploadFile]] = File(None),
    replace_images: Optional[bool] = Form(False)  # Whether to replace all images or append
):
    """
    Update a product by ID with the provided fields and optionally upload multiple images.
    
    Parameters:
    - replace_images: If True, replaces all existing images. If False, appends new images.
    """
    try:
        # Validate ObjectId format
        if not ObjectId.is_valid(product_id):
            raise HTTPException(status_code=400, detail="Invalid product ID format")

        # Convert string ID to ObjectId
        object_id = ObjectId(product_id)

        # Check if product exists
        existing_product = products_collection.find_one({"_id": object_id})
        if not existing_product:
            raise HTTPException(status_code=404, detail="Product not found")

        # Prepare update data for product fields
        update_dict = {}
        
        # Build update dict from form fields
        form_data = {
            "name": name,
            "brand": brand,
            "category": category,
            "sub_category": sub_category,
            "series": series,
            "cf_sku_code": cf_sku_code,
            "upc_code": upc_code,
            "catalogue_page": catalogue_page,
            "rate": rate,
            "stock": stock,
            "status": status,
            "catalogue_order": catalogue_order
        }

        for field, value in form_data.items():
            if value is not None:
                update_dict[field] = value

        if pre_order is not None:
            update_dict["pre_order"] = pre_order.lower() == "true"

        if clearance is not None:
            update_dict["clearance"] = clearance.lower() == "true"

        # clearance_margin is sent as 0 (not null) to clear it, so this is safe
        if clearance_margin is not None:
            update_dict["clearance_margin"] = clearance_margin

        # Handle image uploads if files are provided
        uploaded_image_urls = []
        if files:
            # Limit number of files
            if len(files) > 10:
                raise HTTPException(
                    status_code=400, detail="Maximum 10 files allowed per upload."
                )

            for file in files:
                # Validate file type
                if not file.content_type.startswith("image/"):
                    continue  # Skip non-image files

                # Validate file size
                file.file.seek(0, 2)
                file_size = file.file.tell()
                file.file.seek(0)

                if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
                    continue  # Skip oversized files

                # Generate unique filename
                file_extension = os.path.splitext(file.filename)[1]
                timestamp = int(time.time() * 1000)
                unique_filename = (
                    f"product_images/{existing_product.get('item_id')}_{timestamp}_{file.filename}"
                )

                try:
                    # Upload to S3
                    s3_client.upload_fileobj(
                        file.file,
                        AWS_S3_BUCKET_NAME,
                        unique_filename,
                        ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
                    )

                    # Construct S3 URL
                    s3_url = f"{AWS_S3_URL}/{unique_filename}"
                    uploaded_image_urls.append(s3_url)

                except Exception as upload_error:
                    print(f"Error uploading file {file.filename}: {upload_error}")
                    continue  # Skip this file and continue with others

                finally:
                    file.file.close()

            # Update images array
            if uploaded_image_urls:
                current_images = existing_product.get("images", [])
                
                if replace_images:
                    # Replace all existing images
                    update_dict["images"] = uploaded_image_urls
                else:
                    # Append new images to existing ones
                    update_dict["images"] = current_images + uploaded_image_urls

        # Add updated_at timestamp
        update_dict["updated_at"] = datetime.now()

        # Perform the update
        if update_dict:  # Only update if there are fields to update
            result = products_collection.update_one(
                {"_id": object_id},
                {"$set": update_dict}
            )

            if result.modified_count == 0:
                return JSONResponse({
                    "message": "No changes were made to the product",
                    "product_id": product_id
                })

        # Fetch and return the updated product
        updated_product = products_collection.find_one({"_id": object_id})
        serialized_product = serialize_mongo_document(updated_product)

        response_data = {
            "message": "Product updated successfully",
            "product": serialized_product
        }

        # Add upload summary if images were processed
        if files:
            response_data["upload_summary"] = {
                "uploaded_images_count": len(uploaded_image_urls),
                "uploaded_images": uploaded_image_urls,
                "replace_mode": replace_images
            }

        return JSONResponse(response_data)

    except HTTPException:
        # Re-raise HTTP exceptions (like 400, 404)
        raise
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not configured.")
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail="Error uploading files to S3.")
    except Exception as e:
        print(f"Error updating product {product_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        # Ensure all file handles are closed
        if files:
            for file in files:
                if hasattr(file, "file") and not file.file.closed:
                    file.file.close()
    
router.include_router(
    admin_special_margins_router,
    prefix="/customer/special_margins",
    tags=["Admin Sales People"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_inventory_aging_router,
    prefix="/inventory_aging",
    tags=["Admin Inventory Aging"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_salespeople_router,
    prefix="/salespeople",
    tags=["Admin Sales People"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_catalogues_router,
    prefix="/catalogues",
    tags=["Admin Catalogues"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_trainings_router,
    prefix="/trainings",
    tags=["Admin Trainings"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_announcements_router,
    prefix="/announcements",
    tags=["Admin Announcments"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_daily_visits_router,
    prefix="/daily_visits",
    tags=["Admin Daily Visits"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_hooks_router,
    prefix="/hooks",
    tags=["Admin Hooks And Categories"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_hooks_categories_router,
    prefix="/hooks_categories",
    tags=["Admin Hooks And Categories"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_potential_customers_router,
    prefix="/potential_customers",
    tags=["Admin Potential Customers"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_expected_reorders_router,
    prefix="/expected_reorders",
    tags=["Admin Expected Reorders"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_targeted_customers_router,
    prefix="/targeted_customers",
    tags=["Admin Targeted Customers"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_return_orders_router,
    prefix="/return_orders",
    tags=["Admin Return Orders"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_sales_by_customer_router,
    prefix="/sales_by_customer",
    tags=["Admin Sales By Customer"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_external_links_router,
    prefix="/external_links",
    tags=["Admin External Links"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_linktree_router,
    prefix="/linktree",
    tags=["Admin Link Tree"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_business_cards_router,
    prefix="/cards",
    tags=["Admin Business Cards"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_customer_analytics_router,
    prefix="/customer_analytics",
    tags=["Admin Customer Analytics"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_order_analytics_router,
    prefix="/order_analytics",
    tags=["Admin Order Analytics"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_attendance_router,
    prefix="/attendance",
    tags=["Employee Attendance"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_catalogue_leads_router,
    prefix="/catalogue_leads",
    tags=["Catalogue Leads"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_brand_leads_router,
    prefix="/brand_leads",
    tags=["Brand Leads"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_b2b_registrations_router,
    prefix="/b2b_registrations",
    tags=["B2B Registrations"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_users_router,
    prefix="/users",
    tags=["Customer Management"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_careers_router,
    prefix="/careers",
    tags=["Admin Careers"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_career_applications_router,
    prefix="/career_applications",
    tags=["Admin Career Applications"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_contact_submissions_router,
    prefix="/contact_submissions",
    tags=["Admin Contact Submissions"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_chats_router,
    prefix="/chats",
    tags=["Admin Chats"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_chatbot_customers_router,
    prefix="/chatbot_customers",
    tags=["Admin Chatbot Customers"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_templates_router,
    prefix="/templates",
    tags=["Admin WhatsApp Templates"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_segments_router,
    prefix="/segments",
    tags=["Admin Customer Segments"],
    dependencies=[Depends(JWTBearer())],
)
router.include_router(
    admin_campaigns_router,
    prefix="/campaigns",
    tags=["Admin WhatsApp Campaigns"],
    dependencies=[Depends(JWTBearer())],
)


# ─── Pre-order bulk management ────────────────────────────────────────────────

@router.get("/pre-order/brands", dependencies=[Depends(JWTBearer())])
def get_pre_order_brands():
    """Return brands that have a vendor_id (i.e. can have purchase orders)."""
    brands = list(db.brands.find(
        {"vendor_id": {"$exists": True, "$ne": None, "$ne": ""}},
        {"name": 1, "vendor_id": 1, "_id": 0}
    ).sort("name", 1))
    return [{"name": b["name"], "vendor_id": b["vendor_id"]} for b in brands]


@router.get("/pre-order/purchase-orders", dependencies=[Depends(JWTBearer())])
def get_pre_order_purchase_orders(brand: str = Query(...)):
    """Return purchase orders for the given brand(s)'s vendor(s), newest first.
    brand may be a single name or comma-separated names (e.g. "Dogfest,Catfest")."""
    brand_names = [b.strip() for b in brand.split(",") if b.strip()]
    vendor_ids = []
    for bname in brand_names:
        doc = db.brands.find_one({"name": bname}, {"vendor_id": 1})
        if doc and doc.get("vendor_id"):
            vendor_ids.append(doc["vendor_id"])
    if not vendor_ids:
        raise HTTPException(status_code=404, detail="Brand not found or has no vendor")
    pos = list(db.purchase_orders.find(
        {"vendor_id": {"$in": vendor_ids}, "status": {"$nin": ["cancelled"]}},
        {"purchaseorder_number": 1, "date": 1, "status": 1, "total": 1, "_id": 0}
    ).sort("date", -1).limit(50))
    return serialize_mongo_document(pos)


@router.get("/pre-order/line-items", dependencies=[Depends(JWTBearer())])
def get_pre_order_line_items(po_number: str = Query(...)):
    """Return line items for a PO, enriched with pre_order status and upcoming_stock from products."""
    po = db.purchase_orders.find_one(
        {"purchaseorder_number": po_number},
        {"line_items": 1, "_id": 0}
    )
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    line_items = [li for li in po.get("line_items", []) if li.get("item_id")]
    if not line_items:
        return []

    # Logistics dates for this PO (inward / ETA at port), if tracked in brand_orders
    brand_order = db.brand_orders.find_one(
        {"purchaseorder_number": po_number},
        {"inward_date": 1, "eta_port_date": 1, "_id": 0}
    ) or {}
    inward_date = brand_order.get("inward_date")
    eta_port_date = brand_order.get("eta_port_date")

    item_ids = [li["item_id"] for li in line_items]
    products_map = {
        p["item_id"]: p
        for p in db.products.find(
            {"item_id": {"$in": item_ids}},
            {"item_id": 1, "pre_order": 1, "cf_sku_code": 1, "name": 1, "_id": 0}
        )
        if p.get("item_id")
    }

    result = []
    for li in line_items:
        iid = li.get("item_id")
        prod = products_map.get(iid, {})
        qty = float(li.get("quantity") or 0)
        qty_received = float(li.get("quantity_received") or 0)
        result.append({
            "item_id": iid,
            "name": li.get("name") or prod.get("name") or "",
            "sku": prod.get("cf_sku_code") or "",
            "quantity": qty,
            "quantity_received": qty_received,
            "upcoming_stock": max(0, int(qty - qty_received)),
            "pre_order": prod.get("pre_order", False),
            "in_products": bool(prod),
            "inward_date": inward_date,
            "eta_port_date": eta_port_date,
        })
    return result


@router.post("/pre-order/mark", dependencies=[Depends(JWTBearer())])
def mark_pre_order_products(payload: dict):
    """
    Mark item_ids as pre_order=True.
    If unmark_others=True, set pre_order=False on remaining items from the same PO.
    """
    item_ids = payload.get("item_ids", [])
    po_number = payload.get("po_number", "")
    unmark_others = payload.get("unmark_others", True)

    marked = 0
    unmarked = 0

    if item_ids:
        res = db.products.update_many(
            {"item_id": {"$in": item_ids}},
            {"$set": {"pre_order": True}}
        )
        marked = res.modified_count

    if unmark_others and po_number:
        po = db.purchase_orders.find_one(
            {"purchaseorder_number": po_number},
            {"line_items": 1}
        )
        if po:
            all_ids = [li["item_id"] for li in po.get("line_items", []) if li.get("item_id")]
            unmark_ids = [iid for iid in all_ids if iid not in item_ids]
            if unmark_ids:
                res2 = db.products.update_many(
                    {"item_id": {"$in": unmark_ids}},
                    {"$set": {"pre_order": False}}
                )
                unmarked = res2.modified_count

    return {"marked": marked, "unmarked": unmarked}


# ── App settings (admin-configurable) ────────────────────────────────────────
from .app_settings import get_settings as _get_app_settings, update_settings as _update_app_settings


@router.get("/settings")
def admin_get_settings():
    """Return the admin-configurable app settings (e.g. min order value)."""
    return _get_app_settings()


@router.put("/settings")
def admin_update_settings(payload: dict):
    """Update app settings. Only known keys are persisted."""
    return _update_app_settings(payload or {})
