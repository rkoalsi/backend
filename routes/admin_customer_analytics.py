from fastapi import APIRouter, Query, HTTPException, Path
from fastapi.responses import JSONResponse, StreamingResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from dotenv import load_dotenv
import boto3, logging, os, openpyxl
from datetime import datetime
from typing import Optional
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from io import BytesIO

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]

logger = logging.getLogger(__name__)
logger.propagate = False

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


@router.get("")
def get_admin_customer_analytics(
    status: Optional[str] = Query(None, description="Filter by invoice status"),
    tier: Optional[str] = Query(None, description="Filter by tiers (A,B,C)"),
    due_status: Optional[str] = Query(
        "all", description="Filter by Payments Due (all, due, not_due)"
    ),
    last_billed: Optional[str] = Query(
        "all",
        description="Filter by last billing activity (all, last_month, last_45_days, last_2_months, last_3_months, not_last_month, not_last_45_days, not_last_2_months, not_last_3_months)",
    ),
    sort_by: Optional[bool] = Query(True, description="Low to High or High to Low"),
):
    try:

        # Build the match stage dynamically
        match_stage = {
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
        customer_status_match_stage = {}
        sort_stage = {"$sort": {"totalSalesCurrentMonth": 1}}

        # Add status filter if provided, otherwise use default exclusions
        if status == "all":
            pass
        elif status:
            customer_status_match_stage["customerDetails.status"] = status

        # Add salesperson filter if provided

        if sort_by:
            sort_stage["$sort"] = [
                {"totalSalesCurrentMonth": -1},
            ]

        sales_person_logic = {
            "$cond": [
                {"$ne": ["$salesperson_name", None]},
                "$salesperson_name",
                "$cf_sales_person",
            ]
        }
        if tier and len(tier) == 1:
            customer_status_match_stage["customerDetails.cf_tier"] = {
                "$regex": f"^{tier}$",
                "$options": "i",
            }

        # Get current date for dynamic calculations
        from datetime import datetime

        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # Calculate current financial year
        if current_month >= 4:  # April onwards
            current_fy_start_year = current_year
        else:  # January-March
            current_fy_start_year = current_year - 1

        # Calculate last and previous financial years
        last_fy_start_year = current_fy_start_year - 1
        previous_fy_start_year = current_fy_start_year - 2

        # Calculate total COMPLETED months passed in current FY (excluding current month)
        if current_month >= 4:
            # Same calendar year as FY start
            completed_months_in_current_fy = (
                current_month - 4
            )  # Don't add +1 since we exclude current month
        else:
            # Next calendar year (Jan-Mar)
            completed_months_in_current_fy = (
                12 - 4
            ) + current_month  # (Apr-Dec) + (Jan-current month, excluding current)

        # Ensure we have at least 1 month to avoid division by zero
        completed_months_in_current_fy = max(1, completed_months_in_current_fy)

        # Complete aggregation pipeline
        pipeline = [
            # Stage 1: Filter invoices from April 1, 2023 onwards and only paid invoices
            {"$match": match_stage},
            # Stage 2: Add computed fields for date analysis
            {
                "$addFields": {
                    "invoiceDate": {"$dateFromString": {"dateString": "$date"}},
                    "year": {"$year": {"$dateFromString": {"dateString": "$date"}}},
                    "month": {"$month": {"$dateFromString": {"dateString": "$date"}}},
                    "yearMonth": {
                        "$concat": [
                            {
                                "$toString": {
                                    "$year": {
                                        "$dateFromString": {"dateString": "$date"}
                                    }
                                }
                            },
                            "-",
                            {
                                "$cond": {
                                    "if": {
                                        "$lt": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            10,
                                        ]
                                    },
                                    "then": {
                                        "$concat": [
                                            "0",
                                            {
                                                "$toString": {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                }
                                            },
                                        ]
                                    },
                                    "else": {
                                        "$toString": {
                                            "$month": {
                                                "$dateFromString": {
                                                    "dateString": "$date"
                                                }
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    },
                    # Add payment categorization logic
                    "isDuePayment": {
                        "$not": {"$in": ["$status", ["void", "draft", "sent", "paid"]]}
                    },
                    "isNotDuePayment": {
                        "$not": {
                            "$in": ["$status", ["void", "overdue", "partially_paid"]]
                        }
                    },
                    # Check if invoice is in current month (dynamic)
                    "isCurrentMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_year,
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_month,
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in completed months (dynamic: current FY start to last month)
                    "isCompletedMonth": {
                        "$or": (
                            [
                                # Same FY year, from April to previous month
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                4,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                    ]
                                },
                                # Next calendar year (if current month is Jan-Mar), from Jan to previous month
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year + 1,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                1,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                        {
                                            "$lte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                3,
                                            ]
                                        },
                                    ]
                                },
                            ]
                            if current_month > 4
                            else [
                                # If current month is April or earlier, only check same calendar year
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                4,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                    ]
                                }
                            ]
                        )
                    },
                    # Check if invoice is in current financial year (dynamic)
                    "isCurrentFY": {
                        "$or": [
                            # From April onwards in FY start year
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            # Jan-Mar in next calendar year (if FY spans two calendar years)
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in last financial year (dynamic)
                    "isLastFY": {
                        "$or": [
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            last_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            last_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in previous financial year (dynamic)
                    "isPreviousFY": {
                        "$or": [
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            previous_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            previous_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Normalize city name to handle variations (with null handling) - Updated to use shipping_address
                    "normalizedCity": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(bangalore|bengaluru)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "bengaluru",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(mumbai|bombay)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "mumbai",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(delhi|new delhi)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "delhi",
                                },
                            ],
                            "default": {
                                "$toLower": {
                                    "$ifNull": [
                                        "$shipping_address.city",
                                        "unknown_city",
                                    ]
                                }
                            },
                        }
                    },
                    # Combine street + street2 into full normalized street address (remove punctuation, extra spaces)
                    "normalizedFullStreet": {
                        "$trim": {
                            "input": {
                                "$replaceAll": {
                                    "input": {
                                        "$replaceAll": {
                                            "input": {
                                                "$replaceAll": {
                                                    "input": {
                                                        "$toLower": {
                                                            "$concat": [
                                                                {"$ifNull": ["$shipping_address.street", ""]},
                                                                " ",
                                                                {"$ifNull": ["$shipping_address.street2", ""]}
                                                            ]
                                                        }
                                                    },
                                                    "find": ",",
                                                    "replacement": " "
                                                }
                                            },
                                            "find": ".",
                                            "replacement": " "
                                        }
                                    },
                                    "find": "  ",
                                    "replacement": " "
                                }
                            }
                        }
                    },
                    # Add fields to check for missing shipping address data
                    "hasShippingAddress": {"$ne": ["$shipping_address", None]},
                    "shippingAddressComplete": {
                        "$and": [
                            {"$ne": ["$shipping_address", None]},
                            {"$ne": ["$shipping_address.city", None]},
                            {"$ne": ["$shipping_address.city", ""]},
                            {"$ne": ["$shipping_address.state", None]},
                            {"$ne": ["$shipping_address.country", None]},
                        ]
                    },
                    # Billing period validations (dynamic)
                    # Last month (previous month from current)
                    "billedLastMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    (
                                        current_year
                                        if current_month > 1
                                        else current_year - 1
                                    ),
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_month - 1 if current_month > 1 else 12,
                                ]
                            },
                        ]
                    },
                    # Last 45 days (dynamic calculation)
                    "billedLast45Days": {
                        "$gte": [
                            {"$dateFromString": {"dateString": "$date"}},
                            {
                                "$dateFromString": {
                                    "dateString": f"{current_date.year}-{current_date.month:02d}-{max(1, current_date.day - 45):02d}"
                                }
                            },
                        ]
                    },
                    # Last 2 months (dynamic)
                    "billedLast2Months": {
                        "$or": [
                            # Current month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_year,
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_month,
                                        ]
                                    },
                                ]
                            },
                            # Previous month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 1
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 1
                                                if current_month > 1
                                                else 12
                                            ),
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Last 3 months (dynamic)
                    "billedLast3Months": {
                        "$or": [
                            # Current month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_year,
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_month,
                                        ]
                                    },
                                ]
                            },
                            # Previous month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 1
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 1
                                                if current_month > 1
                                                else 12
                                            ),
                                        ]
                                    },
                                ]
                            },
                            # Month before that
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 2
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 2
                                                if current_month > 2
                                                else (12 + current_month - 2)
                                            ),
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                }
            },
            # Stage 3: Group by customer name AND address (all fields normalized)
            # Note: Uses normalizedFullStreet (street + street2 combined) to handle data entry variations
            {
                "$group": {
                    "_id": {
                        "customerNameLower": {"$toLower": {"$trim": {"input": "$customer_name"}}},
                        "city": "$normalizedCity",
                        "state": {"$toLower": {"$trim": {"input": {"$ifNull": ["$shipping_address.state", ""]}}}},
                        "zip": {"$replaceAll": {"input": {"$ifNull": ["$shipping_address.zip", ""]}, "find": " ", "replacement": ""}},
                        "fullStreet": "$normalizedFullStreet",
                    },
                    "customerId": {"$first": "$customer_id"},
                    "customerName": {"$first": "$customer_name"},
                    "shippingAddress": {"$first": "$shipping_address"},
                    # Updated logic: Get the salesperson field that matches the sp_code
                    "salesPerson": {"$first": sales_person_logic},
                    # NEW: Collect ALL invoices for validation
                    "allInvoices": {
                        "$push": {
                            "_id": "$_id",
                            "invoice_number": "$invoice_number",
                            "date": "$date",
                            "due_date": "$due_date",
                            "status": "$status",
                            "total": "$total",
                            "balance": "$balance",
                            "customer_id": "$customer_id",
                            "invoice_id": "$invoice_id",
                            "yearMonth": "$yearMonth",
                            "isCurrentMonth": "$isCurrentMonth",
                            "isCurrentFY": "$isCurrentFY",
                            "isLastFY": "$isLastFY",
                            "isPreviousFY": "$isPreviousFY",
                        }
                    },
                    # Collect payment information
                    "duePayments": {
                        "$push": {
                            "$cond": [
                                "$isDuePayment",
                                {
                                    "_id": "$_id",
                                    "date": "$date",
                                    "due_date": "$due_date",
                                    "invoice_number": "$invoice_number",
                                    "status": "$status",
                                    "invoice_id": "$invoice_id",
                                    "total": "$total",
                                    "balance": "$balance",
                                },
                                "$$REMOVE",
                            ]
                        }
                    },
                    "notDuePayments": {
                        "$push": {
                            "$cond": [
                                "$isNotDuePayment",
                                {
                                    "_id": "$_id",
                                    "date": "$date",
                                    "due_date": "$due_date",
                                    "invoice_number": "$invoice_number",
                                    "status": "$status",
                                    "invoice_id": "$invoice_id",
                                    "balance": "$balance",
                                    "total": "$total",
                                },
                                "$$REMOVE",
                            ]
                        }
                    },
                    # Total sales current month (August 2025)
                    "totalSalesCurrentMonth": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isCurrentMonth", True]}, "$total", 0]
                        }
                    },
                    # Last bill date
                    "lastBillDate": {"$max": "$invoiceDate"},
                    # FIXED: Count of ALL orders in current financial year for frequency calculation
                    "currentFYOrders": {
                        "$sum": {"$cond": [{"$eq": ["$isCurrentFY", True]}, 1, 0]}
                    },
                    # FIXED: Unique months in current financial year for frequency calculation
                    "currentFYMonths": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$isCurrentFY", True]},
                                "$yearMonth",
                                None,
                            ]
                        }
                    },
                    # Keep completed month orders for backward compatibility (if needed elsewhere)
                    "completedMonthOrders": {
                        "$sum": {"$cond": [{"$eq": ["$isCompletedMonth", True]}, 1, 0]}
                    },
                    # Keep completed months for backward compatibility (if needed elsewhere)
                    "completedMonths": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$isCompletedMonth", True]},
                                "$yearMonth",
                                None,
                            ]
                        }
                    },
                    # Total billing current year (April 2025 onwards)
                    "billingTillDateCurrentYear": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isCurrentFY", True]}, "$total", 0]
                        }
                    },
                    # Total sales last financial year (April 2024 - March 2025)
                    "totalSalesLastFY": {
                        "$sum": {"$cond": [{"$eq": ["$isLastFY", True]}, "$total", 0]}
                    },
                    # Total sales previous financial year (April 2023 - March 2024)
                    "totalSalesPreviousFY": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isPreviousFY", True]}, "$total", 0]
                        }
                    },
                    # Billing validation checks
                    "hasBilledLastMonth": {
                        "$sum": {"$cond": [{"$eq": ["$billedLastMonth", True]}, 1, 0]}
                    },
                    "hasBilledLast45Days": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast45Days", True]}, 1, 0]}
                    },
                    "hasBilledLast2Months": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast2Months", True]}, 1, 0]}
                    },
                    "hasBilledLast3Months": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast3Months", True]}, 1, 0]}
                    },
                    # All invoice dates for additional analysis
                    "allInvoiceDates": {"$push": "$invoiceDate"},
                }
            },
            # Stage 4: Lookup customer details from customers collection
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "customerId",
                    "foreignField": "contact_id",
                    "as": "customerDetails",
                }
            },
            {"$match": customer_status_match_stage},
            # Stage 5: Calculate average order frequency and billing validations
            {
                "$addFields": {
                    # FIXED: Remove null values from currentFYMonths array (for frequency calculation)
                    "currentFYMonthsFiltered": {
                        "$filter": {
                            "input": "$currentFYMonths",
                            "cond": {"$ne": ["$$this", None]},
                        }
                    },
                    # Keep the old logic for backward compatibility
                    "completedMonthsFiltered": {
                        "$filter": {
                            "input": "$completedMonths",
                            "cond": {"$ne": ["$$this", None]},
                        }
                    },
                    # Extract customer status and tier from lookup
                    "customerStatus": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.status", 0]},
                            "unknown",
                        ]
                    },
                    "customerTier": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.cf_tier", 0]},
                            "unknown",
                        ]
                    },
                    # Get sales person from customer record (overrides invoice sales person)
                    "salesPerson": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.cf_sales_person", 0]},
                            "$salesPerson",  # Fallback to invoice sales person if customer doesn't have one
                        ]
                    },
                    # Boolean flags for billing periods (true = HAS billed)
                    "hasBilledLastMonth": {"$gt": ["$hasBilledLastMonth", 0]},
                    "hasBilledLast45Days": {"$gt": ["$hasBilledLast45Days", 0]},
                    "hasBilledLast2Months": {"$gt": ["$hasBilledLast2Months", 0]},
                    "hasBilledLast3Months": {"$gt": ["$hasBilledLast3Months", 0]},
                }
            },
            # Stage 6: Calculate final metrics
            {
                "$addFields": {
                    # FIXED: Calculate frequency using total orders divided by completed months in FY
                    "averageOrderFrequencyMonthly": {
                        "$cond": [
                            {"$gt": ["$currentFYOrders", 0]},
                            {
                                "$divide": [
                                    "$currentFYOrders",
                                    completed_months_in_current_fy,
                                ]
                            },
                            0,
                        ]
                    },
                    # Format shipping address as string (updated from billing to shipping)
                    "shippingAddressFormatted": {
                        "$concat": [
                            {"$ifNull": ["$shippingAddress.street", ""]},
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.street2", ""]},
                                    {"$concat": [", ", "$shippingAddress.street2"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.city", ""]},
                                    {"$concat": [", ", "$shippingAddress.city"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.state", ""]},
                                    {"$concat": [", ", "$shippingAddress.state"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.zip", ""]},
                                    {"$concat": [" - ", "$shippingAddress.zip"]},
                                    "",
                                ]
                            },
                        ]
                    },
                }
            },
            # Stage 7: Filter based on due_status if not "all"
            {
                "$addFields": {
                    # Apply filtering based on due_status parameter
                    "filteredDuePayments": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": [due_status, "all"]},
                                    {"$eq": [due_status, "due"]},
                                ]
                            },
                            "$duePayments",
                            [],
                        ]
                    },
                    "filteredNotDuePayments": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": [due_status, "all"]},
                                    {"$eq": [due_status, "not_due"]},
                                ]
                            },
                            "$notDuePayments",
                            [],
                        ]
                    },
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {
                                "$eq": [due_status, "all"]
                            },  # Include all customers when due_status is "all"
                            {
                                "$and": [
                                    {"$eq": [due_status, "due"]},
                                    {
                                        "$gt": [{"$size": "$filteredDuePayments"}, 0]
                                    },  # Only customers with due payments
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [due_status, "not_due"]},
                                    {
                                        "$gt": [{"$size": "$filteredNotDuePayments"}, 0]
                                    },  # Only customers with not due payments
                                ]
                            },
                        ]
                    }
                }
            },
            # NEW Stage 8: Filter based on last_billed parameter
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": [last_billed, "all"]},  # Include all customers
                            # Positive filters (customers who HAVE billed in specific periods)
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_month"]},
                                    {"$eq": ["$hasBilledLastMonth", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_45_days"]},
                                    {"$eq": ["$hasBilledLast45Days", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_2_months"]},
                                    {"$eq": ["$hasBilledLast2Months", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_3_months"]},
                                    {"$eq": ["$hasBilledLast3Months", True]},
                                ]
                            },
                            # Negative filters (customers who have NOT billed in specific periods)
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_month"]},
                                    {"$eq": ["$hasBilledLastMonth", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_45_days"]},
                                    {"$eq": ["$hasBilledLast45Days", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_2_months"]},
                                    {"$eq": ["$hasBilledLast2Months", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_3_months"]},
                                    {"$eq": ["$hasBilledLast3Months", False]},
                                ]
                            },
                        ]
                    }
                }
            },
            # Stage 9: Project final output format
            {
                "$project": {
                    "_id": 0,
                    "customerName": 1,
                    "shippingAddress": "$shippingAddressFormatted",  # Updated to use shipping address
                    "status": "$customerStatus",
                    "tier": "$customerTier",
                    "totalSalesCurrentMonth": {
                        "$round": ["$totalSalesCurrentMonth", 2]
                    },
                    "lastBillDate": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$lastBillDate"}
                    },
                    "averageOrderFrequencyMonthly": {
                        "$round": ["$averageOrderFrequencyMonthly", 2]
                    },
                    "billingTillDateCurrentYear": {
                        "$round": ["$billingTillDateCurrentYear", 2]
                    },
                    "totalSalesLastFY": {"$round": ["$totalSalesLastFY", 2]},
                    "totalSalesPreviousFY": {"$round": ["$totalSalesPreviousFY", 2]},
                    "salesPerson": 1,
                    "hasBilledLastMonth": 1,
                    "hasBilledLast45Days": 1,
                    "hasBilledLast2Months": 1,
                    "hasBilledLast3Months": 1,
                    # Include payment lists
                    "duePayments": "$filteredDuePayments",
                    "notDuePayments": "$filteredNotDuePayments",
                    # NEW: Include all invoices for validation
                    "allInvoices": 1,
                    # Additional fields for validation
                    "totalInvoiceCount": {"$size": "$allInvoices"},
                    "currentFYInvoiceCount": "$currentFYOrders",
                }
            },
            # Stage 11: Sort by customer name
            {"$sort": {"customerName": 1}},
        ]

        customers = list(db.invoices.aggregate(pipeline))
        return serialize_mongo_document(customers)
    except Exception as e:
        logger.error(f"Error in get_customer_analytics: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/report")
def download_customer_analytics_report(
    status: Optional[str] = Query(None, description="Filter by invoice status"),
    tier: Optional[str] = Query(None, description="Filter by tiers (A,B,C)"),
    due_status: Optional[str] = Query(
        "all", description="Filter by Payments Due (all, due, not_due)"
    ),
    last_billed: Optional[str] = Query(
        "all",
        description="Filter by last billing activity (all, last_month, last_45_days, last_2_months, last_3_months, not_last_month, not_last_45_days, not_last_2_months, not_last_3_months)",
    ),
    sort_by: Optional[bool] = Query(True, description="Low to High or High to Low"),
):
    try:
        # Build the match stage dynamically
        match_stage = {
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
        customer_status_match_stage = {}
        sort_stage = {"$sort": {"totalSalesCurrentMonth": 1}}

        # Add status filter if provided, otherwise use default exclusions
        if status == "all":
            pass
        elif status:
            customer_status_match_stage["customerDetails.status"] = status

        if sort_by:
            sort_stage["$sort"] = [
                {"totalSalesCurrentMonth": -1},
            ]

        sales_person_logic = {
            "$cond": [
                {"$ne": ["$salesperson_name", None]},
                "$salesperson_name",
                "$cf_sales_person",
            ]
        }
        if tier and len(tier) == 1:
            customer_status_match_stage["customerDetails.cf_tier"] = {
                "$regex": f"^{tier}$",
                "$options": "i",
            }

        # Get current date for dynamic calculations
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # Calculate current financial year
        if current_month >= 4:  # April onwards
            current_fy_start_year = current_year
        else:  # January-March
            current_fy_start_year = current_year - 1

        # Calculate last and previous financial years
        last_fy_start_year = current_fy_start_year - 1
        previous_fy_start_year = current_fy_start_year - 2

        # Calculate total COMPLETED months passed in current FY (excluding current month)
        if current_month >= 4:
            # Same calendar year as FY start
            completed_months_in_current_fy = (
                current_month - 4
            )  # Don't add +1 since we exclude current month
        else:
            # Next calendar year (Jan-Mar)
            completed_months_in_current_fy = (
                12 - 4
            ) + current_month  # (Apr-Dec) + (Jan-current month, excluding current)

        # Ensure we have at least 1 month to avoid division by zero
        completed_months_in_current_fy = max(1, completed_months_in_current_fy)

        # Complete aggregation pipeline
        pipeline = [
            # Stage 1: Filter invoices from April 1, 2023 onwards and only paid invoices
            {"$match": match_stage},
            # Stage 2: Add computed fields for date analysis
            {
                "$addFields": {
                    "invoiceDate": {"$dateFromString": {"dateString": "$date"}},
                    "year": {"$year": {"$dateFromString": {"dateString": "$date"}}},
                    "month": {"$month": {"$dateFromString": {"dateString": "$date"}}},
                    "yearMonth": {
                        "$concat": [
                            {
                                "$toString": {
                                    "$year": {
                                        "$dateFromString": {"dateString": "$date"}
                                    }
                                }
                            },
                            "-",
                            {
                                "$cond": {
                                    "if": {
                                        "$lt": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            10,
                                        ]
                                    },
                                    "then": {
                                        "$concat": [
                                            "0",
                                            {
                                                "$toString": {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                }
                                            },
                                        ]
                                    },
                                    "else": {
                                        "$toString": {
                                            "$month": {
                                                "$dateFromString": {
                                                    "dateString": "$date"
                                                }
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    },
                    # Add payment categorization logic
                    "isDuePayment": {
                        "$not": {"$in": ["$status", ["void", "draft", "sent", "paid"]]}
                    },
                    "isNotDuePayment": {
                        "$not": {
                            "$in": ["$status", ["void", "overdue", "partially_paid"]]
                        }
                    },
                    # Check if invoice is in current month (dynamic)
                    "isCurrentMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_year,
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_month,
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in completed months (dynamic: current FY start to last month)
                    "isCompletedMonth": {
                        "$or": (
                            [
                                # Same FY year, from April to previous month
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                4,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                    ]
                                },
                                # Next calendar year (if current month is Jan-Mar), from Jan to previous month
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year + 1,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                1,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                        {
                                            "$lte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                3,
                                            ]
                                        },
                                    ]
                                },
                            ]
                            if current_month > 4
                            else [
                                # If current month is April or earlier, only check same calendar year
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                {
                                                    "$year": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_fy_start_year,
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                4,
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                {
                                                    "$month": {
                                                        "$dateFromString": {
                                                            "dateString": "$date"
                                                        }
                                                    }
                                                },
                                                current_month,
                                            ]
                                        },
                                    ]
                                }
                            ]
                        )
                    },
                    # Check if invoice is in current financial year (dynamic)
                    "isCurrentFY": {
                        "$or": [
                            # From April onwards in FY start year
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            # Jan-Mar in next calendar year (if FY spans two calendar years)
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in last financial year (dynamic)
                    "isLastFY": {
                        "$or": [
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            last_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            last_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in previous financial year (dynamic)
                    "isPreviousFY": {
                        "$or": [
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            previous_fy_start_year,
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            4,
                                        ]
                                    },
                                ]
                            },
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            previous_fy_start_year + 1,
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            3,
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Normalize city name to handle variations (with null handling) - Updated to use shipping_address
                    "normalizedCity": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(bangalore|bengaluru)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "bengaluru",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(mumbai|bombay)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "mumbai",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$ne": ["$shipping_address.city", None]},
                                            {"$ne": ["$shipping_address.city", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$shipping_address.city",
                                                            "",
                                                        ]
                                                    },
                                                    "regex": "^(delhi|new delhi)$",
                                                    "options": "i",
                                                }
                                            },
                                        ]
                                    },
                                    "then": "delhi",
                                },
                            ],
                            "default": {
                                "$toLower": {
                                    "$ifNull": [
                                        "$shipping_address.city",
                                        "unknown_city",
                                    ]
                                }
                            },
                        }
                    },
                    # Combine street + street2 into full normalized street address (remove punctuation, extra spaces)
                    "normalizedFullStreet": {
                        "$trim": {
                            "input": {
                                "$replaceAll": {
                                    "input": {
                                        "$replaceAll": {
                                            "input": {
                                                "$replaceAll": {
                                                    "input": {
                                                        "$toLower": {
                                                            "$concat": [
                                                                {"$ifNull": ["$shipping_address.street", ""]},
                                                                " ",
                                                                {"$ifNull": ["$shipping_address.street2", ""]}
                                                            ]
                                                        }
                                                    },
                                                    "find": ",",
                                                    "replacement": " "
                                                }
                                            },
                                            "find": ".",
                                            "replacement": " "
                                        }
                                    },
                                    "find": "  ",
                                    "replacement": " "
                                }
                            }
                        }
                    },
                    # Add fields to check for missing shipping address data
                    "hasShippingAddress": {"$ne": ["$shipping_address", None]},
                    "shippingAddressComplete": {
                        "$and": [
                            {"$ne": ["$shipping_address", None]},
                            {"$ne": ["$shipping_address.city", None]},
                            {"$ne": ["$shipping_address.city", ""]},
                            {"$ne": ["$shipping_address.state", None]},
                            {"$ne": ["$shipping_address.country", None]},
                        ]
                    },
                    # Billing period validations (dynamic)
                    # Last month (previous month from current)
                    "billedLastMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    (
                                        current_year
                                        if current_month > 1
                                        else current_year - 1
                                    ),
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    current_month - 1 if current_month > 1 else 12,
                                ]
                            },
                        ]
                    },
                    # Last 45 days (dynamic calculation)
                    "billedLast45Days": {
                        "$gte": [
                            {"$dateFromString": {"dateString": "$date"}},
                            {
                                "$dateFromString": {
                                    "dateString": f"{current_date.year}-{current_date.month:02d}-{max(1, current_date.day - 45):02d}"
                                }
                            },
                        ]
                    },
                    # Last 2 months (dynamic)
                    "billedLast2Months": {
                        "$or": [
                            # Current month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_year,
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_month,
                                        ]
                                    },
                                ]
                            },
                            # Previous month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 1
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 1
                                                if current_month > 1
                                                else 12
                                            ),
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                    # Last 3 months (dynamic)
                    "billedLast3Months": {
                        "$or": [
                            # Current month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_year,
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            current_month,
                                        ]
                                    },
                                ]
                            },
                            # Previous month
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 1
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 1
                                                if current_month > 1
                                                else 12
                                            ),
                                        ]
                                    },
                                ]
                            },
                            # Month before that
                            {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$year": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_year
                                                if current_month > 2
                                                else current_year - 1
                                            ),
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            {
                                                "$month": {
                                                    "$dateFromString": {
                                                        "dateString": "$date"
                                                    }
                                                }
                                            },
                                            (
                                                current_month - 2
                                                if current_month > 2
                                                else (12 + current_month - 2)
                                            ),
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                }
            },
            # Stage 3: Group by customer name AND address (all fields normalized)
            # Note: Uses normalizedFullStreet (street + street2 combined) to handle data entry variations
            {
                "$group": {
                    "_id": {
                        "customerNameLower": {"$toLower": {"$trim": {"input": "$customer_name"}}},
                        "city": "$normalizedCity",
                        "state": {"$toLower": {"$trim": {"input": {"$ifNull": ["$shipping_address.state", ""]}}}},
                        "zip": {"$replaceAll": {"input": {"$ifNull": ["$shipping_address.zip", ""]}, "find": " ", "replacement": ""}},
                        "fullStreet": "$normalizedFullStreet",
                    },
                    "customerId": {"$first": "$customer_id"},
                    "customerName": {"$first": "$customer_name"},
                    "shippingAddress": {"$first": "$shipping_address"},
                    # Updated logic: Get the salesperson field that matches the sp_code
                    "salesPerson": {"$first": sales_person_logic},
                    # NEW: Collect ALL invoices for validation
                    "allInvoices": {
                        "$push": {
                            "_id": "$_id",
                            "invoice_number": "$invoice_number",
                            "date": "$date",
                            "due_date": "$due_date",
                            "status": "$status",
                            "total": "$total",
                            "balance": "$balance",
                            "customer_id": "$customer_id",
                            "invoice_id": "$invoice_id",
                            "yearMonth": "$yearMonth",
                            "isCurrentMonth": "$isCurrentMonth",
                            "isCurrentFY": "$isCurrentFY",
                            "isLastFY": "$isLastFY",
                            "isPreviousFY": "$isPreviousFY",
                        }
                    },
                    # Collect payment information
                    "duePayments": {
                        "$push": {
                            "$cond": [
                                "$isDuePayment",
                                {
                                    "_id": "$_id",
                                    "date": "$date",
                                    "due_date": "$due_date",
                                    "invoice_number": "$invoice_number",
                                    "status": "$status",
                                    "invoice_id": "$invoice_id",
                                    "total": "$total",
                                    "balance": "$balance",
                                },
                                "$$REMOVE",
                            ]
                        }
                    },
                    "notDuePayments": {
                        "$push": {
                            "$cond": [
                                "$isNotDuePayment",
                                {
                                    "_id": "$_id",
                                    "date": "$date",
                                    "due_date": "$due_date",
                                    "invoice_number": "$invoice_number",
                                    "status": "$status",
                                    "invoice_id": "$invoice_id",
                                    "balance": "$balance",
                                    "total": "$total",
                                },
                                "$$REMOVE",
                            ]
                        }
                    },
                    # Total sales current month (August 2025)
                    "totalSalesCurrentMonth": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isCurrentMonth", True]}, "$total", 0]
                        }
                    },
                    # Last bill date
                    "lastBillDate": {"$max": "$invoiceDate"},
                    # FIXED: Count of ALL orders in current financial year for frequency calculation
                    "currentFYOrders": {
                        "$sum": {"$cond": [{"$eq": ["$isCurrentFY", True]}, 1, 0]}
                    },
                    # FIXED: Unique months in current financial year for frequency calculation
                    "currentFYMonths": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$isCurrentFY", True]},
                                "$yearMonth",
                                None,
                            ]
                        }
                    },
                    # Keep completed month orders for backward compatibility (if needed elsewhere)
                    "completedMonthOrders": {
                        "$sum": {"$cond": [{"$eq": ["$isCompletedMonth", True]}, 1, 0]}
                    },
                    # Keep completed months for backward compatibility (if needed elsewhere)
                    "completedMonths": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$isCompletedMonth", True]},
                                "$yearMonth",
                                None,
                            ]
                        }
                    },
                    # Total billing current year (April 2025 onwards)
                    "billingTillDateCurrentYear": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isCurrentFY", True]}, "$total", 0]
                        }
                    },
                    # Total sales last financial year (April 2024 - March 2025)
                    "totalSalesLastFY": {
                        "$sum": {"$cond": [{"$eq": ["$isLastFY", True]}, "$total", 0]}
                    },
                    # Total sales previous financial year (April 2023 - March 2024)
                    "totalSalesPreviousFY": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isPreviousFY", True]}, "$total", 0]
                        }
                    },
                    # Billing validation checks
                    "hasBilledLastMonth": {
                        "$sum": {"$cond": [{"$eq": ["$billedLastMonth", True]}, 1, 0]}
                    },
                    "hasBilledLast45Days": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast45Days", True]}, 1, 0]}
                    },
                    "hasBilledLast2Months": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast2Months", True]}, 1, 0]}
                    },
                    "hasBilledLast3Months": {
                        "$sum": {"$cond": [{"$eq": ["$billedLast3Months", True]}, 1, 0]}
                    },
                    # All invoice dates for additional analysis
                    "allInvoiceDates": {"$push": "$invoiceDate"},
                }
            },
            # Stage 4: Lookup customer details from customers collection
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "customerId",
                    "foreignField": "contact_id",
                    "as": "customerDetails",
                }
            },
            {"$match": customer_status_match_stage},
            # Stage 5: Calculate average order frequency and billing validations
            {
                "$addFields": {
                    # FIXED: Remove null values from currentFYMonths array (for frequency calculation)
                    "currentFYMonthsFiltered": {
                        "$filter": {
                            "input": "$currentFYMonths",
                            "cond": {"$ne": ["$$this", None]},
                        }
                    },
                    # Keep the old logic for backward compatibility
                    "completedMonthsFiltered": {
                        "$filter": {
                            "input": "$completedMonths",
                            "cond": {"$ne": ["$$this", None]},
                        }
                    },
                    # Extract customer status and tier from lookup
                    "customerStatus": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.status", 0]},
                            "unknown",
                        ]
                    },
                    "customerTier": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.cf_tier", 0]},
                            "unknown",
                        ]
                    },
                    # Get sales person from customer record (overrides invoice sales person)
                    "salesPerson": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$customerDetails.cf_sales_person", 0]},
                            "$salesPerson",  # Fallback to invoice sales person if customer doesn't have one
                        ]
                    },
                    # Boolean flags for billing periods (true = HAS billed)
                    "hasBilledLastMonth": {"$gt": ["$hasBilledLastMonth", 0]},
                    "hasBilledLast45Days": {"$gt": ["$hasBilledLast45Days", 0]},
                    "hasBilledLast2Months": {"$gt": ["$hasBilledLast2Months", 0]},
                    "hasBilledLast3Months": {"$gt": ["$hasBilledLast3Months", 0]},
                }
            },
            # Stage 6: Calculate final metrics
            {
                "$addFields": {
                    # FIXED: Calculate frequency using total orders divided by completed months in FY
                    "averageOrderFrequencyMonthly": {
                        "$cond": [
                            {"$gt": ["$currentFYOrders", 0]},
                            {
                                "$divide": [
                                    "$currentFYOrders",
                                    completed_months_in_current_fy,
                                ]
                            },
                            0,
                        ]
                    },
                    # Format shipping address as string (updated from billing to shipping)
                    "shippingAddressFormatted": {
                        "$concat": [
                            {"$ifNull": ["$shippingAddress.street", ""]},
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.street2", ""]},
                                    {"$concat": [", ", "$shippingAddress.street2"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.city", ""]},
                                    {"$concat": [", ", "$shippingAddress.city"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.state", ""]},
                                    {"$concat": [", ", "$shippingAddress.state"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$shippingAddress.zip", ""]},
                                    {"$concat": [" - ", "$shippingAddress.zip"]},
                                    "",
                                ]
                            },
                        ]
                    },
                }
            },
            # Stage 7: Filter based on due_status if not "all"
            {
                "$addFields": {
                    # Apply filtering based on due_status parameter
                    "filteredDuePayments": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": [due_status, "all"]},
                                    {"$eq": [due_status, "due"]},
                                ]
                            },
                            "$duePayments",
                            [],
                        ]
                    },
                    "filteredNotDuePayments": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": [due_status, "all"]},
                                    {"$eq": [due_status, "not_due"]},
                                ]
                            },
                            "$notDuePayments",
                            [],
                        ]
                    },
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {
                                "$eq": [due_status, "all"]
                            },  # Include all customers when due_status is "all"
                            {
                                "$and": [
                                    {"$eq": [due_status, "due"]},
                                    {
                                        "$gt": [{"$size": "$filteredDuePayments"}, 0]
                                    },  # Only customers with due payments
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [due_status, "not_due"]},
                                    {
                                        "$gt": [{"$size": "$filteredNotDuePayments"}, 0]
                                    },  # Only customers with not due payments
                                ]
                            },
                        ]
                    }
                }
            },
            # NEW Stage 8: Filter based on last_billed parameter
            {
                "$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": [last_billed, "all"]},  # Include all customers
                            # Positive filters (customers who HAVE billed in specific periods)
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_month"]},
                                    {"$eq": ["$hasBilledLastMonth", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_45_days"]},
                                    {"$eq": ["$hasBilledLast45Days", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_2_months"]},
                                    {"$eq": ["$hasBilledLast2Months", True]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "last_3_months"]},
                                    {"$eq": ["$hasBilledLast3Months", True]},
                                ]
                            },
                            # Negative filters (customers who have NOT billed in specific periods)
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_month"]},
                                    {"$eq": ["$hasBilledLastMonth", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_45_days"]},
                                    {"$eq": ["$hasBilledLast45Days", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_2_months"]},
                                    {"$eq": ["$hasBilledLast2Months", False]},
                                ]
                            },
                            {
                                "$and": [
                                    {"$eq": [last_billed, "not_last_3_months"]},
                                    {"$eq": ["$hasBilledLast3Months", False]},
                                ]
                            },
                        ]
                    }
                }
            },
            # Stage 9: Project final output format
            {
                "$project": {
                    "_id": 0,
                    "customerName": 1,
                    "shippingAddress": "$shippingAddressFormatted",  # Updated to use shipping address
                    "status": "$customerStatus",
                    "tier": "$customerTier",
                    "totalSalesCurrentMonth": {
                        "$round": ["$totalSalesCurrentMonth", 2]
                    },
                    "lastBillDate": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$lastBillDate"}
                    },
                    "averageOrderFrequencyMonthly": {
                        "$round": ["$averageOrderFrequencyMonthly", 2]
                    },
                    "billingTillDateCurrentYear": {
                        "$round": ["$billingTillDateCurrentYear", 2]
                    },
                    "totalSalesLastFY": {"$round": ["$totalSalesLastFY", 2]},
                    "totalSalesPreviousFY": {"$round": ["$totalSalesPreviousFY", 2]},
                    "salesPerson": 1,
                    "hasBilledLastMonth": 1,
                    "hasBilledLast45Days": 1,
                    "hasBilledLast2Months": 1,
                    "hasBilledLast3Months": 1,
                    # Include payment lists
                    "duePayments": "$filteredDuePayments",
                    "notDuePayments": "$filteredNotDuePayments",
                    # NEW: Include all invoices for validation
                    "allInvoices": 1,
                    # Additional fields for validation
                    "totalInvoiceCount": {"$size": "$allInvoices"},
                    "currentFYInvoiceCount": "$currentFYOrders",
                }
            },
            # Stage 10: Sort by customer name
            {"$sort": {"customerName": 1}},
        ]

        # Execute the aggregation
        customers = list(db.invoices.aggregate(pipeline))

        # Create Excel file
        workbook = openpyxl.Workbook()
        worksheet = workbook.active
        worksheet.title = "Customer Analytics Report"

        # Define headers
        headers = [
            "Customer Name",
            "Shipping Address",  # Updated from Billing Address
            "Status",
            "Tier",
            "Sales Person",
            "Total Sales Current Month",
            "Last Bill Date",
            "Average Order Frequency (Monthly)",
            "Billing Till Date Current Year",
            "Total Sales Last FY",
            "Total Sales Previous FY",
            "Billed Last Month",
            "Billed Last 45 Days",
            "Billed Last 2 Months",
            "Billed Last 3 Months",
            "Due Payments Count",
            "Not Due Payments Count",
        ]

        # Apply header styling
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(
            start_color="366092", end_color="366092", fill_type="solid"
        )
        header_alignment = Alignment(horizontal="center", vertical="center")

        for col, header in enumerate(headers, 1):
            cell = worksheet.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment

        # Add data rows
        for row, customer in enumerate(customers, 2):
            worksheet.cell(row=row, column=1, value=customer.get("customerName", ""))
            worksheet.cell(
                row=row, column=2, value=customer.get("shippingAddress", "")
            )  # Updated from billingAddress
            worksheet.cell(row=row, column=3, value=customer.get("status", ""))
            worksheet.cell(row=row, column=4, value=customer.get("tier", ""))
            worksheet.cell(row=row, column=5, value=customer.get("salesPerson", ""))
            worksheet.cell(
                row=row, column=6, value=customer.get("totalSalesCurrentMonth", 0)
            )
            worksheet.cell(row=row, column=7, value=customer.get("lastBillDate", ""))
            worksheet.cell(
                row=row, column=8, value=customer.get("averageOrderFrequencyMonthly", 0)
            )
            worksheet.cell(
                row=row, column=9, value=customer.get("billingTillDateCurrentYear", 0)
            )
            worksheet.cell(
                row=row, column=10, value=customer.get("totalSalesLastFY", 0)
            )
            worksheet.cell(
                row=row, column=11, value=customer.get("totalSalesPreviousFY", 0)
            )
            worksheet.cell(
                row=row,
                column=12,
                value="Yes" if customer.get("hasBilledLastMonth", False) else "No",
            )
            worksheet.cell(
                row=row,
                column=13,
                value="Yes" if customer.get("hasBilledLast45Days", False) else "No",
            )
            worksheet.cell(
                row=row,
                column=14,
                value="Yes" if customer.get("hasBilledLast2Months", False) else "No",
            )
            worksheet.cell(
                row=row,
                column=15,
                value="Yes" if customer.get("hasBilledLast3Months", False) else "No",
            )
            worksheet.cell(
                row=row, column=16, value=len(customer.get("duePayments", []))
            )
            worksheet.cell(
                row=row, column=17, value=len(customer.get("notDuePayments", []))
            )

        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)

            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass

            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # Save to BytesIO
        excel_buffer = BytesIO()
        workbook.save(excel_buffer)
        excel_buffer.seek(0)

        # Generate filename with current date
        filename = f"customers_report_{datetime.now().strftime('%Y-%m-%d')}.xlsx"

        # Return as streaming response
        return StreamingResponse(
            BytesIO(excel_buffer.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error in download_customer_analytics_report: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})
