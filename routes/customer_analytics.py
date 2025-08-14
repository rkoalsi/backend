from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional

import logging
from config.root import connect_to_mongo, serialize_mongo_document

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

client, db = connect_to_mongo()


@router.get("")
def get_customer_analytics(
    status: Optional[str] = Query(None, description="Filter by invoice status"),
    tier: Optional[str] = Query(None, description="Filter by tiers (A,B,C)"),
    due_status: Optional[str] = Query("all", description="Filter by Payments Due (all, due, not_due)"),
    sp_code: Optional[str] = Query(None, description="Filter by salesperson code"),
    sort_by: Optional[bool] = Query(True, description="Low to High or High to Low"),
):
    try:

        # Build the match stage dynamically
        match_stage = {
            "date": {"$gte": "2023-04-01"},
            "status": {"$nin": ["void", "draft"]},
            "customer_name": {
                "$not": {
                    "$regex": "(EC)|(NA)|(amzb2b)|(amz2b2)|(PUPEV)|(RS)|(MKT)|(SPUR)|(SSAM)|(OSAM)|Blinkit",
                    "$options": "i",
                }
            },
        }
        customer_status_match_stage = {}
        sort_stage = {"$sort": {"totalSalesCurrentMonth": 1}}

        # Add status filter if provided, otherwise use default exclusions
        if status == "all":
            pass
        elif status:
            customer_status_match_stage["customerDetails.status"] = status

        # Add salesperson filter if provided
        if sp_code:
            match_stage["$or"] = [
                {"salesperson_name": sp_code},
                {"cf_sales_person": sp_code},
            ]

        if sort_by:
            sort_stage["$sort"] = [
                {"totalSalesCurrentMonth": -1},
            ]

        # Create the salesPerson field logic based on whether sp_code is provided
        if sp_code:
            # If sp_code is provided, return the field that matches the sp_code
            sales_person_logic = {
                "$cond": [
                    {"$eq": ["$salesperson_name", sp_code]},
                    "$salesperson_name",
                    {
                        "$cond": [
                            {"$eq": ["$cf_sales_person", sp_code]},
                            "$cf_sales_person",
                            # Fallback to first non-null if neither matches (shouldn't happen due to filter)
                            {
                                "$cond": [
                                    {"$ne": ["$salesperson_name", None]},
                                    "$salesperson_name",
                                    "$cf_sales_person",
                                ]
                            },
                        ]
                    },
                ]
            }
        else:
            # If no sp_code filter, use the original logic
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
                        "$not": {
                            "$in": ["$status", ["void", "draft", "sent", "paid"]]
                        }
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
                    # Normalize city name to handle variations
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
            # Stage 3: Group by customer and normalized address components
            {
                "$group": {
                    "_id": {
                        "customerId": "$customer_id",
                        "city": "$normalizedCity",
                        "state": "$billing_address.state",
                        "zip": "$billing_address.zip",
                        "country": "$billing_address.country",
                    },
                    "customerName": {"$first": "$customer_name"},
                    "billingAddress": {"$first": "$billing_address"},
                    # Updated logic: Get the salesperson field that matches the sp_code
                    "salesPerson": {"$first": sales_person_logic},
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
                                "$$REMOVE"
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
                                "$$REMOVE"
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
                    "localField": "_id.customerId",
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
                    # Format billing address as string
                    "billingAddressFormatted": {
                        "$concat": [
                            {"$ifNull": ["$billingAddress.street", ""]},
                            {
                                "$cond": [
                                    {"$ne": ["$billingAddress.street2", ""]},
                                    {"$concat": [", ", "$billingAddress.street2"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$billingAddress.city", ""]},
                                    {"$concat": [", ", "$billingAddress.city"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$billingAddress.state", ""]},
                                    {"$concat": [", ", "$billingAddress.state"]},
                                    "",
                                ]
                            },
                            {
                                "$cond": [
                                    {"$ne": ["$billingAddress.zip", ""]},
                                    {"$concat": [" - ", "$billingAddress.zip"]},
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
                            {"$or": [
                                {"$eq": [due_status, "all"]},
                                {"$eq": [due_status, "due"]}
                            ]},
                            "$duePayments",
                            []
                        ]
                    },
                    "filteredNotDuePayments": {
                        "$cond": [
                            {"$or": [
                                {"$eq": [due_status, "all"]},
                                {"$eq": [due_status, "not_due"]}
                            ]},
                            "$notDuePayments",
                            []
                        ]
                    }
                }
            },
            # Stage 8: Project final output format
            {
                "$project": {
                    "_id": 0,
                    "customerName": 1,
                    "billingAddress": "$billingAddressFormatted",
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
                }
            },
            # Stage 9: Sort by customer name and then by billing address
            {"$sort": {"customerName": 1}},
        ]

        customers = list(db.invoices.aggregate(pipeline))
        return serialize_mongo_document(customers)
    except Exception as e:
        logger.error(f"Error in get_customer_analytics: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})