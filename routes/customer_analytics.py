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
                    # Check if invoice is in current month (August 2025)
                    "isCurrentMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    8,
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in completed months (April-July 2025)
                    "isCompletedMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$gte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    4,
                                ]
                            },
                            {
                                "$lte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    7,
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in current financial year (April 2025 onwards)
                    "isCurrentFY": {
                        "$and": [
                            {
                                "$gte": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$gte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    4,
                                ]
                            },
                        ]
                    },
                    # Check if invoice is in last financial year (April 2024 - March 2025)
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
                                            2024,
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
                                            2025,
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
                    # Check if invoice is in previous financial year (April 2023 - March 2024)
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
                                            2023,
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
                                            2024,
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
                    # Billing period validations
                    # Last month (July 2025)
                    "billedLastMonth": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    7,
                                ]
                            },
                        ]
                    },
                    # Last 45 days (June 24, 2025 to August 8, 2025)
                    "billedLast45Days": {
                        "$gte": [
                            {"$dateFromString": {"dateString": "$date"}},
                            {"$dateFromString": {"dateString": "2025-06-24"}},
                        ]
                    },
                    # Last 2 months (June-July 2025)
                    "billedLast2Months": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$gte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    6,
                                ]
                            },
                            {
                                "$lte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    7,
                                ]
                            },
                        ]
                    },
                    # Last 3 months (May-July 2025)
                    "billedLast3Months": {
                        "$and": [
                            {
                                "$eq": [
                                    {
                                        "$year": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    2025,
                                ]
                            },
                            {
                                "$gte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    5,
                                ]
                            },
                            {
                                "$lte": [
                                    {
                                        "$month": {
                                            "$dateFromString": {"dateString": "$date"}
                                        }
                                    },
                                    7,
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
                    # Total sales current month (August 2025)
                    "totalSalesCurrentMonth": {
                        "$sum": {
                            "$cond": [{"$eq": ["$isCurrentMonth", True]}, "$total", 0]
                        }
                    },
                    # Last bill date
                    "lastBillDate": {"$max": "$invoiceDate"},
                    # Count of orders in completed months for frequency calculation
                    "completedMonthOrders": {
                        "$sum": {"$cond": [{"$eq": ["$isCompletedMonth", True]}, 1, 0]}
                    },
                    # Unique months in completed period for frequency calculation
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
                    # Remove null values from completedMonths array
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
                    # Average order frequency per month (orders per month in completed months)
                    "averageOrderFrequencyMonthly": {
                        "$cond": [
                            {"$gt": [{"$size": "$completedMonthsFiltered"}, 0]},
                            {
                                "$divide": [
                                    "$completedMonthOrders",
                                    {"$size": "$completedMonthsFiltered"},
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
            # Stage 7: Project final output format
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
                    # Billing validation flags (true = HAS billed in period)
                    "hasBilledLastMonth": 1,
                    "hasBilledLast45Days": 1,
                    "hasBilledLast2Months": 1,
                    "hasBilledLast3Months": 1,
                }
            },
            # Stage 8: Sort by customer name and then by billing address
            {"$sort": {"customerName": 1}},
        ]

        customers = list(db.invoices.aggregate(pipeline))
        return serialize_mongo_document(customers)
    except Exception as e:
        logger.error(f"Error in get_customer_analytics: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})
