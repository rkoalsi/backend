from fastapi import APIRouter, HTTPException, Query
from ..config.root import get_database, serialize_mongo_document  
from dotenv import load_dotenv
import os
from typing import List, Dict, Optional
from collections import defaultdict
import pandas as pd
from pydantic import BaseModel
from datetime import datetime, timedelta
from enum import Enum

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
products_collection = db["products"]
CUSTOMERS_COLLECTION = "customers"
customers_collection = db[CUSTOMERS_COLLECTION]
invoices_collection = db["invoices"]


class ViewType(str, Enum):
    detailed = "detailed"
    summary = "summary"


class DateRange(BaseModel):
    start_date: str  # Format: YYYY-MM-DD
    end_date: str  # Format: YYYY-MM-DD
    exclude_patterns: Optional[List[str]] = []  # List of regex patterns to exclude
    view_type: Optional[ViewType] = ViewType.detailed  # detailed or summary


def build_customer_exclusion_filter(exclude_patterns: List[str]) -> Dict:
    """Build MongoDB filter to exclude customers based on regex patterns"""
    if not exclude_patterns:
        return {}

    regex_conditions = []
    for pattern in exclude_patterns:
        regex_conditions.append(
            {
                "customer_info.contact_name": {
                    "$not": {
                        "$regex": pattern,
                        "$options": "i",
                    }
                }
            }
        )
    if regex_conditions:
        return {"$and": regex_conditions}

    return {}


def build_customer_exclusion_filter_direct(exclude_patterns: List[str]) -> Dict:
    """Build MongoDB filter to exclude customers based on regex patterns for direct customer queries"""
    if not exclude_patterns:
        return {}

    regex_conditions = []
    for pattern in exclude_patterns:
        regex_conditions.append(
            {
                "contact_name": {
                    "$not": {"$regex": pattern, "$options": "i"}  # Case insensitive
                }
            }
        )

    # If we have exclusion patterns, all must be true (AND condition)
    if regex_conditions:
        return {"$and": regex_conditions}

    return {}


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end date with better error handling"""
    try:
        # Validate input
        if not start_date or not end_date:
            raise ValueError("Start date and end date cannot be empty")

        # Validate format
        import re

        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        if not re.match(date_pattern, start_date):
            raise ValueError(f"Invalid start_date format: '{start_date}'")
        if not re.match(date_pattern, end_date):
            raise ValueError(f"Invalid end_date format: '{end_date}'")

        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        if start > end:
            raise ValueError("Start date cannot be after end date")

        # Check for reasonable range (prevent memory issues)
        delta = end - start
        if delta.days > 366:  # More than a year
            raise ValueError(f"Date range too large: {delta.days} days")

        date_list = []
        current = start
        while current <= end:
            date_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        return date_list

    except Exception as e:
        print(f"ERROR in generate_date_range: {e}")
        raise


def build_aggregation_pipeline(
    start_date: str,
    end_date: str,
    exclude_patterns: List[str] = None,
    view_type: ViewType = ViewType.detailed,
) -> List[Dict]:
    """Build MongoDB aggregation pipeline with customer exclusion and view type support"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    if exclude_patterns is None:
        exclude_patterns = []

    pipeline = [
        # Match invoices that are not void or draft
        {
            "$match": {
                "status": {"$nin": ["void", "draft"]},
                "created_time": {"$exists": True},
            }
        },
        # Parse the date string to datetime
        {
            "$addFields": {
                "parsed_date": {
                    "$dateFromString": {
                        "dateString": {"$substr": ["$created_time", 0, 19]}
                    }
                }
            }
        },
        # Filter by date range
        {"$match": {"parsed_date": {"$gte": start_dt, "$lt": end_dt}}},
        # Join with customers collection
        {
            "$lookup": {
                "from": CUSTOMERS_COLLECTION,
                "localField": "customer_id",
                "foreignField": "contact_id",
                "as": "customer_info",
            }
        },
        # Unwind customer info
        {"$unwind": {"path": "$customer_info", "preserveNullAndEmptyArrays": True}},
    ]

    # Add customer exclusion filter if patterns are provided
    exclusion_filter = build_customer_exclusion_filter(exclude_patterns)
    if exclusion_filter:
        pipeline.append({"$match": exclusion_filter})

    # Continue with the rest of the pipeline
    pipeline.extend(
        [
            # Unwind line items
            {"$unwind": {"path": "$line_items", "preserveNullAndEmptyArrays": True}},
        ]
    )

    # Different projections based on view type
    if view_type == ViewType.detailed:
        pipeline.append(
            {
                "$project": {
                    "invoice_id": "$_id",
                    "created_date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$parsed_date"}
                    },
                    "contact_name": "$customer_info.contact_name",
                    "customer_id": "$customer_id",
                    "pincode": "$shipping_address.zip",
                    "item_name": "$line_items.name",
                    "item_description": "$line_items.description",
                    "quantity": "$line_items.quantity",
                    "rate": "$line_items.rate",
                    "amount": "$line_items.item_total",
                    "sales_person": {
                        "$ifNull": ["$cf_sales_person", "$salesperson_name"]
                    },
                }
            }
        )
    else:  # summary view
        pipeline.append(
            {
                "$project": {
                    "invoice_id": "$_id",
                    "created_date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$parsed_date"}
                    },
                    "contact_name": "$customer_info.contact_name",
                    "customer_id": "$customer_id",
                    "pincode": "$shipping_address.zip",
                    "quantity": "$line_items.quantity",
                    "amount": "$line_items.item_total",
                    "sales_person": {
                        "$ifNull": ["$cf_sales_person", "$salesperson_name"]
                    },
                }
            }
        )

    return pipeline


def process_detailed_report_data(
    invoice_data: List[Dict], date_columns: List[str]
) -> List[Dict]:
    """Process raw invoice data into detailed pivot format with error handling"""
    try:
        if not invoice_data:
            return []

        if not date_columns:
            raise ValueError("Date columns cannot be empty")

        report_data = defaultdict(lambda: defaultdict(float))

        for i, record in enumerate(invoice_data):
            try:
                # Create unique row identifier with safe defaults
                contact_name = record.get("contact_name") or "Unknown Customer"
                pincode = record.get("pincode") or "Unknown Pincode"
                item_name = record.get("item_name") or "Unknown Item"
                sales_person = record.get("sales_person") or "Unknown Sales Person"

                # Ensure pincode is string
                if pincode and not isinstance(pincode, str):
                    pincode = str(pincode)

                row_key = f"{contact_name} | {pincode} | {item_name} | {sales_person}"
                date_key = record.get("created_date")
                # Safely convert quantity
                try:
                    quantity = float(record.get("quantity", 0))
                except (TypeError, ValueError):
                    quantity = 0.0

                if date_key and date_key in date_columns:
                    report_data[row_key][date_key] += quantity

            except Exception as re:
                print(f"Error processing record {i}: {re}")
                print(f"Problematic record: {record}")
                continue

        # Convert to list format
        report_list = []
        for row_key, date_quantities in report_data.items():
            try:
                # Safely split the row key
                parts = row_key.split(" | ")
                if len(parts) != 4:
                    continue

                contact_name, pincode, item_name, sales_person = parts

                row_data = {
                    "contact_name": contact_name,
                    "pincode": pincode,
                    "item_name": item_name,
                    "date_wise_quantities": {},
                    "total_quantity": sum(date_quantities.values()),
                    "sales_person": sales_person,
                }

                # Add quantities for each date
                for date in date_columns:
                    row_data["date_wise_quantities"][date] = date_quantities.get(
                        date, 0
                    )

                report_list.append(row_data)

            except Exception as pe:
                print(f"Error processing row_key '{row_key}': {pe}")
                continue

        # Sort by contact name, pincode, item name
        report_list.sort(
            key=lambda x: (
                x.get("contact_name", ""),
                x.get("pincode", ""),
                x.get("item_name", ""),
            )
        )
        return report_list

    except Exception as e:
        print(f"ERROR in process_detailed_report_data: {e}")
        import traceback

        traceback.print_exc()
        raise


def process_summary_report_data(invoice_data: List[Dict]) -> List[Dict]:
    """Process raw invoice data into summary format grouped by customer"""
    try:
        if not invoice_data:
            return []

        # Group by customer (contact_name + pincode)
        customer_data = defaultdict(
            lambda: {
                "total_quantity": 0,
                "total_amount": 0,
                "unique_items": set(),
                "invoice_count": 0,
                "sales_person": "",
            }
        )

        for record in invoice_data:
            try:
                contact_name = record.get("contact_name") or "Unknown Customer"
                pincode = record.get("pincode") or "Unknown Pincode"

                # Ensure pincode is string
                if pincode and not isinstance(pincode, str):
                    pincode = str(pincode)

                customer_key = f"{contact_name} | {pincode}"

                # Safely convert quantity and amount
                try:
                    quantity = float(record.get("quantity", 0))
                    amount = float(record.get("amount", 0))
                except (TypeError, ValueError):
                    quantity = 0.0
                    amount = 0.0

                customer_data[customer_key]["sales_person"] = record.get(
                    "sales_person", ""
                )
                customer_data[customer_key]["total_quantity"] += quantity
                customer_data[customer_key]["total_amount"] += amount
                customer_data[customer_key]["invoice_count"] += 1

            except Exception as e:
                print(f"Error processing summary record: {e}")
                continue

        # Convert to list format
        report_list = []
        for customer_key, data in customer_data.items():
            try:
                parts = customer_key.split(" | ")
                if len(parts) != 2:
                    continue

                contact_name, pincode = parts

                row_data = {
                    "contact_name": contact_name,
                    "pincode": pincode,
                    "total_quantity": data["total_quantity"],
                    "total_amount": data["total_amount"],
                    "invoice_count": data["invoice_count"],
                    "sales_person": data["sales_person"],
                }

                report_list.append(row_data)

            except Exception as e:
                print(f"Error processing customer_key '{customer_key}': {e}")
                continue

        # Sort by contact name, pincode
        report_list.sort(
            key=lambda x: (x.get("contact_name", ""), x.get("pincode", ""))
        )
        return report_list

    except Exception as e:
        print(f"ERROR in process_summary_report_data: {e}")
        import traceback

        traceback.print_exc()
        raise


@router.post("/billed_customers")
async def generate_billed_customers_report(date_range: DateRange):
    """
    Generate a sales report for the given date range with support for detailed and summary views
    Supports excluding customers based on regex patterns
    """
    try:
        # Enhanced date validation
        if not date_range.start_date or not date_range.end_date:
            raise HTTPException(
                status_code=400, detail="Both start_date and end_date are required"
            )

        # Validate date format using regex first
        import re

        date_pattern = r"^\d{4}-\d{2}-\d{2}$"

        if not re.match(date_pattern, date_range.start_date):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid start_date format. Expected YYYY-MM-DD, got: '{date_range.start_date}'",
            )

        if not re.match(date_pattern, date_range.end_date):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid end_date format. Expected YYYY-MM-DD, got: '{date_range.end_date}'",
            )

        # Validate date parsing
        try:
            start_dt = datetime.strptime(date_range.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(date_range.end_date, "%Y-%m-%d")
        except ValueError as ve:
            raise HTTPException(
                status_code=400, detail=f"Date parsing error: {str(ve)}"
            )

        if start_dt > end_dt:
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        # Build and execute aggregation pipeline with exclusions and view type
        try:
            pipeline = build_aggregation_pipeline(
                date_range.start_date,
                date_range.end_date,
                date_range.exclude_patterns,
                date_range.view_type,
            )
        except Exception as pe:
            print(f"Error in build_aggregation_pipeline: {pe}")
            raise HTTPException(
                status_code=500, detail=f"Pipeline build error: {str(pe)}"
            )

        try:
            invoice_data = list(invoices_collection.aggregate(pipeline))
        except Exception as ae:
            print(f"Error in aggregation execution: {ae}")
            raise HTTPException(
                status_code=500, detail=f"Database query error: {str(ae)}"
            )

        if not invoice_data:
            return {
                "status": "success",
                "message": "No data found for the specified date range",
                "view_type": date_range.view_type,
                "date_range": {
                    "start_date": date_range.start_date,
                    "end_date": date_range.end_date,
                },
                "exclusions": {
                    "exclude_patterns": date_range.exclude_patterns,
                    "patterns_applied": len(date_range.exclude_patterns) > 0,
                },
                "summary": {
                    "total_records": 0,
                    "total_customers": 0,
                    "total_unique_items": 0,
                    "date_columns": (
                        [] if date_range.view_type == ViewType.detailed else None
                    ),
                },
                "report": [],
            }

        # Process data based on view type
        try:
            if date_range.view_type == ViewType.detailed:
                date_columns = generate_date_range(
                    date_range.start_date, date_range.end_date
                )
                report_list = process_detailed_report_data(invoice_data, date_columns)

                # Calculate summary statistics for detailed view
                total_records = len(report_list)
                total_customers = len(
                    set(
                        row["contact_name"]
                        for row in report_list
                        if row.get("contact_name")
                    )
                )
                total_items = len(
                    set(row["item_name"] for row in report_list if row.get("item_name"))
                )

                summary = {
                    "total_records": total_records,
                    "total_customers": total_customers,
                    "total_unique_items": total_items,
                    "date_columns": date_columns,
                }
            else:  # summary view
                report_list = process_summary_report_data(invoice_data)
                print(report_list)
                # Calculate summary statistics for summary view
                total_records = len(report_list)
                total_customers = len(
                    set(
                        row["contact_name"]
                        for row in report_list
                        if row.get("contact_name")
                    )
                )

                summary = {
                    "total_records": total_records,
                    "total_customers": total_customers,
                    "total_unique_items": None,  # Not applicable for summary view
                    "date_columns": None,  # Not applicable for summary view
                }

        except Exception as de:
            raise HTTPException(
                status_code=500, detail=f"Data processing error: {str(de)}"
            )

        return {
            "status": "success",
            "view_type": date_range.view_type,
            "date_range": {
                "start_date": date_range.start_date,
                "end_date": date_range.end_date,
            },
            "exclusions": {
                "exclude_patterns": date_range.exclude_patterns,
                "patterns_applied": len(date_range.exclude_patterns) > 0,
            },
            "summary": summary,
            "report": report_list,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/billed_customers")
async def generate_billed_customers_report_xlsx(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    exclude_patterns: List[str] = Query(
        [], description="Regex patterns to exclude customers"
    ),
    view_type: ViewType = Query(
        ViewType.detailed, description="View type: detailed or summary"
    ),
):
    """
    Generate XLSX version of the invoice report with customer exclusions and view type support
    """
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt > end_dt:
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        # Get report data using the POST endpoint logic
        date_range = DateRange(
            start_date=start_date,
            end_date=end_date,
            exclude_patterns=exclude_patterns,
            view_type=view_type,
        )
        report_response = await generate_billed_customers_report(date_range)

        if report_response["summary"]["total_records"] == 0:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range"
            )

        # Convert to DataFrame based on view type
        rows = []
        if view_type == ViewType.detailed:
            for record in report_response["report"]:
                row = {
                    "Contact Name": record["contact_name"],
                    "Pincode": record["pincode"],
                    "Item Name": record["item_name"],
                    "Total Quantity": record["total_quantity"],
                    "Sales Person": record["sales_person"],
                }
                # Add date columns
                for date, quantity in record["date_wise_quantities"].items():
                    row[date] = quantity
                rows.append(row)
        else:  # summary view
            for record in report_response["report"]:
                row = {
                    "Contact Name": record["contact_name"],
                    "Pincode": record["pincode"],
                    "Total Quantity": record["total_quantity"],
                    "Total Amount": record["total_amount"],
                    "Invoice Count": record["invoice_count"],
                    "Sales Person": record["sales_person"],
                }
                rows.append(row)

        df = pd.DataFrame(rows)

        # Create XLSX content
        from io import BytesIO

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            sheet_name = (
                "Detailed Report"
                if view_type == ViewType.detailed
                else "Summary Report"
            )
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Add a summary sheet
            summary_data = [
                ["Report Type", view_type.value.title()],
                ["Report Period", f"{start_date} to {end_date}"],
                ["Total Records", report_response["summary"]["total_records"]],
                ["Total Customers", report_response["summary"]["total_customers"]],
            ]

            if view_type == ViewType.detailed:
                summary_data.append(
                    [
                        "Total Unique Items",
                        report_response["summary"]["total_unique_items"],
                    ]
                )

            summary_data.extend(
                [
                    [
                        "Exclusion Patterns Applied",
                        ", ".join(exclude_patterns) if exclude_patterns else "None",
                    ],
                    ["Generated On", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ]
            )

            summary_df = pd.DataFrame(summary_data, columns=["Metric", "Value"])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        xlsx_data = output.getvalue()

        # Convert to base64 for JSON response
        import base64

        xlsx_base64 = base64.b64encode(xlsx_data).decode("utf-8")

        return {
            "status": "success",
            "xlsx_data": xlsx_base64,
            "filename": f"billed_customers_{view_type.value}_{start_date}_to_{end_date}.xlsx",
            "view_type": view_type,
            "exclusions": {
                "exclude_patterns": exclude_patterns,
                "patterns_applied": len(exclude_patterns) > 0,
            },
            "summary": report_response["summary"],
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating XLSX: {str(e)}")


# Keep the existing unbilled_customers endpoints unchanged
@router.post("/unbilled_customers")
async def generate_unbilled_customers_report(date_range: DateRange):
    """
    Generate a report of customers who were NOT billed in the given date range
    Supports excluding customers based on regex patterns
    """
    try:
        # Validate date format
        start_dt = datetime.strptime(date_range.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(date_range.end_date, "%Y-%m-%d")
        if start_dt > end_dt:
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        # Get all customer IDs who were billed in the date range (with exclusions)
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
            {
                "$match": {
                    "parsed_date": {"$gte": start_dt, "$lt": end_dt + timedelta(days=1)}
                }
            },
            # Join with customers to apply exclusions
            {
                "$lookup": {
                    "from": CUSTOMERS_COLLECTION,
                    "localField": "customer_id",
                    "foreignField": "contact_id",
                    "as": "customer_info",
                }
            },
            {"$unwind": {"path": "$customer_info", "preserveNullAndEmptyArrays": True}},
        ]

        # Add customer exclusion filter
        exclusion_filter = build_customer_exclusion_filter(
            date_range.exclude_patterns or []
        )
        if exclusion_filter:
            billed_customers_pipeline.append({"$match": exclusion_filter})

        billed_customers_pipeline.append({"$group": {"_id": "$customer_id"}})

        billed_customer_ids = [
            doc["_id"]
            for doc in invoices_collection.aggregate(billed_customers_pipeline)
        ]

        # Build base match conditions for unbilled customers
        base_match = {
            "status": "active",
            "contact_id": {"$nin": billed_customer_ids},
        }

        # Add direct customer exclusion filter
        direct_exclusion_filter = build_customer_exclusion_filter_direct(
            date_range.exclude_patterns or []
        )
        if direct_exclusion_filter:
            base_match.update(direct_exclusion_filter)

        # Get all active customers who were NOT billed
        unbilled_customers_pipeline = [
            {"$match": base_match},
            {
                "$lookup": {
                    "from": "invoices",
                    "let": {"customer_id": "$contact_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {"$eq": ["$customer_id", "$$customer_id"]},
                                "status": {"$nin": ["void", "draft"]},
                                "created_time": {"$exists": True},
                            }
                        },
                        {
                            "$addFields": {
                                "parsed_date": {
                                    "$dateFromString": {
                                        "dateString": {
                                            "$substr": ["$created_time", 0, 19]
                                        }
                                    }
                                }
                            }
                        },
                        {"$sort": {"parsed_date": -1}},
                        {"$limit": 1},
                    ],
                    "as": "last_invoice",
                }
            },
            {
                "$project": {
                    "contact_id": 1,
                    "contact_name": 1,
                    "email": 1,
                    "phone": 1,
                    "status": 1,
                    "cf_sales_person": 1,
                    "billing_address": 1,
                    "shipping_address": 1,
                    "last_invoice_date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {"$arrayElemAt": ["$last_invoice.parsed_date", 0]},
                        }
                    },
                    "last_invoice_amount": {"$arrayElemAt": ["$last_invoice.total", 0]},
                    "days_since_last_invoice": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$last_invoice"}, 0]},
                            "then": {
                                "$dateDiff": {
                                    "startDate": {
                                        "$arrayElemAt": ["$last_invoice.parsed_date", 0]
                                    },
                                    "endDate": end_dt,
                                    "unit": "day",
                                }
                            },
                            "else": None,
                        }
                    },
                }
            },
            {"$sort": {"days_since_last_invoice": -1}},
        ]

        unbilled_customers = list(
            customers_collection.aggregate(unbilled_customers_pipeline)
        )

        if not unbilled_customers:
            return {
                "status": "success",
                "message": "No unbilled customers found for the specified date range",
                "date_range": {
                    "start_date": date_range.start_date,
                    "end_date": date_range.end_date,
                },
                "exclusions": {
                    "exclude_patterns": date_range.exclude_patterns or [],
                    "patterns_applied": len(date_range.exclude_patterns or []) > 0,
                },
                "summary": {
                    "total_unbilled_customers": 0,
                    "customers_never_billed": 0,
                    "customers_with_past_billing": 0,
                },
                "report": [],
            }

        # Process the data
        processed_customers = []
        never_billed_count = 0
        with_past_billing_count = 0

        for customer in unbilled_customers:
            customer_data = {
                "contact_id": customer.get("contact_id"),
                "contact_name": customer.get("contact_name", "Unknown Customer"),
                "email": customer.get("email"),
                "phone": customer.get("phone"),
                "status": customer.get("status"),
                "sales_person": customer.get("cf_sales_person"),
                "pincode": customer.get("shipping_address", {}).get("zip")
                or customer.get("billing_address", {}).get("zip"),
                "last_invoice_date": customer.get("last_invoice_date"),
                "last_invoice_amount": customer.get("last_invoice_amount"),
                "days_since_last_invoice": customer.get("days_since_last_invoice"),
                "billing_status": (
                    "Never Billed"
                    if not customer.get("last_invoice_date")
                    else "Previously Billed"
                ),
            }

            if not customer.get("last_invoice_date"):
                never_billed_count += 1
            else:
                with_past_billing_count += 1

            processed_customers.append(customer_data)

        return {
            "status": "success",
            "date_range": {
                "start_date": date_range.start_date,
                "end_date": date_range.end_date,
            },
            "exclusions": {
                "exclude_patterns": date_range.exclude_patterns or [],
                "patterns_applied": len(date_range.exclude_patterns or []) > 0,
            },
            "summary": {
                "total_unbilled_customers": len(processed_customers),
                "customers_never_billed": never_billed_count,
                "customers_with_past_billing": with_past_billing_count,
            },
            "report": processed_customers,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/unbilled_customers")
async def generate_unbilled_customers_report_xlsx(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    exclude_patterns: List[str] = Query(
        [], description="Regex patterns to exclude customers"
    ),
):
    """
    Generate XLSX version of the unbilled customers report with customer exclusions
    """
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt > end_dt:
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        # Get report data using the POST endpoint logic
        date_range = DateRange(
            start_date=start_date, end_date=end_date, exclude_patterns=exclude_patterns
        )
        report_response = await generate_unbilled_customers_report(date_range)

        if report_response["summary"]["total_unbilled_customers"] == 0:
            raise HTTPException(
                status_code=404,
                detail="No unbilled customers found for the specified date range",
            )

        # Convert to DataFrame
        rows = []
        for customer in report_response["report"]:
            row = {
                "Contact ID": customer["contact_id"],
                "Contact Name": customer["contact_name"],
                "Email": customer["email"] or "N/A",
                "Phone": customer["phone"] or "N/A",
                "Status": customer["status"],
                "Sales Person": customer["sales_person"] or "Unassigned",
                "Pincode": customer["pincode"] or "N/A",
                "Billing Status": customer["billing_status"],
                "Last Invoice Date": customer["last_invoice_date"] or "Never",
                "Last Invoice Amount": customer["last_invoice_amount"] or 0,
                "Days Since Last Invoice": customer["days_since_last_invoice"] or "N/A",
            }
            rows.append(row)

        df = pd.DataFrame(rows)

        # Create XLSX content
        from io import BytesIO

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Unbilled Customers", index=False)

            # Add a summary sheet
            summary_df = pd.DataFrame(
                [
                    ["Report Period", f"{start_date} to {end_date}"],
                    [
                        "Total Unbilled Customers",
                        report_response["summary"]["total_unbilled_customers"],
                    ],
                    [
                        "Never Billed",
                        report_response["summary"]["customers_never_billed"],
                    ],
                    [
                        "Previously Billed",
                        report_response["summary"]["customers_with_past_billing"],
                    ],
                    [
                        "Exclusion Patterns Applied",
                        ", ".join(exclude_patterns) if exclude_patterns else "None",
                    ],
                    ["Generated On", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ],
                columns=["Metric", "Value"],
            )
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        xlsx_data = output.getvalue()

        # Convert to base64 for JSON response
        import base64

        xlsx_base64 = base64.b64encode(xlsx_data).decode("utf-8")

        return {
            "status": "success",
            "xlsx_data": xlsx_base64,
            "filename": f"unbilled_customers_{start_date}_to_{end_date}.xlsx",
            "exclusions": {
                "exclude_patterns": exclude_patterns,
                "patterns_applied": len(exclude_patterns) > 0,
            },
            "summary": report_response["summary"],
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating XLSX: {str(e)}")


@router.get("/billing_stats")
async def get_billing_stats(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    exclude_patterns: List[str] = Query(
        [], description="Regex patterns to exclude customers"
    ),
):
    """
    Get quick statistics for both billed and unbilled customers with exclusions
    """
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt > end_dt:
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        # Get billed customers count with exclusions
        billed_pipeline = [
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
            {
                "$match": {
                    "parsed_date": {"$gte": start_dt, "$lt": end_dt + timedelta(days=1)}
                }
            },
            # Join with customers to apply exclusions
            {
                "$lookup": {
                    "from": CUSTOMERS_COLLECTION,
                    "localField": "customer_id",
                    "foreignField": "contact_id",
                    "as": "customer_info",
                }
            },
            {"$unwind": {"path": "$customer_info", "preserveNullAndEmptyArrays": True}},
        ]

        # Add customer exclusion filter
        exclusion_filter = build_customer_exclusion_filter(exclude_patterns)
        if exclusion_filter:
            billed_pipeline.append({"$match": exclusion_filter})

        billed_pipeline.extend(
            [{"$group": {"_id": "$customer_id"}}, {"$count": "total_billed"}]
        )

        billed_result = list(invoices_collection.aggregate(billed_pipeline))
        total_billed = billed_result[0]["total_billed"] if billed_result else 0

        # Get total active customers (with exclusions)
        active_customers_match = {"status": "active"}
        direct_exclusion_filter = build_customer_exclusion_filter_direct(
            exclude_patterns
        )
        if direct_exclusion_filter:
            active_customers_match.update(direct_exclusion_filter)

        total_active_customers = customers_collection.count_documents(
            active_customers_match
        )

        # Calculate unbilled (approximation for quick stats)
        total_unbilled = total_active_customers - total_billed

        return {
            "date_range": {"start_date": start_date, "end_date": end_date},
            "exclusions": {
                "exclude_patterns": exclude_patterns,
                "patterns_applied": len(exclude_patterns) > 0,
            },
            "stats": {
                "total_billed_customers": total_billed,
                "total_unbilled_customers": max(
                    0, total_unbilled
                ),  # Ensure non-negative
                "total_active_customers": total_active_customers,
                "billing_percentage": (
                    round((total_billed / total_active_customers * 100), 2)
                    if total_active_customers > 0
                    else 0
                ),
            },
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting billing stats: {str(e)}"
        )
