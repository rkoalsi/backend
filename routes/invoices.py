from fastapi import APIRouter, Query, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from typing import Optional
from bson import ObjectId
import re
from datetime import date

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]
invoice_collection = db["invoices"]


def get_invoice(
    invoice_id: str,
):
    result = invoice_collection.find_one({"_id": ObjectId(invoice_id)})
    if result:
        invoice = result
        invoice["status"] = str(invoice["status"]).capitalize()
        return serialize_mongo_document(invoice)
    return None


@router.get("")
def get_invoices(
    created_by: str = Query(""),
    # search: Optional[str] = Query(None, description="Search term for name or SKU code"),
):
    """
    Retrieves paginated invoices with optional filters.
    It also includes the number of days an invoice is overdue, calculated as the difference between today's date and the due_date.
    """
    # Retrieve the user document
    user = db.users.find_one({"_id": ObjectId(created_by)})
    code = user.get("code", "")

    # Define forbidden keywords for salesperson fields
    forbidden_keywords = (
        "(Company customers|defaulters|Amazon|staff purchase|marketing inv's)"
    )

    # Today's date in ISO format (YYYY-MM-DD)
    today_str = date.today().isoformat()
    escaped_sales_person = re.escape(code)

    # Build the query to match invoices past their due date and not marked as paid.
    query = {
        "due_date": {"$lt": today_str},
        "status": {"$nin": ["paid"]},
        # Must match 'code' in either cf_sales_person or salesperson_name
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
        ],
        # Exclude documents if cf_sales_person or salesperson_name contains any forbidden keywords
        "cf_sales_person": {"$not": {"$regex": forbidden_keywords, "$options": "i"}},
        "salesperson_name": {"$not": {"$regex": forbidden_keywords, "$options": "i"}},
    }

    # Define the projection, including a new field to calculate the overdue days.
    project = {
        "_id": 1,
        "invoice_id": 1,
        "invoice_number": 1,
        "status": {
            "$cond": {
                "if": {"$eq": ["$status", "partially_paid"]},
                "then": "partially paid",
                "else": "$status",
            }
        },
        "date": 1,
        "due_date": 1,
        "customer_id": 1,
        "customer_name": 1,
        "total": 1,
        "balance": 1,
        "cf_sales_person": 1,
        "salesperson_name": 1,
        "created_at": 1,
        "overdue_by_days": {
            "$dateDiff": {
                "startDate": {"$dateFromString": {"dateString": "$due_date"}},
                "endDate": "$$NOW",
                "unit": "day",
            }
        },
    }

    # Construct the aggregation pipeline
    pipeline = [
        {"$match": query},
        {"$sort": {"created_at": -1}},  # Latest first
        {"$project": project},
    ]

    # Execute the pipeline
    try:
        fetched_invoices = list(db.invoices.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Serialize the documents
    all_invoices = [serialize_mongo_document(doc) for doc in fetched_invoices]

    # Count total matching documents
    try:
        total_invoices = db.invoices.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    print(total_invoices)
    # Return the response
    return {
        "invoices": all_invoices,
        "total": total_invoices,
    }


@router.get("/{invoice_id}")
def read_invoice(invoice_id: str):
    """
    Retrieve an Invoice by its _id field.
    """
    invoice = get_invoice(invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return invoice
