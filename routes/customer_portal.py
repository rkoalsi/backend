from fastapi import APIRouter, Query, HTTPException
from ..config.root import get_database, serialize_mongo_document
from typing import Optional
from bson import ObjectId
from datetime import datetime

router = APIRouter()
db = get_database()


@router.get("/invoices")
def get_customer_invoices(
    customer_id: str = Query(..., description="Customer ID from Zoho"),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """
    Get all invoices for a specific customer by their customer_id.
    Returns paginated results with invoice details.
    """
    query = {"customer_id": customer_id}

    if status:
        query["status"] = status

    # Get total count
    total = db.invoices.count_documents(query)

    # Calculate skip
    skip = (page - 1) * per_page

    # Fetch invoices sorted by date descending
    invoices_cursor = db.invoices.find(query).sort("date", -1).skip(skip).limit(per_page)
    invoices = [serialize_mongo_document(doc) for doc in invoices_cursor]

    # Calculate summary stats
    all_invoices = list(db.invoices.find({"customer_id": customer_id}))
    total_amount = sum(inv.get("total", 0) or 0 for inv in all_invoices)
    total_balance = sum(inv.get("balance", 0) or 0 for inv in all_invoices)
    paid_count = len([inv for inv in all_invoices if inv.get("status") == "paid"])
    overdue_count = len([inv for inv in all_invoices if inv.get("status") == "overdue"])

    return {
        "invoices": invoices,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
        "summary": {
            "total_invoices": len(all_invoices),
            "total_amount": total_amount,
            "total_balance": total_balance,
            "paid_count": paid_count,
            "overdue_count": overdue_count,
        }
    }


@router.get("/credit-notes")
def get_customer_credit_notes(
    customer_id: str = Query(..., description="Customer ID from Zoho"),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """
    Get all credit notes for a specific customer by their customer_id.
    Returns paginated results with credit note details.
    """
    query = {"customer_id": customer_id}

    if status:
        query["status"] = status

    # Get total count
    total = db.credit_notes.count_documents(query)

    # Calculate skip
    skip = (page - 1) * per_page

    # Fetch credit notes sorted by date descending
    credit_notes_cursor = db.credit_notes.find(query).sort("date", -1).skip(skip).limit(per_page)
    credit_notes = [serialize_mongo_document(doc) for doc in credit_notes_cursor]

    # Calculate summary stats
    all_credit_notes = list(db.credit_notes.find({"customer_id": customer_id}))
    total_credits = sum(cn.get("total", 0) or 0 for cn in all_credit_notes)
    total_balance = sum(cn.get("balance", 0) or 0 for cn in all_credit_notes)

    return {
        "credit_notes": credit_notes,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
        "summary": {
            "total_credit_notes": len(all_credit_notes),
            "total_credits": total_credits,
            "total_balance": total_balance,
        }
    }


@router.get("/dashboard-summary")
def get_customer_dashboard_summary(
    customer_id: str = Query(..., description="Customer ID from Zoho"),
):
    """
    Get a summary of invoices and credit notes for the customer dashboard.
    Returns recent items and statistics.
    """
    # Get recent invoices (last 5)
    recent_invoices = list(
        db.invoices.find({"customer_id": customer_id})
        .sort("date", -1)
        .limit(5)
    )
    recent_invoices = [serialize_mongo_document(doc) for doc in recent_invoices]

    # Get recent credit notes (last 5)
    recent_credit_notes = list(
        db.credit_notes.find({"customer_id": customer_id})
        .sort("date", -1)
        .limit(5)
    )
    recent_credit_notes = [serialize_mongo_document(doc) for doc in recent_credit_notes]

    # Calculate invoice stats
    all_invoices = list(db.invoices.find({"customer_id": customer_id}))
    invoice_stats = {
        "total": len(all_invoices),
        "total_amount": sum(inv.get("total", 0) or 0 for inv in all_invoices),
        "total_balance": sum(inv.get("balance", 0) or 0 for inv in all_invoices),
        "paid": len([inv for inv in all_invoices if inv.get("status") == "paid"]),
        "overdue": len([inv for inv in all_invoices if inv.get("status") == "overdue"]),
        "pending": len([inv for inv in all_invoices if inv.get("status") not in ["paid", "void"]]),
    }

    # Calculate credit note stats
    all_credit_notes = list(db.credit_notes.find({"customer_id": customer_id}))
    credit_note_stats = {
        "total": len(all_credit_notes),
        "total_amount": sum(cn.get("total", 0) or 0 for cn in all_credit_notes),
        "total_balance": sum(cn.get("balance", 0) or 0 for cn in all_credit_notes),
    }

    return {
        "recent_invoices": recent_invoices,
        "recent_credit_notes": recent_credit_notes,
        "invoice_stats": invoice_stats,
        "credit_note_stats": credit_note_stats,
    }
