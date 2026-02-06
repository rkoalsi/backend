from fastapi import APIRouter, Query, HTTPException, Response
from ..config.root import get_database, serialize_mongo_document
from typing import Optional
from bson import ObjectId
from datetime import datetime, date
import os, requests
from .helpers import get_access_token

router = APIRouter()
db = get_database()

org_id = os.getenv("ORG_ID")
CUSTOMER_STATEMENT_URL = os.getenv("CUSTOMER_STATEMENT_URL")


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
    invoices_cursor = (
        db.invoices.find(query).sort("date", -1).skip(skip).limit(per_page)
    )
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
        },
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
    credit_notes_cursor = (
        db.credit_notes.find(query).sort("date", -1).skip(skip).limit(per_page)
    )
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
        },
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
        db.invoices.find({"customer_id": customer_id}).sort("date", -1).limit(5)
    )
    recent_invoices = [serialize_mongo_document(doc) for doc in recent_invoices]

    # Get recent credit notes (last 5)
    recent_credit_notes = list(
        db.credit_notes.find({"customer_id": customer_id}).sort("date", -1).limit(5)
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
        "pending": len(
            [inv for inv in all_invoices if inv.get("status") not in ["paid", "void"]]
        ),
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


@router.get("/payments")
def get_customer_payments(
    customer_id: str = Query(..., description="Customer ID from Zoho"),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
    payment_mode: Optional[str] = Query(None, description="Filter by payment mode"),
):
    """
    Get all payments for a specific customer by their customer_id.
    Returns paginated results with payment details.
    """
    query = {"customer_id": customer_id}

    if payment_mode:
        query["payment_mode"] = payment_mode

    # Get total count
    total = db.customer_payments.count_documents(query)

    # Calculate skip
    skip = (page - 1) * per_page

    # Fetch payments sorted by date descending
    payments_cursor = (
        db.customer_payments.find(query).sort("date", -1).skip(skip).limit(per_page)
    )
    payments = [serialize_mongo_document(doc) for doc in payments_cursor]

    # Calculate summary stats
    all_payments = list(db.customer_payments.find({"customer_id": customer_id}))
    total_amount = sum(p.get("amount", 0) or 0 for p in all_payments)
    total_unused = sum(p.get("unused_amount", 0) or 0 for p in all_payments)

    return {
        "payments": payments,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
        "summary": {
            "total_payments": len(all_payments),
            "total_amount": total_amount,
            "total_unused": total_unused,
        },
    }


def get_financial_year_dates():
    """
    Get the start and end dates for the current Indian financial year.
    Indian FY runs from April 1st to March 31st.
    """
    today = date.today()
    current_year = today.year

    # If we're in Jan-Mar, FY started in April of previous year
    # If we're in Apr-Dec, FY started in April of current year
    if today.month < 4:
        fy_start_year = current_year - 1
    else:
        fy_start_year = current_year

    from_date = date(fy_start_year, 4, 1)  # April 1st
    to_date = today  # Current date

    return from_date.isoformat(), to_date.isoformat()


@router.get("/statement/download")
async def download_customer_statement(
    customer_id: str = Query(..., description="Customer ID from Zoho"),
):
    """
    Download the customer statement PDF for the current financial year.
    Financial year in India: April 1st to March 31st.
    """
    try:
        # Get financial year date range
        from_date, to_date = get_financial_year_dates()

        # Make request to Zoho Books API
        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        response = requests.get(
            url=CUSTOMER_STATEMENT_URL.format(
                customer_id=customer_id,
                from_date=from_date,
                to_date=to_date,
                org_id=org_id,
            ),
            headers=headers,
            allow_redirects=False,
        )

        if response.status_code == 200:
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename=statement_{customer_id}_{from_date}_to_{to_date}.pdf"
                },
            )
        elif response.status_code == 307:
            raise HTTPException(
                status_code=307,
                detail="Redirect encountered. Check Zoho endpoint or token.",
            )
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch statement PDF: {response.text}",
            )

    except HTTPException as e:
        print(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
