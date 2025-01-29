from fastapi import APIRouter, Query, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from typing import Optional

router = APIRouter()

client, db = connect_to_mongo()
products_collection = db["products"]


@router.get("")
def get_invoices(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(25, ge=1, le=100, description="Number of items per page"),
    created_by: str = Query(""),
    # search: Optional[str] = Query(None, description="Search term for name or SKU code"),
):
    """
    Retrieves paginated products with optional brand, category, and search filters,
    sorted such that new products appear first within each brand.
    """
    # Define base query
    query = {"cf_sales_person": created_by, "status": "overdue"}
    project = {
        "invoice_id": 1,
        "invoice_number": 1,
        "date": 1,
        "due_date": 1,
        "customer_id": 1,
        "customer_name": 1,
        "total": 1,
        "balance": 1,
    }
    # Add search filter
    # if search:
    #     regex = {"$regex": search, "$options": "i"}  # Case-insensitive search
    #     query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

    # Aggregation Pipeline
    pipeline = [
        {"$match": query},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
        # {"$project": project},
    ]
    print(pipeline)
    # Execute Aggregation Pipeline
    try:
        fetched_invoices = list(db.invoices.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Serialize products
    all_invoices = [serialize_mongo_document(doc) for doc in fetched_invoices]
    # Calculate total products matching the query
    try:
        total_invoices = db.invoices.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Calculate total pages
    total_pages = (
        (total_invoices + per_page - 1) // per_page if total_invoices > 0 else 1
    )

    # Validate page number
    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "invoices": all_invoices,
        "total": total_invoices,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }
