from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Body,
)
from fastapi.responses import JSONResponse, StreamingResponse
from config.root import connect_to_mongo, serialize_mongo_document 
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, openpyxl, io
from datetime import timezone, datetime

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
expected_reorders_collection = db["expected_reorders"]


def format_address(address):
    if not isinstance(address, dict):
        return ""

    parts = [
        address.get("attention"),
        address.get("address"),
        address.get("street2"),
        address.get("city"),
        address.get("state"),
        address.get("zip"),
        address.get("country"),
    ]

    parts = [str(part).strip() for part in parts if part and str(part).strip()]

    return ", ".join(parts)


@router.get("")
def get_expected_reorders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    has_ordered: bool | None = None,
):
    try:
        match_statement = {}
        date_filter = {}
        if code:
            match_statement["created_by_info.code"] = code
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
        if has_ordered:
            match_statement["has_ordered"] = has_ordered
        if date_filter:
            match_statement["created_at"] = date_filter
        if code:
            match_statement["created_by_info.code"] = code
        pipeline = [
            {"$sort": {"created_at": -1}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_info",
                }
            },
            {
                "$unwind": {
                    "path": "$created_by_info",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$match": match_statement},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        cursor = db.expected_reorders.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        del pipeline[-2:]
        total_count = list(
            db.expected_reorders.aggregate([*pipeline, {"$count": "total"}])
        )
        total = total_count[0] if len(total_count) > 0 else {"total": 0}
        total_count = total.get("total", 0)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "expected_reorders": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/report")
def get_expected_reorders_report(
    code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    has_ordered: bool | None = None,
):
    # Corrected query definition
    query = [
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
    match_statement = {}
    date_filter = {}

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
        match_statement["created_at"] = date_filter
    if code:
        match_statement["created_by_info.code"] = code
    if has_ordered:
        match_statement["has_ordered"] = has_ordered
    query.append({"$match": match_statement})
    # Fetch matching customers
    customers_cursor = db.expected_reorders.aggregate(query)
    customers = [serialize_mongo_document(doc) for doc in customers_cursor]

    # Create an Excel workbook using openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Expected Reorders Report"

    # Define the header row
    headers = [
        "Created At",
        "Name",
        "Address",
        "Created By",
        "Expected Amount",
        "Has Ordered",
    ]
    ws.append(headers)

    for cust in customers:
        row = [
            cust.get("created_at", ""),
            cust.get("customer_name", ""),
            format_address(cust.get("address")),
            cust.get("created_by_info", {}).get("name", ""),
            cust.get("expected_amount", 0),
            cust.get("has_ordered", False),
        ]
        ws.append(row)

    # Save the workbook to a binary stream
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=expected_reorders_report.xlsx"
        },
    )


@router.put("/{customer_id}")
def update_potential_customer(
    customer_id: str,
    update_data: dict = Body(
        ..., description="Fields to update for the potential customer"
    ),
):
    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(customer_id)
        existing_customer = expected_reorders_collection.find_one(
            {"_id": customer_obj_id}
        )
        if not existing_customer:
            raise HTTPException(status_code=404, detail="Data not found")
        if "_id" in update_data:
            update_data.pop("_id")
        if "created_by" in update_data:
            update_data.pop("created_by")
        if "created_by_info" in update_data:
            update_data.pop("created_by_info")
        expected_reorders_collection.update_one(
            {"_id": customer_obj_id}, {"$set": update_data}
        )
        return {"message": "Data updated successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{customer_id}")
def delete_potential_customer(customer_id: str):
    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(customer_id)
        result = expected_reorders_collection.delete_one({"_id": customer_obj_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Data not found")

        return {"message": "Data deleted successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
