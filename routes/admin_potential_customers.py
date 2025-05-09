from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Body,
)
from fastapi.responses import JSONResponse, StreamingResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, openpyxl, io
from datetime import datetime, timezone

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
potential_customers_collection = db["potential_customers"]


@router.get("")
def get_potential_customers(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    code: str | None = None,
):
    try:
        match_statement = {}

        if code:
            match_statement["created_by_info.code"] = code
        pipeline = [
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

        cursor = db.potential_customers.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        del pipeline[-2:]
        total_count = list(
            db.potential_customers.aggregate([*pipeline, {"$count": "total"}])
        )
        total = total_count[0] if total_count else None
        total_count = total.get("total", 0)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "potential_customers": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/report")
def get_potential_customers_report(
    code: str = Query(description="Sales Person Code"),
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

    if code:
        match_statement["created_by_info.code"] = code

    query.append({"$match": match_statement})
    # Fetch matching customers
    customers_cursor = db.potential_customers.aggregate(query)
    customers = [serialize_mongo_document(doc) for doc in customers_cursor]

    # Create an Excel workbook using openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Potential Customers Report"

    # Define the header row
    headers = ["Name", "Address", "Tier", "Mobile", "Created By"]
    ws.append(headers)

    for cust in customers:
        row = [
            cust.get("name", ""),
            cust.get("address", ""),
            cust.get("tier", ""),
            cust.get("mobile", ""),
            cust.get("created_by_info", {}).get("name", ""),
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
            "Content-Disposition": "attachment; filename=potential_customers_report.xlsx"
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
        existing_customer = potential_customers_collection.find_one(
            {"_id": customer_obj_id}
        )
        if not existing_customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        update_data.pop("_id")
        update_data.pop("created_by")
        update_data.pop("created_by_info", "")
        potential_customers_collection.update_one(
            {"_id": customer_obj_id}, {"$set": update_data}
        )
        return {"message": "Customer updated successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{customer_id}")
def delete_potential_customer(customer_id: str):
    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(customer_id)
        result = potential_customers_collection.delete_one({"_id": customer_obj_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Customer not found")

        return {"message": "Customer deleted successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
