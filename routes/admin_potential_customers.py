from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Body,
)
from fastapi.responses import JSONResponse, StreamingResponse
from config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from dotenv import load_dotenv
import os, openpyxl, io
from datetime import datetime, timezone
from typing import Optional

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
potential_customers_collection = db["potential_customers"]


@router.get("")
def get_potential_customers(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    code: Optional[str] = Query(
        None, description="Sales person code"
    ),  # Use Optional for type hinting
    startDate: Optional[str] = Query(
        None, description="Start date for filtering (YYYY-MM-DD)"
    ),
    endDate: Optional[str] = Query(
        None, description="End date for filtering (YYYY-MM-DD)"
    ),
):
    try:
        match_statement = {}
        date_filter = {}

        if code:
            match_statement["created_by_info.code"] = code

        if startDate:
            try:
                # Convert to datetime at the beginning of the day (UTC)
                start_dt = datetime.strptime(startDate, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                date_filter["$gte"] = start_dt
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid startDate format. Use YYYY-MM-DD."
                )

        if endDate:
            try:
                # Convert to datetime at the end of the day (UTC)
                end_dt = datetime.strptime(endDate, "%Y-%m-%d").replace(
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    tzinfo=timezone.utc,
                )
                # Alternatively, for $lt comparison with the next day:
                # end_dt = datetime.strptime(endDate, "%Y-%m-%d") + timedelta(days=1)
                # date_filter["$lt"] = end_dt
                date_filter["$lte"] = end_dt
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid endDate format. Use YYYY-MM-DD."
                )

        if date_filter:
            match_statement["created_at"] = date_filter

        # Base pipeline
        pipeline_stages = [
            {
                "$lookup": {
                    "from": "users",  # Make sure this is your actual users collection name
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_info",
                }
            },
            {
                "$unwind": {
                    "path": "$created_by_info",
                    "preserveNullAndEmptyArrays": True,  # Keep customers even if created_by_info is missing
                }
            },
            # Match stage must come AFTER $lookup and $unwind if filtering on looked-up fields (like code)
            # If filtering on 'created_at' (original field), its position relative to $lookup doesn't matter as much
            # but generally, it's good to filter as early as possible.
            # For this case, if code and date are both present, $match will have both.
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},  # Sort after matching
        ]

        # Pipeline for fetching data with pagination
        data_pipeline = pipeline_stages + [
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        # Pipeline for counting total documents matching the filters
        count_pipeline = pipeline_stages + [{"$count": "total"}]

        # Execute data query
        # Ensure 'db' is your initialized MongoDB client/database object
        # For example, if using PyMongo:
        # cursor = db.potential_customers.aggregate(data_pipeline)
        # For Motor (async):
        # cursor = await db.potential_customers.aggregate(data_pipeline).to_list(length=limit)
        # For this synchronous example, I'll assume db.potential_customers.aggregate is sync.

        # This is a placeholder for your actual database call
        # Replace with your database interaction logic (e.g., using PyMongo or Motor)
        # For PyMongo:
        # customers_cursor = db.potential_customers.aggregate(data_pipeline)
        # customers_list = [serialize_mongo_document(doc) for doc in customers_cursor]
        # total_count_result = list(db.potential_customers.aggregate(count_pipeline))

        # Mocking database interaction for demonstration if db is not fully set up:
        # customers_list = [] # Replace with actual data fetching
        # total_count_result = [{"total": 0}] # Replace with actual count fetching

        # Assuming db is available and refers to your MongoDB database from PyMongo
        customers_cursor = db.potential_customers.aggregate(data_pipeline)
        customers_list = [serialize_mongo_document(doc) for doc in customers_cursor]

        total_count_aggregation_result = list(
            db.potential_customers.aggregate(count_pipeline)
        )

        total_count = 0
        if (
            total_count_aggregation_result
            and "total" in total_count_aggregation_result[0]
        ):
            total_count = total_count_aggregation_result[0]["total"]

        total_pages = (
            (total_count + limit - 1) // limit if total_count > 0 else 0
        )  # Changed to 0 if no customers

        # Validate page number
        if page >= total_pages and total_pages != 0:  # page is 0-indexed
            raise HTTPException(
                status_code=400,
                detail=f"Page number {page} out of range. Total pages: {total_pages}.",
            )
        # If page is 0 and total_pages is 0, it's valid (empty result)

        return {
            "potential_customers": customers_list,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:  # Re-raise HTTPExceptions
        raise
    except Exception as e:
        # Log the exception e for debugging
        print(f"An error occurred: {e}")  # Basic logging
        return JSONResponse(
            content={"error": "An internal server error occurred", "detail": str(e)},
            status_code=500,
        )


@router.get("/report")
def get_potential_customers_report(
    # Removed default for code to make it potentially required or handle if None
    code: Optional[str] = Query(None, description="Sales Person Code"),
    startDate: Optional[str] = Query(
        None, description="Start date for filtering (YYYY-MM-DD)"
    ),
    endDate: Optional[str] = Query(
        None, description="End date for filtering (YYYY-MM-DD)"
    ),
):
    try:
        match_statement = {}
        date_filter = {}

        if code:
            match_statement["created_by_info.code"] = code  # Filter on looked-up field

        # Date filtering logic (similar to get_potential_customers)
        if startDate:
            try:
                start_dt = datetime.strptime(startDate, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                date_filter["$gte"] = start_dt
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid startDate format. Use YYYY-MM-DD."
                )

        if endDate:
            try:
                end_dt = datetime.strptime(endDate, "%Y-%m-%d").replace(
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    tzinfo=timezone.utc,
                )
                date_filter["$lte"] = end_dt
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid endDate format. Use YYYY-MM-DD."
                )

        if date_filter:
            # If 'created_at' is a field in the 'potential_customers' collection directly
            match_statement["created_at"] = date_filter

        query_pipeline = [
            {
                "$lookup": {
                    "from": "users",  # Your users collection
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
            # Match stage should be here to apply all filters
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},  # Optional: sort report data
        ]

        # Fetch matching customers
        # Replace with your actual database call
        # customers_cursor = db.potential_customers.aggregate(query_pipeline)
        # customers = [serialize_mongo_document(doc) for doc in customers_cursor]

        # Mocking for demonstration if db is not fully set up:
        # customers = [] # Replace with actual data

        # Assuming db is available
        customers_cursor = db.potential_customers.aggregate(query_pipeline)
        customers = [serialize_mongo_document(doc) for doc in customers_cursor]

        # Create an Excel workbook using openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Potential Customers Report"

        headers = [
            "Name",
            "Address",
            "Tier",
            "Mobile",
            "Created At",
            "Created By",
        ]  # Added Created At
        ws.append(headers)

        for cust in customers:
            created_at_str = cust.get("created_at", "")
            # Ensure created_at is a string (it should be if serialized correctly)
            # If it's already a datetime object, format it
            if isinstance(created_at_str, datetime):
                created_at_str = created_at_str.strftime("%Y-%m-%d %H:%M:%S")

            row = [
                cust.get("name", ""),
                cust.get("address", ""),
                cust.get("tier", ""),
                cust.get("mobile", ""),
                created_at_str,  # Display created_at
                cust.get("created_by_info", {}).get(
                    "name", "N/A"
                ),  # Handle if created_by_info is missing
            ]
            ws.append(row)

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
    except HTTPException:  # Re-raise HTTPExceptions
        raise
    except Exception as e:
        print(f"An error occurred during report generation: {e}")  # Basic logging
        return JSONResponse(
            content={"error": "An internal server error occurred", "detail": str(e)},
            status_code=500,
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
