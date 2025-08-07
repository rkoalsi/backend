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
import os, openpyxl, io, datetime
from .helpers import notify_person

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
targeted_customers_collection = db["targeted_customers"]


@router.get("")
def get_targeted_customers(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}

        pipeline = [
            # Lookup created_by details
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
            # Lookup sales_people details
            {
                "$lookup": {
                    "from": "users",
                    "localField": "sales_people",
                    "foreignField": "_id",
                    "as": "sales_people_info",
                }
            },
            {"$match": match_statement},
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        total_count = db.targeted_customers.count_documents(match_statement)
        cursor = db.targeted_customers.aggregate(pipeline)
        cat = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")

        return {
            "targeted_customers": cat,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/report")
def get_targeted_customers_report():
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

    # Fetch matching customers
    customers_cursor = db.targeted_customers.aggregate(query)
    customers = [serialize_mongo_document(doc) for doc in customers_cursor]

    # Create an Excel workbook using openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Potential Customers Report"

    # Define the header row
    headers = ["Name", "Address", "Tier", "Created By"]
    ws.append(headers)

    for cust in customers:
        row = [
            cust.get("name", ""),
            cust.get("address", ""),
            cust.get("tier", ""),
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
            "Content-Disposition": "attachment; filename=targeted_customers_report.xlsx"
        },
    )


@router.post("")
def create_targeted_customer(
    data: dict = Body(..., description="Fields to update for the targeted customer"),
):
    try:
        data["customer_id"] = ObjectId(data.get("customer_id"))
        data["created_by"] = ObjectId(data.get("created_by"))
        data["created_at"] = datetime.datetime.now()
        if "sales_people" in data:
            data["sales_people"] = [ObjectId(sp) for sp in data.get("sales_people", [])]
            for sp in data["sales_people"]:
                person = db.users.find_one({"_id": ObjectId(sp)})
                customer = db.customers.find_one(
                    {"_id": ObjectId(data.get("customer_id"))}
                )
                template = db.templates.find_one({"name": "targeted_customers"})
                params = {
                    "name_of_customer": customer.get("contact_name", ""),
                }
                notify_person(db, template, params, person)
        targeted_customers_collection.insert_one({**data})
        return {"message": "Target Customer created successfully"}
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{customer_id}")
def update_targeted_customer(
    customer_id: str,
    update_data: dict = Body(
        ..., description="Fields to update for the targeted customer"
    ),
):
    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(customer_id)
        existing_customer = customers_collection.find_one({"_id": customer_obj_id})
        if not existing_customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        if "sales_people" in update_data:
            update_data["sales_people"] = [
                ObjectId(sp) for sp in update_data.get("sales_people", [])
            ]
        _id = update_data.pop("_id")
        update_data["created_by"] = ObjectId(update_data.get("created_by"))
        update_data["customer_id"] = ObjectId(update_data.get("customer_id"))
        update_data["updated_at"] = datetime.datetime.now()
        targeted_customers_collection.update_one(
            {"_id": ObjectId(_id)}, {"$set": update_data}
        )
        return {"message": "Customer updated successfully"}
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{customer_id}")
def delete_targeted_customer(customer_id: str):
    try:
        if not ObjectId.is_valid(customer_id):
            raise HTTPException(status_code=400, detail="Invalid customer ID")

        customer_obj_id = ObjectId(customer_id)
        result = targeted_customers_collection.delete_one({"_id": customer_obj_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Customer not found")

        return {"message": "Customer deleted successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
