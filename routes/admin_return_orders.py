from fastapi import APIRouter, HTTPException, Query, Form, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
from config.root import connect_to_mongo, serialize_mongo_document  
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople
from dotenv import load_dotenv
import os, datetime, uuid, boto3, io
from typing import Optional
from botocore.exceptions import ClientError
from pydantic import BaseModel
import pandas as pd
from io import BytesIO

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
client, db = connect_to_mongo()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]
return_orders_collection = db["return_orders"]

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
AWS_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_S3_REGION = os.getenv("S3_REGION", "ap-south-1")  # Default to ap-south-1
AWS_S3_URL = os.getenv("S3_URL")

s3_client = boto3.client(
    "s3",
    region_name=AWS_S3_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


# Pydantic model for status update
class StatusUpdateRequest(BaseModel):
    status: str


@router.get("")
def get_return_orders(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
):
    try:
        match_statement = {}

        # Use aggregation pipeline to join with users collection
        pipeline = [
            {"$match": match_statement},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_user",
                }
            },
            {
                "$unwind": "$created_by_user",
            },
            {"$skip": page * limit},
            {"$limit": limit},
        ]

        # Get total count
        total_count = return_orders_collection.count_documents(match_statement)

        # Execute aggregation
        cursor = return_orders_collection.aggregate(pipeline)
        return_orders = [serialize_mongo_document(doc) for doc in cursor]

        # Calculate total pages
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        # Validate page number
        if page >= total_pages and total_pages > 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "return_orders": return_orders,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/download_report")
def download_return_orders_report():
    """Download all return orders as an Excel report"""
    try:
        # Use aggregation pipeline to get all return orders with user names
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_user",
                }
            },
            {"$sort": {"created_at": -1}},  # Sort by creation date, newest first
        ]

        cursor = return_orders_collection.aggregate(pipeline)
        return_orders = [serialize_mongo_document(doc) for doc in cursor]

        if not return_orders:
            raise HTTPException(status_code=404, detail="No return orders found")

        # Prepare data for Excel
        excel_data = []

        for order in return_orders:
            # Calculate total items
            total_items = sum(
                item.get("quantity", 0) for item in order.get("items", [])
            )

            # Prepare items details
            items_details = []
            for item in order.get("items", []):
                items_details.append(
                    f"{item.get('product_name', '')} (SKU: {item.get('sku', '')}, Qty: {item.get('quantity', 0)})"
                )

            items_string = " | ".join(items_details)

            # Prepare address
            pickup_address = order.get("pickup_address", {})
            full_address = ""
            if pickup_address:
                address_parts = [
                    pickup_address.get("attention", ""),
                    pickup_address.get("address", ""),
                    pickup_address.get("city", ""),
                    pickup_address.get("state", ""),
                    pickup_address.get("zip", ""),
                    pickup_address.get("country", ""),
                ]
                full_address = ", ".join([part for part in address_parts if part])

            excel_data.append(
                {
                    "Return Order ID": order.get("_id", ""),
                    "Customer Name": order.get("customer_name", ""),
                    "Customer ID": order.get("customer_id", ""),
                    "Return Form Date": order.get("return_form_date", ""),
                    "Return Date": order.get("return_date", ""),
                    "Contact Number": order.get("contact_no", ""),
                    "Box Count": order.get("box_count", ""),
                    "Status": order.get("status", "").upper(),
                    "Return Reason": order.get("return_reason", ""),
                    "Total Items": total_items,
                    "Items Details": items_string,
                    "Debit Note Document": order.get("debit_note_document", ""),
                    "Created By": order.get(
                        "created_by_user", [{"name": "Unknown User"}]
                    )[0].get("name"),
                    "Pickup Address": full_address,
                    "Pickup Phone": pickup_address.get("phone", ""),
                    "Created At": order.get("created_at", ""),
                    "Updated At": order.get("updated_at", ""),
                }
            )

        # Create DataFrame
        df = pd.DataFrame(excel_data)
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Return Orders", index=False)

            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets["Return Orders"]

            # Define formats
            header_format = workbook.add_format(
                {
                    "bold": True,
                    "text_wrap": True,
                    "valign": "top",
                    "fg_color": "#D7E4BC",
                    "border": 1,
                }
            )

            # Write headers with formatting
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # Adjust column widths
            worksheet.set_column("A:A", 25)  # Return Order ID
            worksheet.set_column("B:B", 20)  # Customer Name
            worksheet.set_column("C:C", 20)  # Customer ID
            worksheet.set_column("D:D", 18)  # Return Form Date
            worksheet.set_column("E:E", 15)  # Return Date
            worksheet.set_column("F:F", 15)  # Contact Number
            worksheet.set_column("G:G", 12)  # Box Count
            worksheet.set_column("H:H", 12)  # Status
            worksheet.set_column("I:I", 30)  # Return Reason
            worksheet.set_column("J:J", 12)  # Total Items
            worksheet.set_column("K:K", 50)  # Items Details
            worksheet.set_column("L:L", 50)  # Debit Note Document
            worksheet.set_column("M:M", 15)  # Created By
            worksheet.set_column("N:N", 40)  # Pickup Address
            worksheet.set_column("O:O", 15)  # Pickup Phone
            worksheet.set_column("P:P", 15)  # Created At
            worksheet.set_column("Q:Q", 15)  # Updated At

        output.seek(0)

        # Generate filename with current timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"return_orders_report_{timestamp}.xlsx"

        # Return as streaming response
        return StreamingResponse(
            io.BytesIO(output.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{return_order_id}")
def get_return_order_by_id(return_order_id: str):
    """Get a specific return order by ID with user name lookup"""
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Use aggregation to get return order with user name
        pipeline = [
            {"$match": {"_id": ObjectId(return_order_id)}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_user",
                }
            },
            {
                "$addFields": {
                    "created_by_name": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$created_by_user"}, 0]},
                            "then": {"$arrayElemAt": ["$created_by_user.name", 0]},
                            "else": "Unknown User",
                        }
                    }
                }
            },
            {"$project": {"created_by_user": 0}},
        ]

        cursor = return_orders_collection.aggregate(pipeline)
        return_order = list(cursor)

        if not return_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        return {"return_order": serialize_mongo_document(return_order[0])}

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{return_order_id}/status")
def update_return_order_status(
    return_order_id: str, status_request: StatusUpdateRequest
):
    """Update the status of a return order"""
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Validate status (you can customize these valid statuses)
        valid_statuses = [
            "draft",
            "pending",
            "approved",
            "rejected",
            "completed",
            "cancelled",
        ]
        if status_request.status.lower() not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status. Valid statuses are: {', '.join(valid_statuses)}",
            )

        # Check if return order exists
        existing_order = return_orders_collection.find_one(
            {"_id": ObjectId(return_order_id)}
        )
        if not existing_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        # Update the status
        update_result = return_orders_collection.update_one(
            {"_id": ObjectId(return_order_id)},
            {
                "$set": {
                    "status": status_request.status.lower(),
                    "updated_at": datetime.datetime.utcnow(),
                }
            },
        )

        if update_result.modified_count == 0:
            raise HTTPException(
                status_code=400, detail="Failed to update return order status"
            )

        # Get updated return order with user name
        pipeline = [
            {"$match": {"_id": ObjectId(return_order_id)}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_user",
                }
            },
            {
                "$addFields": {
                    "created_by_name": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$created_by_user"}, 0]},
                            "then": {"$arrayElemAt": ["$created_by_user.name", 0]},
                            "else": "Unknown User",
                        }
                    }
                }
            },
            {"$project": {"created_by_user": 0}},
        ]

        cursor = return_orders_collection.aggregate(pipeline)
        updated_order = list(cursor)[0]

        return {
            "message": f"Return order status updated to {status_request.status}",
            "return_order": serialize_mongo_document(updated_order),
        }

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{return_order_id}")
def update_return_order(return_order_id: str, update_data: dict):
    """Update a return order with any fields"""
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Check if return order exists
        existing_order = return_orders_collection.find_one(
            {"_id": ObjectId(return_order_id)}
        )
        if not existing_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        # Add updated_at timestamp
        update_data["updated_at"] = datetime.datetime.utcnow()

        # Update the return order
        update_result = return_orders_collection.update_one(
            {"_id": ObjectId(return_order_id)}, {"$set": update_data}
        )

        if update_result.modified_count == 0:
            raise HTTPException(status_code=400, detail="Failed to update return order")

        # Get updated return order with user name
        pipeline = [
            {"$match": {"_id": ObjectId(return_order_id)}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_user",
                }
            },
            {
                "$addFields": {
                    "created_by_name": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$created_by_user"}, 0]},
                            "then": {"$arrayElemAt": ["$created_by_user.name", 0]},
                            "else": "Unknown User",
                        }
                    }
                }
            },
            {"$project": {"created_by_user": 0}},
        ]

        cursor = return_orders_collection.aggregate(pipeline)
        updated_order = list(cursor)[0]

        return {
            "message": "Return order updated successfully",
            "return_order": serialize_mongo_document(updated_order),
        }

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{return_order_id}")
def delete_return_order(return_order_id: str):
    """Delete a return order"""
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Check if return order exists
        existing_order = return_orders_collection.find_one(
            {"_id": ObjectId(return_order_id)}
        )
        if not existing_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        # Delete the return order
        delete_result = return_orders_collection.delete_one(
            {"_id": ObjectId(return_order_id)}
        )

        if delete_result.deleted_count == 0:
            raise HTTPException(status_code=400, detail="Failed to delete return order")

        return {"message": "Return order deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
