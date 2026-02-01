from fastapi import APIRouter, HTTPException, Query, Form, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople, get_access_token
from dotenv import load_dotenv
import os, datetime, uuid, boto3, io, requests
from typing import Optional
from botocore.exceptions import ClientError
from pydantic import BaseModel
import pandas as pd
from io import BytesIO

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
ZOHO_INVENTORY_BASE_URL = "https://www.zohoapis.com/inventory/v1"
db = get_database()
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


def find_salesorder_for_return(
    customer_id: str, product_skus: list, product_names: list = None
) -> dict:
    """
    Auto-detect salesorder_id from shipments/invoices based on customer_id and product SKUs/names.
    Returns dict with salesorder_id and line_items mapping if found.
    """
    try:
        # Get customer's contact_id and customer_id from customers collection
        customer = customers_collection.find_one({"_id": ObjectId(customer_id)})
        if not customer:
            return {"error": "Customer not found", "salesorder_id": None}

        contact_id = customer.get("contact_id")
        zoho_customer_id = customer.get("customer_id")  # This is the Zoho customer_id

        if not contact_id and not zoho_customer_id:
            return {"error": "Customer has no contact_id or customer_id", "salesorder_id": None}

        # Normalize SKUs and names for comparison
        product_skus_lower = [sku.lower().strip() for sku in product_skus if sku]
        product_names_lower = [
            name.lower().strip() for name in (product_names or []) if name
        ]

        def match_line_item(line_item):
            """Check if a line item matches any of our return products"""
            # Try matching by SKU
            item_sku = str(line_item.get("sku", "")).lower().strip()
            if item_sku and item_sku in product_skus_lower:
                return item_sku

            # Try matching by item_custom_fields SKU
            custom_fields = line_item.get("item_custom_fields", [])
            for cf in custom_fields:
                if cf.get("label", "").lower() in ["sku code", "sku", "cf_sku_code"]:
                    cf_sku = str(cf.get("value", "")).lower().strip()
                    if cf_sku and cf_sku in product_skus_lower:
                        return cf_sku

            # Try matching by name as fallback
            item_name = str(line_item.get("name", "")).lower().strip()
            if item_name and item_name in product_names_lower:
                return item_name

            return None

        # Build query to match customer_id using both contact_id AND customer_id from customer document
        # This handles different data type formats and the fact that invoices may store
        # either the contact_id or customer_id from Zoho
        customer_id_conditions = []

        # Add conditions for contact_id if available
        if contact_id:
            customer_id_conditions.extend([
                {"customer_id": contact_id},
                {"customer_id": str(contact_id)},
            ])
            if str(contact_id).isdigit():
                customer_id_conditions.append({"customer_id": int(contact_id)})

        # Add conditions for zoho_customer_id if available and different from contact_id
        if zoho_customer_id and str(zoho_customer_id) != str(contact_id):
            customer_id_conditions.extend([
                {"customer_id": zoho_customer_id},
                {"customer_id": str(zoho_customer_id)},
            ])
            if str(zoho_customer_id).isdigit():
                customer_id_conditions.append({"customer_id": int(zoho_customer_id)})

        if not customer_id_conditions:
            return {"error": "No valid customer identifiers found", "salesorder_id": None}

        customer_id_query = {"$or": customer_id_conditions}

        # Search for shipments with this customer that have matching products
        # Look for the most recent shipment containing any of the return products
        shipments = list(
            db.shipments.find(customer_id_query).sort("date", -1).limit(20)
        )

        for shipment in shipments:
            salesorder_id = shipment.get("salesorder_id")
            if not salesorder_id:
                continue

            line_items = shipment.get("line_items", [])
            matching_items = []

            for line_item in line_items:
                matched_key = match_line_item(line_item)
                if matched_key:
                    matching_items.append(
                        {
                            "item_id": line_item.get("item_id"),
                            "salesorder_item_id": line_item.get("salesorder_item_id")
                            or line_item.get("so_line_item_id"),
                            "sku": line_item.get("sku", ""),
                            "name": line_item.get("name", ""),
                            "matched_by": matched_key,
                        }
                    )

            if matching_items:
                return {
                    "salesorder_id": salesorder_id,
                    "salesorder_number": shipment.get("salesorder_number", ""),
                    "line_items": matching_items,
                    "shipment_id": shipment.get("shipment_id"),
                    "source": "shipment",
                }

        # If no shipment found, try invoices
        invoices = list(
            db.invoices.find(customer_id_query).sort("date", -1).limit(20)
        )

        for invoice in invoices:
            salesorder_id = invoice.get("salesorder_id")
            if not salesorder_id:
                continue

            line_items = invoice.get("line_items", [])
            matching_items = []

            for line_item in line_items:
                matched_key = match_line_item(line_item)
                if matched_key:
                    matching_items.append(
                        {
                            "item_id": line_item.get("item_id"),
                            "salesorder_item_id": line_item.get("so_line_item_id")
                            or line_item.get("salesorder_item_id"),
                            "sku": line_item.get("sku", ""),
                            "name": line_item.get("name", ""),
                            "matched_by": matched_key,
                        }
                    )

            if matching_items:
                return {
                    "salesorder_id": salesorder_id,
                    "salesorder_number": invoice.get("reference_number", ""),
                    "line_items": matching_items,
                    "invoice_id": invoice.get("invoice_id"),
                    "source": "invoice",
                }

        return {
            "error": f"No matching sales order found for customer. Searched {len(shipments)} shipments and {len(invoices)} invoices with contact_id: {contact_id}, customer_id: {zoho_customer_id}.",
            "salesorder_id": None,
            "debug": {
                "contact_id": contact_id,
                "zoho_customer_id": zoho_customer_id,
                "shipments_searched": len(shipments),
                "invoices_searched": len(invoices),
                "product_skus_searched": product_skus_lower,
            },
        }

    except Exception as e:
        print(f"Error finding salesorder: {e}")
        return {"error": str(e), "salesorder_id": None}


def create_zoho_sales_return(return_order: dict) -> dict:
    """
    Create a sales return in Zoho Inventory.
    Returns the Zoho response or error details.
    """
    try:
        # Get access token for Zoho Inventory
        access_token = get_access_token("inventory")
        if not access_token:
            return {"success": False, "error": "Failed to get Zoho access token"}

        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type": "application/json",
        }

        # Extract product SKUs and names from return order items
        return_items = return_order.get("items", [])
        product_skus = []
        product_names = []
        sku_to_quantity = {}
        name_to_quantity = {}

        for item in return_items:
            sku = item.get("sku", "")
            name = item.get("product_name", "")
            quantity = item.get("quantity", 0)

            if sku:
                product_skus.append(sku)
                sku_to_quantity[sku.lower().strip()] = quantity
            if name:
                product_names.append(name)
                name_to_quantity[name.lower().strip()] = quantity

        if not product_skus and not product_names:
            return {
                "success": False,
                "error": "No product SKUs or names found in return order",
            }

        # Find the salesorder_id from shipments/invoices
        customer_id = str(return_order.get("customer_id", ""))
        salesorder_info = find_salesorder_for_return(
            customer_id, product_skus, product_names
        )

        if not salesorder_info.get("salesorder_id"):
            return {
                "success": False,
                "error": salesorder_info.get(
                    "error", "Could not find associated sales order"
                ),
            }

        salesorder_id = salesorder_info["salesorder_id"]

        # Fetch the sales order from Zoho to get accurate salesorder_item_ids
        so_line_items_map = {}
        try:
            so_url = f"{ZOHO_INVENTORY_BASE_URL}/salesorders/{salesorder_id}"
            so_params = {"organization_id": org_id}
            so_response = requests.get(so_url, headers=headers, params=so_params)
            so_data = so_response.json()
            if so_response.status_code == 200 and so_data.get("code") == 0:
                so_line_items = so_data.get("salesorder", {}).get("line_items", [])
                for so_item in so_line_items:
                    item_id = str(so_item.get("item_id", ""))
                    if item_id:
                        so_line_items_map[item_id] = so_item.get("line_item_id")
            else:
                print(f"Warning: Could not fetch sales order {salesorder_id} from Zoho: {so_data.get('message')}")
        except Exception as e:
            print(f"Warning: Error fetching sales order from Zoho: {e}")

        # Build line_items for Zoho API
        zoho_line_items = []
        for matched_item in salesorder_info.get("line_items", []):
            matched_by = matched_item.get("matched_by", "").lower().strip()

            # Try to find quantity by SKU first, then by name
            quantity = sku_to_quantity.get(matched_by, 0)
            if quantity == 0:
                quantity = name_to_quantity.get(matched_by, 0)
            # Also try the original values
            if quantity == 0:
                sku = matched_item.get("sku", "").lower().strip()
                quantity = sku_to_quantity.get(sku, 0)
            if quantity == 0:
                name = matched_item.get("name", "").lower().strip()
                quantity = name_to_quantity.get(name, 0)

            if quantity > 0 and matched_item.get("item_id"):
                item_id_str = str(matched_item["item_id"])
                line_item = {
                    "item_id": matched_item["item_id"],
                    "quantity": quantity,
                }
                # Get salesorder_item_id from Zoho sales order, fall back to local data
                so_item_id = so_line_items_map.get(item_id_str)
                if so_item_id:
                    line_item["salesorder_item_id"] = so_item_id
                elif matched_item.get("salesorder_item_id"):
                    line_item["salesorder_item_id"] = matched_item["salesorder_item_id"]

                zoho_line_items.append(line_item)

        if not zoho_line_items:
            return {
                "success": False,
                "error": "No matching line items found for Zoho sales return",
            }

        # Prepare the sales return payload
        sales_return_payload = {
            "date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "reason": return_order.get("return_reason", "Customer return"),
            "line_items": zoho_line_items,
        }

        # Make the API request to create sales return
        url = f"{ZOHO_INVENTORY_BASE_URL}/salesreturns"
        params = {
            "organization_id": org_id,
            "salesorder_id": salesorder_id,
        }

        print(f"Creating Zoho sales return with payload: {sales_return_payload}")
        print(f"URL params: {params}")

        response = requests.post(
            url, headers=headers, json=sales_return_payload, params=params
        )

        response_data = response.json()
        print(f"Zoho sales return response: {response_data}")

        if response.status_code == 201 and response_data.get("code") == 0:
            sales_return = response_data.get("salesreturn", {})
            return {
                "success": True,
                "salesreturn_id": sales_return.get("salesreturn_id"),
                "salesreturn_number": sales_return.get("salesreturn_number"),
                "salesorder_id": salesorder_id,
                "salesorder_number": salesorder_info.get("salesorder_number"),
                "status": sales_return.get("salesreturn_status"),
            }
        else:
            return {
                "success": False,
                "error": response_data.get("message", "Unknown Zoho API error"),
                "code": response_data.get("code"),
            }

    except Exception as e:
        print(f"Error creating Zoho sales return: {e}")
        return {"success": False, "error": str(e)}


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
            {"$sort": {"created_at": -1}},
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
                "$unwind": {
                    "path": "$created_by_user",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$lookup": {
                    "from": "sales_returns",
                    "localField": "zoho_salesreturn_id",
                    "foreignField": "salesreturn_id",
                    "as": "zoho_salesreturn_doc",
                }
            },
            {
                "$unwind": {
                    "path": "$zoho_salesreturn_doc",
                    "preserveNullAndEmptyArrays": True,
                }
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
                    "Zoho Sales Return ID": order.get("zoho_salesreturn_id", ""),
                    "Zoho Sales Return Number": order.get(
                        "zoho_salesreturn_number", ""
                    ),
                    "Zoho Sales Order Number": order.get("zoho_salesorder_number", ""),
                    "Zoho Sales Return Status": order.get(
                        "zoho_salesreturn_status", ""
                    ),
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
            worksheet.set_column("P:P", 20)  # Zoho Sales Return ID
            worksheet.set_column("Q:Q", 20)  # Zoho Sales Return Number
            worksheet.set_column("R:R", 20)  # Zoho Sales Order Number
            worksheet.set_column("S:S", 18)  # Zoho Sales Return Status
            worksheet.set_column("T:T", 15)  # Created At
            worksheet.set_column("U:U", 15)  # Updated At

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
            "picked_up",
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

        new_status = status_request.status.lower()
        old_status = existing_order.get("status", "")

        # Prepare update data
        update_data = {
            "status": new_status,
            "updated_at": datetime.datetime.now(datetime.timezone.utc),
        }

        # If status is being changed to "approved" and Zoho sales return hasn't been created yet
        zoho_result = None
        if new_status == "approved" and not existing_order.get("zoho_salesreturn_id"):
            print(f"Creating Zoho sales return for return order {return_order_id}")
            zoho_result = create_zoho_sales_return(existing_order)

            if zoho_result.get("success"):
                update_data["zoho_salesreturn_id"] = zoho_result.get("salesreturn_id")
                update_data["zoho_salesreturn_number"] = zoho_result.get(
                    "salesreturn_number"
                )
                update_data["zoho_salesorder_id"] = zoho_result.get("salesorder_id")
                update_data["zoho_salesorder_number"] = zoho_result.get(
                    "salesorder_number"
                )
                update_data["zoho_salesreturn_status"] = zoho_result.get("status")
                update_data["zoho_salesreturn_created_at"] = datetime.datetime.now(datetime.timezone.utc)
                print(f"Zoho sales return created successfully: {zoho_result}")
            else:
                print(f"Failed to create Zoho sales return: {zoho_result.get('error')}")
                # Don't block status update, but include warning in response

        # Update the status
        update_result = return_orders_collection.update_one(
            {"_id": ObjectId(return_order_id)},
            {"$set": update_data},
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
                "$unwind": {
                    "path": "$created_by_user",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$lookup": {
                    "from": "sales_returns",
                    "localField": "zoho_salesreturn_id",
                    "foreignField": "salesreturn_id",
                    "as": "zoho_salesreturn_doc",
                }
            },
            {
                "$unwind": {
                    "path": "$zoho_salesreturn_doc",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]

        cursor = return_orders_collection.aggregate(pipeline)
        updated_order = list(cursor)[0]

        response = {
            "message": f"Return order status updated to {status_request.status}",
            "return_order": serialize_mongo_document(updated_order),
        }

        # Add Zoho sales return info to response if attempted
        if zoho_result:
            if zoho_result.get("success"):
                response["zoho_salesreturn"] = {
                    "created": True,
                    "salesreturn_id": zoho_result.get("salesreturn_id"),
                    "salesreturn_number": zoho_result.get("salesreturn_number"),
                }
            else:
                response["zoho_salesreturn"] = {
                    "created": False,
                    "error": zoho_result.get("error"),
                }

        return response

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

        # Convert customer_id to ObjectId if provided
        if "customer_id" in update_data and update_data["customer_id"]:
            if ObjectId.is_valid(update_data["customer_id"]):
                update_data["customer_id"] = ObjectId(update_data["customer_id"])

        # Add updated_at timestamp
        update_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)

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
                "$unwind": {
                    "path": "$created_by_user",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$lookup": {
                    "from": "sales_returns",
                    "localField": "zoho_salesreturn_id",
                    "foreignField": "salesreturn_id",
                    "as": "zoho_salesreturn_doc",
                }
            },
            {
                "$unwind": {
                    "path": "$zoho_salesreturn_doc",
                    "preserveNullAndEmptyArrays": True,
                }
            },
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


@router.post("/{return_order_id}/create-zoho-salesreturn")
def create_zoho_salesreturn_for_return_order(return_order_id: str):
    """
    Manually create a Zoho sales return for a return order.
    Useful for retrying if auto-creation failed or for creating sales return
    for orders that were approved before this feature was implemented.
    """
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Check if return order exists
        return_order = return_orders_collection.find_one(
            {"_id": ObjectId(return_order_id)}
        )
        if not return_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        # Check if Zoho sales return already exists
        if return_order.get("zoho_salesreturn_id"):
            return {
                "message": "Zoho sales return already exists for this return order",
                "zoho_salesreturn": {
                    "salesreturn_id": return_order.get("zoho_salesreturn_id"),
                    "salesreturn_number": return_order.get("zoho_salesreturn_number"),
                    "salesorder_id": return_order.get("zoho_salesorder_id"),
                    "status": return_order.get("zoho_salesreturn_status"),
                },
            }

        # Create Zoho sales return
        zoho_result = create_zoho_sales_return(return_order)

        if zoho_result.get("success"):
            # Update the return order with Zoho info
            return_orders_collection.update_one(
                {"_id": ObjectId(return_order_id)},
                {
                    "$set": {
                        "zoho_salesreturn_id": zoho_result.get("salesreturn_id"),
                        "zoho_salesreturn_number": zoho_result.get(
                            "salesreturn_number"
                        ),
                        "zoho_salesorder_id": zoho_result.get("salesorder_id"),
                        "zoho_salesorder_number": zoho_result.get("salesorder_number"),
                        "zoho_salesreturn_status": zoho_result.get("status"),
                        "zoho_salesreturn_created_at": datetime.datetime.now(datetime.timezone.utc),
                        "updated_at": datetime.datetime.now(datetime.timezone.utc),
                    }
                },
            )

            return {
                "message": "Zoho sales return created successfully",
                "zoho_salesreturn": {
                    "salesreturn_id": zoho_result.get("salesreturn_id"),
                    "salesreturn_number": zoho_result.get("salesreturn_number"),
                    "salesorder_id": zoho_result.get("salesorder_id"),
                    "salesorder_number": zoho_result.get("salesorder_number"),
                    "status": zoho_result.get("status"),
                },
            }
        else:
            # Return more detailed error info
            error_detail = {
                "message": f"Failed to create Zoho sales return: {zoho_result.get('error')}",
                "details": {
                    "customer_id": str(return_order.get("customer_id", "")),
                    "customer_name": return_order.get("customer_name", ""),
                    "items_count": len(return_order.get("items", [])),
                    "items": [
                        {"sku": item.get("sku"), "name": item.get("product_name")}
                        for item in return_order.get("items", [])
                    ],
                },
            }
            if zoho_result.get("code"):
                error_detail["zoho_code"] = zoho_result.get("code")
            if zoho_result.get("debug"):
                error_detail["debug"] = zoho_result.get("debug")
            raise HTTPException(status_code=400, detail=error_detail)

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/{return_order_id}/zoho-salesreturn")
def get_zoho_salesreturn_status(return_order_id: str):
    """
    Get the Zoho sales return details for a return order.
    If a salesreturn exists, also fetches the latest status from Zoho.
    """
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(return_order_id):
            raise HTTPException(status_code=400, detail="Invalid return order ID")

        # Check if return order exists
        return_order = return_orders_collection.find_one(
            {"_id": ObjectId(return_order_id)}
        )
        if not return_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        zoho_salesreturn_id = return_order.get("zoho_salesreturn_id")

        if not zoho_salesreturn_id:
            return {
                "has_zoho_salesreturn": False,
                "message": "No Zoho sales return associated with this return order",
            }

        # Try to fetch latest status from Zoho
        try:
            access_token = get_access_token("inventory")
            headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

            url = f"{ZOHO_INVENTORY_BASE_URL}/salesreturns/{zoho_salesreturn_id}"
            params = {"organization_id": org_id}

            response = requests.get(url, headers=headers, params=params)
            response_data = response.json()

            if response.status_code == 200 and response_data.get("code") == 0:
                sales_return = response_data.get("salesreturn", {})

                # Update status in database if changed
                new_status = sales_return.get("salesreturn_status")
                if new_status and new_status != return_order.get(
                    "zoho_salesreturn_status"
                ):
                    return_orders_collection.update_one(
                        {"_id": ObjectId(return_order_id)},
                        {"$set": {"zoho_salesreturn_status": new_status}},
                    )

                return {
                    "has_zoho_salesreturn": True,
                    "zoho_salesreturn": {
                        "salesreturn_id": sales_return.get("salesreturn_id"),
                        "salesreturn_number": sales_return.get("salesreturn_number"),
                        "status": sales_return.get("salesreturn_status"),
                        "date": sales_return.get("date"),
                        "reason": sales_return.get("reason"),
                        "salesorder_id": sales_return.get("salesorder_id"),
                        "salesorder_number": sales_return.get("salesorder_number"),
                    },
                }
        except Exception as e:
            print(f"Error fetching Zoho sales return status: {e}")

        # Return stored data if Zoho fetch fails
        return {
            "has_zoho_salesreturn": True,
            "zoho_salesreturn": {
                "salesreturn_id": return_order.get("zoho_salesreturn_id"),
                "salesreturn_number": return_order.get("zoho_salesreturn_number"),
                "status": return_order.get("zoho_salesreturn_status"),
                "salesorder_id": return_order.get("zoho_salesorder_id"),
                "salesorder_number": return_order.get("zoho_salesorder_number"),
                "created_at": return_order.get("zoho_salesreturn_created_at"),
            },
            "note": "Could not fetch latest status from Zoho, showing stored data",
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
