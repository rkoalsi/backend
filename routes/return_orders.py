from fastapi import APIRouter, Query, HTTPException
from config.root import connect_to_mongo, serialize_mongo_document
from bson.objectid import ObjectId
from pymongo import DESCENDING
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
from config.whatsapp import send_whatsapp

router = APIRouter()

client, db = connect_to_mongo()
return_orders_collection = db["return_orders"]


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


# Pydantic models
class ReturnItem(BaseModel):
    product_name: str
    sku: Optional[str] = None
    quantity: int
    image_url: str
    product_id: str


class PickupAddress(BaseModel):
    zip: str
    country: str
    address: str
    city: str
    address_id: str
    country_code: str
    phone: Optional[str] = None
    phone_formatted: Optional[str] = None
    tax_info_id: Optional[str] = None
    attention: str
    street2: str
    state: str
    state_code: str
    fax: str


class ReturnOrderCreate(BaseModel):
    customer_name: str
    customer_id: Optional[str] = None
    return_reason: str
    return_date: Optional[datetime] = None
    status: str = "draft"
    pickup_address: PickupAddress
    items: List[ReturnItem] = []
    created_by: str  


class ReturnOrderUpdate(BaseModel):
    customer_name: Optional[str] = None
    return_reason: Optional[str] = None
    return_amount: Optional[float] = None
    return_date: Optional[datetime] = None
    status: Optional[str] = None
    items: Optional[List[ReturnItem]] = None
    pickup_address: Optional[PickupAddress] = None


# Helper function to validate ObjectId
def validate_object_id(id_str: str) -> ObjectId:
    try:
        return ObjectId(id_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID format")


def return_order_notification(params, created_by):
    try:
        sp = db.users.find_one({"_id": created_by})
        sales_admin = db.users.find_one({"email": "pupscribeinvoicee@gmail.com"})
        warehouse_admin = db.users.find_one({"email": "barkbutleracc@gmail.com"})
        customer_care_admin = db.users.find_one({"designation": "Customer Care"})

        template = db.templates.find_one({"name": "return_order_notification"})
        if not template:
            print("Warning: return_order_notification template not found")
            return

        for salesperson in [sp, sales_admin, warehouse_admin, customer_care_admin]:
            if not salesperson:
                continue
            phone = salesperson.get("phone")
            template_doc = {**template}
            parameters = {**params}
            if phone and phone != "":
                x = send_whatsapp(phone, template_doc, parameters)
    except Exception as e:
        # Log the error but don't fail the entire request
        print(f"Error sending return order notification: {str(e)}")
        # Don't raise exception - notification failure shouldn't fail the order creation


@router.post("")
async def create_return_order(return_order: ReturnOrderCreate):
    """
    Create a new return order
    """
    try:
        # Prepare the document
        order_dict = return_order.dict()

        # Convert created_by to ObjectId
        try:
            order_dict["created_by"] = ObjectId(order_dict["created_by"])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid created_by ID: {order_dict.get('created_by')} - {str(e)}")

        try:
            order_dict["customer_id"] = ObjectId(order_dict["customer_id"])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid customer_id ID: {order_dict.get('customer_id')} - {str(e)}")

        # Set default values
        order_dict["created_at"] = datetime.now()
        order_dict["updated_at"] = datetime.now()

        # Set return_date if not provided
        if not order_dict.get("return_date"):
            order_dict["return_date"] = datetime.now()

        # Convert items to dict format
        if order_dict.get("items"):
            order_dict["items"] = [
                item.dict() if hasattr(item, "dict") else item
                for item in order_dict["items"]
            ]
        for idx, item in enumerate(order_dict["items"]):
            try:
                item["product_id"] = ObjectId(item["product_id"])
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid product_id for item {idx}: {item.get('product_id')} - {str(e)}")

        # Insert the document
        result = return_orders_collection.insert_one(order_dict)
        params = {
            "customer_name": order_dict.get("customer_name"),
            "items": len(order_dict.get("items", [])),
            "status": str(order_dict.get("status", [])).capitalize(),
            "reason": str(order_dict.get("return_reason", [])).capitalize(),
            "address": format_address(order_dict.get("pickup_address", [])),
            "date": datetime.now().strftime("%d/%m/%Y"),
        }
        return_order_notification(
            params,
            created_by=ObjectId(order_dict["created_by"]),
        )
        if result.inserted_id:
            # Fetch and return the created document
            created_order = return_orders_collection.find_one(
                {"_id": result.inserted_id}
            )
            serialized_order = serialize_mongo_document(created_order)
            serialized_order["items_count"] = len(serialized_order.get("items", []))

            return {
                "message": "Return order created successfully",
                "return_order": serialized_order,
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create return order")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error creating return order: {str(e)}"
        )


@router.get("")
async def get_return_orders(
    created_by: str = Query(..., description="User ID who created the return orders"),
    search: Optional[str] = Query(
        None, description="Search by customer name or order ID"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
):
    """
    Get all return orders created by a specific user with pagination
    """
    try:
        # Validate created_by as ObjectId
        created_by_id = validate_object_id(created_by)

        # Build query
        query = {"created_by": created_by_id}

        # Add search filter if provided
        if search:
            query["$or"] = [
                {"customer_name": {"$regex": search, "$options": "i"}},
                {"original_order_id": {"$regex": search, "$options": "i"}},
                {"return_reason": {"$regex": search, "$options": "i"}},
            ]

        # Calculate skip for pagination
        skip = (page - 1) * limit

        # Execute query with pagination and sorting (newest first)
        cursor = (
            return_orders_collection.find(query)
            .sort("created_at", DESCENDING)
            .skip(skip)
            .limit(limit)
        )
        return_orders = list(cursor)

        # Get total count for pagination
        total_count = return_orders_collection.count_documents(query)

        # Serialize documents
        serialized_orders = [serialize_mongo_document(order) for order in return_orders]

        # Add items_count to each order
        for order in serialized_orders:
            order["items_count"] = len(order.get("items", []))

        return {
            "return_orders": serialized_orders,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total_count,
                "pages": (total_count + limit - 1) // limit,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching return orders: {str(e)}"
        )


@router.get("/{return_order_id}")
async def get_return_order_by_id(return_order_id: str):
    """
    Get a specific return order by ID
    """
    try:
        object_id = validate_object_id(return_order_id)

        return_order = return_orders_collection.find_one({"_id": object_id})

        if not return_order:
            raise HTTPException(status_code=404, detail="Return order not found")

        serialized_order = serialize_mongo_document(return_order)
        serialized_order["items_count"] = len(serialized_order.get("items", []))

        return {"return_order": serialized_order}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching return order: {str(e)}"
        )


@router.put("/{return_order_id}")
async def update_return_order(return_order_id: str, update_data: ReturnOrderUpdate):
    """
    Update a return order
    """
    try:
        object_id = validate_object_id(return_order_id)

        # Prepare update data (exclude None values)
        update_dict = {k: v for k, v in update_data.dict().items() if v is not None}

        if not update_dict:
            raise HTTPException(status_code=400, detail="No valid update data provided")

        # Add updated timestamp
        update_dict["updated_at"] = datetime.now()

        # Convert items to dict format if provided
        if "items" in update_dict and update_dict["items"]:
            update_dict["items"] = [
                item.dict() if hasattr(item, "dict") else item
                for item in update_dict["items"]
            ]

        # Update the document
        result = return_orders_collection.update_one(
            {"_id": object_id}, {"$set": update_dict}
        )

        data = serialize_mongo_document(
            dict(return_orders_collection.find_one({"_id": object_id}))
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Return order not found")

        # Fetch and return updated document
        updated_order = return_orders_collection.find_one({"_id": object_id})
        serialized_order = serialize_mongo_document(updated_order)
        serialized_order["items_count"] = len(serialized_order.get("items", []))
        params = {
            "customer_name": data.get("customer_name"),
            "items": len(data.get("items", [])),
            "status": str(data.get("status", [])).capitalize(),
            "reason": str(data.get("return_reason", [])).capitalize(),
            "address": format_address(data.get("pickup_address", [])),
            "date": datetime.strptime(data.get("created_at"), "%d/%m/%Y"),
        }
        return_order_notification(
            params,
            created_by=ObjectId(data["created_by"]),
        )
        return {
            "message": "Return order updated successfully",
            "return_order": serialized_order,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error updating return order: {str(e)}"
        )


@router.delete("/{return_order_id}")
async def delete_return_order(return_order_id: str):
    """
    Permanently delete a return order from database
    """
    try:
        object_id = validate_object_id(return_order_id)

        result = return_orders_collection.delete_one({"_id": object_id})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Return order not found")

        return {"message": "Return order deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error deleting return order: {str(e)}"
        )
