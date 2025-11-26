from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from bson import ObjectId
from config.root import get_database, serialize_mongo_document
from config.auth import get_current_user
from routes.helpers import notify_sales_admin

router = APIRouter()

class CustomerCreationRequest(BaseModel):
    shop_name: str
    customer_name: str
    address: str
    gst_no: Optional[str] = None
    pan_card_no: Optional[str] = None
    whatsapp_no: str
    payment_terms: str
    multiple_branches: str
    tier_category: str
    sales_person: str
    margin_details: Optional[str] = None

class CommentCreate(BaseModel):
    text: str

class ReplyCreate(BaseModel):
    reply: str
    user_id: str
    user_name: str
    user_role: str

@router.post("/")
async def create_customer_request(
    request_data: CustomerCreationRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a new customer creation request"""
    try:
        db = get_database()

        # Extract user data from JWT payload (nested under "data" key)
        user_data = current_user.get("data", {})

        # Get user_id as ObjectId
        user_id = user_data.get("_id")
        if isinstance(user_id, str):
            user_id = ObjectId(user_id)
        elif user_id is None:
            raise HTTPException(status_code=400, detail="User ID not found in token")

        # Build created_by_name with fallbacks
        first_name = user_data.get('first_name', '').strip()
        last_name = user_data.get('last_name', '').strip()
        created_by_name = f"{first_name} {last_name}".strip()

        # Fallback to email or code if name is empty
        if not created_by_name:
            created_by_name = user_data.get('email') or user_data.get('code') or 'Unknown User'

        # Prepare the request document
        request_doc = {
            "shop_name": request_data.shop_name,
            "customer_name": request_data.customer_name,
            "address": request_data.address,
            "gst_no": request_data.gst_no,
            "pan_card_no": request_data.pan_card_no,
            "whatsapp_no": request_data.whatsapp_no,
            "payment_terms": request_data.payment_terms,
            "multiple_branches": request_data.multiple_branches,
            "tier_category": request_data.tier_category,
            "sales_person": request_data.sales_person,
            "margin_details": request_data.margin_details,
            "created_by": user_id,
            "created_by_name": created_by_name,
            "created_at": datetime.now(),
            "status": "pending"
        }

        # Insert into database
        result = db.customer_creation_requests.insert_one(request_doc)

        # Notify admin
        try:
            template = db.templates.find_one({"name": "customer_creation_request"})
            if template:
                params = {
                    "sales_person_name": request_doc["created_by_name"],
                    "shop_name": request_data.shop_name,
                    "customer_name": request_data.customer_name,
                }
                notify_sales_admin(db, template, params)
        except Exception as e:
            print(f"Failed to send notification: {e}")

        return {
            "message": "Customer creation request submitted successfully",
            "request_id": str(result.inserted_id)
        }

    except Exception as e:
        print(f"Error creating customer request: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/")
async def get_customer_requests(
    current_user: dict = Depends(get_current_user),
    page: int = 1,
    limit: int = 10,
    status: Optional[str] = None
):
    """Get customer creation requests - admins see all, sales people see only their own"""
    try:
        db = get_database()

        # Extract user data from JWT payload (nested under "data" key)
        user_data = current_user.get("data", {})

        # Get user_id as ObjectId
        user_id = user_data.get("_id")
        if isinstance(user_id, str):
            user_id = ObjectId(user_id)

        # Build filter
        filter_query = {}

        # If user is sales_person, only show their own requests
        user_role = user_data.get("role", "")
        if user_role == "sales_person":
            filter_query["created_by"] = user_id

        if status:
            filter_query["status"] = status

        # Calculate skip value
        skip = (page - 1) * limit

        # Get total count
        total_count = db.customer_creation_requests.count_documents(filter_query)

        # Get requests sorted by latest first
        requests = list(
            db.customer_creation_requests.find(filter_query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(limit)
        )

        # Serialize the results
        serialized_requests = [serialize_mongo_document(req) for req in requests]

        return {
            "requests": serialized_requests,
            "total_count": total_count,
            "page": page,
            "per_page": limit
        }

    except Exception as e:
        print(f"Error fetching customer requests: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{request_id}/status")
async def update_request_status(
    request_id: str,
    status: str,
    current_user: dict = Depends(get_current_user)
):
    """Update the status of a customer creation request (admin only)"""
    try:
        db = get_database()

        # Extract user data from JWT payload (nested under "data" key)
        user_data = current_user.get("data", {})

        # Get user_id as ObjectId
        user_id = user_data.get("_id")
        if isinstance(user_id, str):
            user_id = ObjectId(user_id)

        # Validate status
        valid_statuses = ["pending", "approved", "rejected", "admin_commented", "salesperson_replied"]
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")

        # Update the request
        result = db.customer_creation_requests.update_one(
            {"_id": ObjectId(request_id)},
            {
                "$set": {
                    "status": status,
                    "updated_at": datetime.now(),
                    "updated_by": user_id
                }
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request not found")

        return {"message": "Request status updated successfully"}

    except Exception as e:
        print(f"Error updating request status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{request_id}/comments")
async def add_comment(
    request_id: str,
    comment_data: CommentCreate,
    current_user: dict = Depends(get_current_user)
):
    """Add an admin comment to a customer creation request"""
    try:
        db = get_database()

        # Extract user data from JWT payload
        user_data = current_user.get("data", {})

        # Build admin name
        first_name = user_data.get('first_name', '').strip()
        last_name = user_data.get('last_name', '').strip()
        admin_name = f"{first_name} {last_name}".strip()

        if not admin_name:
            admin_name = user_data.get('email') or user_data.get('code') or 'Admin'

        # Create comment object
        comment = {
            "_id": str(ObjectId()),
            "admin_id": user_data.get("_id"),
            "admin_name": admin_name,
            "text": comment_data.text,
            "created_at": datetime.now(),
            "updated_at": None,
            "reply": None
        }

        # Add comment to the request and update status
        result = db.customer_creation_requests.update_one(
            {"_id": ObjectId(request_id)},
            {
                "$push": {"admin_comments": comment},
                "$set": {"status": "admin_commented"}
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request not found")

        return {"message": "Comment added successfully", "comment": serialize_mongo_document(comment)}

    except Exception as e:
        print(f"Error adding comment: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{request_id}/comments/{comment_id}/reply")
async def add_reply(
    request_id: str,
    comment_id: str,
    reply_data: ReplyCreate,
    current_user: dict = Depends(get_current_user)
):
    """Add a reply to an admin comment (from sales person)"""
    try:
        db = get_database()

        # Create reply object
        reply = {
            "user_id": reply_data.user_id,
            "user_name": reply_data.user_name,
            "user_role": reply_data.user_role,
            "text": reply_data.reply,
            "created_at": datetime.now(),
            "updated_at": None
        }

        # Update the specific comment with the reply and change status
        result = db.customer_creation_requests.update_one(
            {
                "_id": ObjectId(request_id),
                "admin_comments._id": comment_id
            },
            {
                "$set": {
                    "admin_comments.$.reply": reply,
                    "status": "salesperson_replied"
                }
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request or comment not found")

        return {"message": "Reply added successfully"}

    except Exception as e:
        print(f"Error adding reply: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{request_id}/comments/{comment_id}/reply")
async def update_reply(
    request_id: str,
    comment_id: str,
    reply_data: ReplyCreate,
    current_user: dict = Depends(get_current_user)
):
    """Update a reply to an admin comment"""
    try:
        db = get_database()

        # Update the reply text and updated_at timestamp
        result = db.customer_creation_requests.update_one(
            {
                "_id": ObjectId(request_id),
                "admin_comments._id": comment_id
            },
            {
                "$set": {
                    "admin_comments.$.reply.text": reply_data.reply,
                    "admin_comments.$.reply.updated_at": datetime.now()
                }
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request or comment not found")

        return {"message": "Reply updated successfully"}

    except Exception as e:
        print(f"Error updating reply: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{request_id}/comments/{comment_id}/reply")
async def delete_reply(
    request_id: str,
    comment_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a reply to an admin comment"""
    try:
        db = get_database()

        # Remove the reply from the comment
        result = db.customer_creation_requests.update_one(
            {
                "_id": ObjectId(request_id),
                "admin_comments._id": comment_id
            },
            {
                "$set": {
                    "admin_comments.$.reply": None
                }
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request or comment not found")

        return {"message": "Reply deleted successfully"}

    except Exception as e:
        print(f"Error deleting reply: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{request_id}")
async def update_customer_request(
    request_id: str,
    request_data: CustomerCreationRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update a customer creation request (by creator or admin)"""
    try:
        db = get_database()

        # Extract user data from JWT payload
        user_data = current_user.get("data", {})

        # Get user_id as ObjectId
        user_id = user_data.get("_id")
        if isinstance(user_id, str):
            user_id = ObjectId(user_id)

        # Find the existing request
        existing_request = db.customer_creation_requests.find_one({"_id": ObjectId(request_id)})
        if not existing_request:
            raise HTTPException(status_code=404, detail="Request not found")

        # Check if user has permission to edit (creator or admin)
        user_role = user_data.get("role", "")
        is_creator = existing_request.get("created_by") == user_id
        is_admin = user_role not in ["sales_person"]

        if not (is_creator or is_admin):
            raise HTTPException(status_code=403, detail="Not authorized to edit this request")

        # Prepare update document
        update_doc = {
            "shop_name": request_data.shop_name,
            "customer_name": request_data.customer_name,
            "address": request_data.address,
            "gst_no": request_data.gst_no,
            "pan_card_no": request_data.pan_card_no,
            "whatsapp_no": request_data.whatsapp_no,
            "payment_terms": request_data.payment_terms,
            "multiple_branches": request_data.multiple_branches,
            "tier_category": request_data.tier_category,
            "sales_person": request_data.sales_person,
            "margin_details": request_data.margin_details,
            "updated_at": datetime.now(),
            "updated_by": user_id
        }

        # Update the request
        result = db.customer_creation_requests.update_one(
            {"_id": ObjectId(request_id)},
            {"$set": update_doc}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request not found or no changes made")

        return {"message": "Customer creation request updated successfully"}

    except Exception as e:
        print(f"Error updating customer request: {e}")
        raise HTTPException(status_code=500, detail=str(e))
