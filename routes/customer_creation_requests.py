from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document
from ..config.auth import get_current_user
from .helpers import notify_sales_admin
from ..config.whatsapp import send_whatsapp
import os
import requests
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Zoho configuration from environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRANT_TYPE = os.getenv("GRANT_TYPE")
BOOKS_REFRESH_TOKEN = os.getenv("BOOKS_REFRESH_TOKEN")
ORG_ID = os.getenv("ORG_ID")
BOOKS_URL = os.getenv("BOOKS_URL")

class AddressModel(BaseModel):
    """Structured address following Zoho Books API format"""
    attention: Optional[str] = None
    address: Optional[str] = None  # Street 1
    street2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    state_code: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = "India"
    phone: Optional[str] = None
    fax: Optional[str] = None

class CustomerCreationRequest(BaseModel):
    shop_name: str
    customer_name: str
    address: str  # Keep for backward compatibility
    gst_no: Optional[str] = None
    pan_card_no: Optional[str] = None
    whatsapp_no: str
    payment_terms: str
    multiple_branches: str
    tier_category: str
    sales_person: str
    margin_details: Optional[str] = None
    # New structured address fields
    billing_address: Optional[AddressModel] = None
    shipping_address: Optional[AddressModel] = None
    place_of_supply: Optional[str] = None
    customer_mail_id: Optional[str] = None
    gst_treatment: Optional[str] = None
    pincode: Optional[str] = None

class CommentCreate(BaseModel):
    text: str

class ReplyCreate(BaseModel):
    reply: str
    user_id: str
    user_name: str
    user_role: str

# Zoho Helper Functions

# Indian state to state code mapping for Zoho Books (official Zoho codes)
INDIAN_STATE_CODES = {
    "andaman and nicobar islands": "AN",
    "andhra pradesh": "AD",  # Zoho uses AD, not AP
    "arunachal pradesh": "AR",
    "assam": "AS",
    "bihar": "BR",
    "chandigarh": "CH",
    "chhattisgarh": "CG",  # Zoho uses CG, not CT
    "dadra and nagar haveli and daman and diu": "DN",  # Zoho uses DN
    "daman and diu": "DD",  # Separate entry
    "delhi": "DL",
    "goa": "GA",
    "gujarat": "GJ",
    "haryana": "HR",
    "himachal pradesh": "HP",
    "jammu and kashmir": "JK",
    "jharkhand": "JH",
    "karnataka": "KA",
    "kerala": "KL",
    "ladakh": "LA",
    "lakshadweep": "LD",
    "madhya pradesh": "MP",
    "maharashtra": "MH",
    "manipur": "MN",
    "meghalaya": "ML",
    "mizoram": "MZ",
    "nagaland": "NL",
    "odisha": "OD",  # Zoho uses OD, not OR
    "puducherry": "PY",
    "punjab": "PB",
    "rajasthan": "RJ",
    "sikkim": "SK",
    "tamil nadu": "TN",
    "telangana": "TS",  # Zoho uses TS, not TG
    "tripura": "TR",
    "uttar pradesh": "UP",
    "uttarakhand": "UK",
    "west bengal": "WB"
}

def get_state_code(state_name: str) -> str:
    """
    Get the state code for a given Indian state name.
    Returns the state code if found, otherwise returns the original state name.
    """
    if not state_name:
        return ""

    # Try exact match (case-insensitive)
    state_code = INDIAN_STATE_CODES.get(state_name.lower().strip())
    if state_code:
        return state_code

    # If not found, return the original state name
    return state_name

def get_zoho_books_access_token() -> Optional[str]:
    """
    Get access token for Zoho Books API using refresh token.

    Returns:
        str: Access token if successful, None otherwise
    """
    if not all([CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, BOOKS_REFRESH_TOKEN, BOOKS_URL]):
        logger.error("Missing Zoho Books configuration in environment variables")
        return None

    try:
        url = BOOKS_URL.format(
            clientId=CLIENT_ID,
            clientSecret=CLIENT_SECRET,
            grantType=GRANT_TYPE,
            books_refresh_token=BOOKS_REFRESH_TOKEN,
        )

        response = requests.post(url, timeout=30)

        if response.status_code == 200:
            data = response.json()
            access_token = data.get("access_token", "")
            logger.info(f"Got Zoho Books Access Token: ...{access_token[-4:]}")
            return access_token
        else:
            logger.error(f"Failed to get access token: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        logger.error(f"Error getting Zoho Books access token: {e}")
        return None


def create_zoho_contact(customer_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a contact (customer) in Zoho Books.

    Args:
        customer_data: Dictionary containing customer information

    Returns:
        dict: Response with 'success' (bool), 'contact_id' (str if success), and 'message' (str)
    """
    if not ORG_ID:
        logger.error("Missing ORG_ID in environment variables")
        return {"success": False, "message": "Zoho organization ID not configured"}

    # Get access token
    access_token = get_zoho_books_access_token()
    if not access_token:
        return {"success": False, "message": "Failed to get Zoho access token"}

    # Prepare the request
    url = f"https://www.zohoapis.com/books/v3/contacts?organization_id={ORG_ID}"

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }

    # Build the contact payload according to Zoho Books API
    contact_payload = {
        "contact_name": customer_data.get("shop_name", ""),
        "company_name": customer_data.get("shop_name", ""),
        "contact_type": "customer",
        "customer_sub_type": "business",
        "payment_terms_label": customer_data.get("payment_terms", ""),
    }

    # Add contact person
    if customer_data.get("customer_name") or customer_data.get("customer_mail_id"):
        contact_persons = [{
            "first_name": customer_data.get("customer_name", ""),
            "email": customer_data.get("customer_mail_id", ""),
            "phone": customer_data.get("whatsapp_no", ""),
            "is_primary_contact": True
        }]
        contact_payload["contact_persons"] = contact_persons

    # Add billing address
    billing_addr = customer_data.get("billing_address")
    if billing_addr:
        # Handle both dict (structured) and string (legacy) formats
        if isinstance(billing_addr, dict):
            # Use structured address with all fields
            billing_address = {
                k: v for k, v in billing_addr.items()
                if v is not None and v != ""
            }
            # Ensure country defaults to India if not provided
            if "country" not in billing_address:
                billing_address["country"] = "India"

            # Add state_code if state is provided (keep state as full name)
            if "state" in billing_address and billing_address["state"]:
                state_code = get_state_code(billing_address["state"])
                billing_address["state_code"] = state_code
                # Keep state as full name, don't change it

            contact_payload["billing_address"] = billing_address
        else:
            # Legacy string format - convert to structured
            state_name = customer_data.get("place_of_supply", "")
            state_code = get_state_code(state_name)
            billing_address = {
                "attention": customer_data.get("customer_name", ""),
                "address": str(billing_addr),
                "city": customer_data.get("place_of_supply", ""),
                "state": state_name,  # Keep full name
                "state_code": state_code,  # Two-letter code
                "zip": customer_data.get("pincode", ""),
                "country": "India"
            }
            contact_payload["billing_address"] = billing_address

    # Add shipping address and capture state_code for place_of_supply
    shipping_addr = customer_data.get("shipping_address")
    shipping_state_code = None  # Will be used for place_of_supply

    if shipping_addr:
        # Handle both dict (structured) and string (legacy) formats
        if isinstance(shipping_addr, dict):
            # Use structured address with all fields
            shipping_address = {
                k: v for k, v in shipping_addr.items()
                if v is not None and v != ""
            }
            # Ensure country defaults to India if not provided
            if "country" not in shipping_address:
                shipping_address["country"] = "India"

            # Add state_code if state is provided (keep state as full name)
            if "state" in shipping_address and shipping_address["state"]:
                shipping_state_code = get_state_code(shipping_address["state"])
                shipping_address["state_code"] = shipping_state_code
                # Keep state as full name, don't change it

            contact_payload["shipping_address"] = shipping_address
        else:
            # Legacy string format - convert to structured
            state_name = customer_data.get("place_of_supply", "")
            shipping_state_code = get_state_code(state_name)
            shipping_address = {
                "attention": customer_data.get("customer_name", ""),
                "address": str(shipping_addr),
                "city": customer_data.get("place_of_supply", ""),
                "state": state_name,  # Keep full name
                "state_code": shipping_state_code,  # Two-letter code
                "zip": customer_data.get("pincode", ""),
                "country": "India"
            }
            contact_payload["shipping_address"] = shipping_address

    # Add standard Zoho fields (no custom fields to avoid errors)

    if customer_data.get("gst_no"):
        contact_payload["gst_no"] = customer_data.get("gst_no")

    if customer_data.get("gst_treatment"):
        # Map GST treatment to Zoho format
        gst_treatment_map = {
            "Business GST": "business_gst",
            "Unregistered Business": "unregistered_business",
            "Consumer": "consumer"
        }
        zoho_gst_treatment = gst_treatment_map.get(customer_data.get("gst_treatment"), "business_gst")
        contact_payload["gst_treatment"] = zoho_gst_treatment

    # Use shipping address state_code for place_of_supply (Zoho expects state code, not full name)
    if shipping_state_code:
        contact_payload["place_of_contact"] = shipping_state_code
    elif customer_data.get("place_of_supply"):
        # Fallback: convert place_of_supply to state code
        contact_payload["place_of_contact"] = get_state_code(customer_data.get("place_of_supply"))

    # Add notes with additional details
    notes_parts = []
    if customer_data.get("pan_card_no"):
        notes_parts.append(f"PAN Number: {customer_data.get('pan_card_no')}")
    if customer_data.get("margin_details"):
        notes_parts.append(f"Margin Details: {customer_data.get('margin_details')}")
    if customer_data.get("sales_person"):
        notes_parts.append(f"Sales Person: {customer_data.get('sales_person')}")

    if notes_parts:
        contact_payload["notes"] = "\n".join(notes_parts)

    try:
        logger.info(f"Creating Zoho contact for: {customer_data.get('shop_name')}")
        logger.info(f"Zoho Contact Payload: {contact_payload}")

        response = requests.post(url, json=contact_payload, headers=headers, timeout=30)

        if response.status_code == 201:
            response_data = response.json()
            contact = response_data.get("contact", {})
            contact_id = contact.get("contact_id")

            logger.info(f"Successfully created Zoho contact with ID: {contact_id}")

            return {
                "success": True,
                "contact_id": contact_id,
                "message": "Customer created successfully in Zoho Books"
            }
        else:
            error_message = response.text
            logger.error(f"Failed to create Zoho contact: {response.status_code} - {error_message}")

            return {
                "success": False,
                "message": f"Zoho API error: {error_message}"
            }

    except Exception as e:
        logger.error(f"Exception while creating Zoho contact: {e}")
        return {
            "success": False,
            "message": f"Error creating customer in Zoho: {str(e)}"
        }

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
            "billing_address": request_data.billing_address.model_dump() if request_data.billing_address else None,
            "shipping_address": request_data.shipping_address.model_dump() if request_data.shipping_address else None,
            "place_of_supply": request_data.place_of_supply,
            "customer_mail_id": request_data.customer_mail_id,
            "gst_treatment": request_data.gst_treatment,
            "pincode": request_data.pincode,
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
        valid_statuses = ["pending", "approved", "rejected", "admin_commented", "salesperson_replied", "created_on_zoho"]
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")

        # Get the request details
        request_doc = db.customer_creation_requests.find_one({"_id": ObjectId(request_id)})
        if not request_doc:
            raise HTTPException(status_code=404, detail="Request not found")

        # Check if already created on Zoho - prevent further status changes
        if request_doc.get("status") == "created_on_zoho":
            raise HTTPException(
                status_code=400,
                detail="Cannot modify request that has been created in Zoho Books"
            )

        # If status is being set to "approved", create customer in Zoho
        zoho_contact_id = None
        final_status = status

        if status == "approved":
            logger.info(f"Creating customer in Zoho for request: {request_id}")

            # Create customer in Zoho
            zoho_result = create_zoho_contact(request_doc)

            if zoho_result.get("success"):
                zoho_contact_id = zoho_result.get("contact_id")
                final_status = "created_on_zoho"
                logger.info(f"Customer created in Zoho with contact_id: {zoho_contact_id}")
            else:
                # If Zoho creation fails, log the error but still approve the request
                error_msg = zoho_result.get("message", "Unknown error")
                logger.error(f"Failed to create customer in Zoho: {error_msg}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create customer in Zoho Books: {error_msg}"
                )

        # Prepare update document
        update_doc = {
            "status": final_status,
            "updated_at": datetime.now(),
            "updated_by": user_id
        }

        # Add Zoho contact ID if available
        if zoho_contact_id:
            update_doc["zoho_contact_id"] = zoho_contact_id

        # Update the request
        result = db.customer_creation_requests.update_one(
            {"_id": ObjectId(request_id)},
            {"$set": update_doc}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Request not found or no changes made")

        response_message = "Request status updated successfully"
        if final_status == "created_on_zoho":
            response_message = f"Customer created in Zoho Books successfully (Contact ID: {zoho_contact_id})"

        return {
            "message": response_message,
            "status": final_status,
            "zoho_contact_id": zoho_contact_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating request status: {e}")
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

        # Send WhatsApp notification to salesperson
        try:
            # Get the customer request to find the salesperson
            customer_request = db.customer_creation_requests.find_one({"_id": ObjectId(request_id)})
            if customer_request:
                # Get salesperson details
                salesperson = db.users.find_one({"_id": customer_request.get("created_by")})
                if salesperson:
                    # Get WhatsApp template
                    template = db.templates.find_one({"name": "admin_comment_customer_creation_request"})
                    if template and salesperson.get("phone"):
                        # Prepare parameters for template
                        params = {
                            "admin_name": admin_name,
                            "sales_person_name": f"{salesperson.get('first_name', '')} {salesperson.get('last_name', '')}".strip() or salesperson.get('email') or 'Salesperson',
                            "button_url": request_id
                        }

                        # Send WhatsApp message
                        send_whatsapp(salesperson.get("phone"), template, params)
                        print(f"WhatsApp notification sent to salesperson: {salesperson.get('email')}")
        except Exception as e:
            print(f"Failed to send WhatsApp notification: {e}")

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

        # Send WhatsApp notification to admin
        try:
            # Get the customer request to find the comment and admin
            customer_request = db.customer_creation_requests.find_one({"_id": ObjectId(request_id)})
            if customer_request and customer_request.get("admin_comments"):
                # Find the specific comment to get admin details
                target_comment = None
                for comment in customer_request.get("admin_comments", []):
                    if comment.get("_id") == comment_id:
                        target_comment = comment
                        break

                if target_comment:
                    # Get admin details
                    admin = db.users.find_one({"_id": ObjectId(target_comment.get("admin_id"))})
                    if admin:
                        # Get WhatsApp template
                        template = db.templates.find_one({"name": "sp_reply_comment_customer_creation_request"})
                        if template and admin.get("phone"):
                            # Prepare parameters for template
                            params = {
                                "sales_person_name": reply_data.user_name,
                                "admin_name": f"{admin.get('first_name', '')} {admin.get('last_name', '')}".strip() or admin.get('email') or 'Admin',
                                "button_url": request_id
                            }

                            # Send WhatsApp message
                            send_whatsapp(admin.get("phone"), template, params)
                            print(f"WhatsApp notification sent to admin: {admin.get('email')}")
        except Exception as e:
            print(f"Failed to send WhatsApp notification: {e}")

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

        # Check if already created on Zoho - prevent editing
        if existing_request.get("status") == "created_on_zoho":
            raise HTTPException(
                status_code=400,
                detail="Cannot edit request that has been created in Zoho Books"
            )

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
            "billing_address": request_data.billing_address.model_dump() if request_data.billing_address else None,
            "shipping_address": request_data.shipping_address.model_dump() if request_data.shipping_address else None,
            "place_of_supply": request_data.place_of_supply,
            "customer_mail_id": request_data.customer_mail_id,
            "gst_treatment": request_data.gst_treatment,
            "pincode": request_data.pincode,
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
