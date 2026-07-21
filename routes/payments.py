"""
Razorpay payment-gateway integration (basic flow).

Uses the Razorpay Payment Links REST API directly via `requests` with HTTP Basic
Auth (key + secret) — no extra SDK dependency required. Test credentials are read
from the environment (RAZORPAY_API_TEST_KEY / RAZORPAY_API_TEST_SECRET).

Flow:
  1. POST /payments/order/{order_id}/payment_link
     -> creates a Razorpay payment link for the order's `total_amount`,
        stores it on the order document, and returns the short URL.
  2. GET  /payments/order/{order_id}/payment_link
     -> returns the stored link and refreshes its live status from Razorpay.
  3. POST /payments/webhook
     -> receives Razorpay webhook events. On a successful payment it creates AND
        creates the order's Zoho estimate in DRAFT (via orders.finalise). On failure /
        cancellation / expiry it records the event and does NOT create an estimate.

Every request sent to and response received from Razorpay (and every webhook
payload) is persisted to the `razorpay_transactions` collection for auditing.
"""

import os
import re
import hmac
import hashlib
import requests
import httpx
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from bson.objectid import ObjectId
from dotenv import load_dotenv

from ..config.root import get_database, serialize_mongo_document
from ..config.whatsapp import send_whatsapp
from .notifications import create_notification

load_dotenv()

RAZORPAY_KEY = os.getenv("RAZORPAY_API_TEST_KEY")
RAZORPAY_SECRET = os.getenv("RAZORPAY_API_TEST_SECRET")
# Set this to the same secret configured for the webhook in the Razorpay dashboard.
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_TEST_SECRET")

RAZORPAY_BASE_URL = "https://api.razorpay.com/v1"

ZOHO_BOOKS_BASE = "https://books.zoho.com/api/v3"
ZOHO_ORG_ID = os.getenv("ORG_ID")

# Razorpay payment `method` -> Zoho Books customer-payment `payment_mode`.
RAZORPAY_METHOD_TO_ZOHO_MODE = {
    "card": "creditcard",
    "emi": "creditcard",
    "netbanking": "banktransfer",
    "upi": "upi",
    "wallet": "others",
    "bank_transfer": "banktransfer",
}

# Razorpay payment-link statuses that mean money was (fully) collected.
PAID_STATUSES = {"paid"}
# Statuses that mean the customer will NOT pay this link -> never create an estimate.
FAILED_STATUSES = {"cancelled", "expired"}

db = get_database()
orders_collection = db["orders"]
customers_collection = db["customers"]
# Audit log of everything exchanged with Razorpay.
razorpay_transactions = db["razorpay_transactions"]

router = APIRouter()


def _log_transaction(action: str, order_id, **fields):
    """Persist a Razorpay interaction (request/response/webhook) for auditing."""
    try:
        # Store order_id as an ObjectId so it joins against the orders collection;
        # fall back to the raw value if it isn't a valid ObjectId.
        if order_id and ObjectId.is_valid(order_id):
            order_ref = ObjectId(order_id)
        else:
            order_ref = order_id or None
        doc = {
            "action": action,
            "order_id": order_ref,
            "created_at": datetime.now(),
        }
        doc.update(fields)
        razorpay_transactions.insert_one(doc)
    except Exception as e:  # never let logging break the payment flow
        print(f"[razorpay] failed to log transaction ({action}): {e}")


def _auth():
    """HTTP Basic Auth tuple for the Razorpay API."""
    if not RAZORPAY_KEY or not RAZORPAY_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Razorpay credentials are not configured on the server",
        )
    return (RAZORPAY_KEY, RAZORPAY_SECRET)


def _get_order_or_404(order_id: str) -> dict:
    if not ObjectId.is_valid(order_id):
        raise HTTPException(status_code=400, detail="Invalid order id")
    order = orders_collection.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


def _is_self_registered_order(order: dict) -> bool:
    """True if this order belongs to a self-registered B2B customer — checked via
    the order creator's user doc, then via the linked customer's user account."""
    # 1. Created by a self-registered user (the customer placing their own order).
    created_by = order.get("created_by")
    if created_by:
        try:
            u = db.users.find_one({"_id": ObjectId(created_by)}, {"self_registered": 1})
            if u and u.get("self_registered"):
                return True
        except Exception:
            pass
    # 2. The order's customer is linked to a self-registered user account.
    customer_id = order.get("customer_id")
    if customer_id:
        try:
            cust = customers_collection.find_one({"_id": ObjectId(customer_id)}, {"contact_id": 1})
            contact_id = cust.get("contact_id") if cust else None
            if contact_id and db.users.find_one(
                {"customer_id": contact_id, "self_registered": True}, {"_id": 1}
            ):
                return True
        except Exception:
            pass
    return False


def _customer_contact(order: dict) -> dict:
    """Best-effort prefill of Razorpay's `customer` block from the order's customer."""
    contact = {}
    customer_id = order.get("customer_id")
    if not customer_id:
        return contact
    try:
        customer = customers_collection.find_one({"_id": ObjectId(customer_id)}) or {}
    except Exception:
        return contact

    name = customer.get("contact_name") or customer.get("company_name")
    if name:
        contact["name"] = str(name)[:50]

    person = (customer.get("contact_persons") or [{}])[0] if customer.get("contact_persons") else {}
    email = customer.get("email") or person.get("email")
    phone = customer.get("mobile") or customer.get("phone") or person.get("mobile") or person.get("phone")
    if email:
        contact["email"] = str(email)
    if phone:
        contact["contact"] = str(phone)
    return contact


def _notify_customer_payment_success(order: dict):
    """Send the customer a WhatsApp 'order confirmation' after a successful payment,
    plus an in-app notification. Never raises — payment must never fail because a
    notification did. Skips quietly if the template hasn't been created yet.

    Idempotent: both the Checkout verify endpoint and the payment-link webhook can
    fire for the same payment, so we atomically claim the notification first and
    only the first caller actually sends."""
    try:
        claim = orders_collection.update_one(
            {"_id": order["_id"], "payment.customer_notified": {"$ne": True}},
            {"$set": {"payment.customer_notified": True}},
        )
        if claim.modified_count == 0:
            return  # already notified by the other path
        # Re-fetch so we use the estimate_number set during estimate creation
        # (this runs after the estimate exists), not the raw order id.
        order = orders_collection.find_one({"_id": order["_id"]}) or order
        contact = _customer_contact(order)
        phone = contact.get("contact")
        customer_name = contact.get("name") or "Customer"
        order_ref = order.get("estimate_number") or f"#{str(order.get('_id'))[-6:]}"
        try:
            amount = float(order.get("total_amount") or 0)
        except (TypeError, ValueError):
            amount = 0
        amount_str = f"₹{amount:,.0f}"

        template_doc = db.templates.find_one({"name": "order_confirmation"})
        if template_doc and phone:
            template = serialize_mongo_document(dict(template_doc))
            # Approved template has exactly 2 body vars: {{1}}=name, {{2}}=order.
            params = {
                "customer_name": customer_name,
                "order_number": order_ref,
            }
            send_whatsapp(phone, {**template}, {**params})
        elif not template_doc:
            print("Template 'order_confirmation' not found, skipping customer WhatsApp")

        # In-app notification for the customer who placed the order.
        created_by = order.get("created_by")
        if created_by:
            create_notification(
                db,
                str(created_by),
                "order_confirmation",
                f"Payment received for order {order_ref}",
                f"We've received your payment of {amount_str}. Your order {order_ref} is confirmed.",
                f"/orders/past/{order.get('_id')}",
            )
    except Exception as e:
        print(f"[payments] failed to notify customer of payment success: {e}")


@router.post("/order/{order_id}/payment_link")
def create_payment_link(order_id: str):
    """
    Create a Razorpay payment link for the order's total amount and persist it.
    """
    order = _get_order_or_404(order_id)

    total_amount = order.get("total_amount")
    try:
        total_amount = float(total_amount)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Order total amount is missing or invalid")
    if total_amount <= 0:
        raise HTTPException(status_code=400, detail="Order total amount must be greater than zero")

    # Reuse an existing link instead of creating a duplicate — Razorpay enforces
    # a unique reference_id, and we don't want multiple live links per order.
    existing = order.get("payment") or {}
    existing_status = existing.get("status")
    if existing.get("short_url"):
        if existing_status == "paid":
            raise HTTPException(status_code=400, detail="This order has already been paid")
        if existing_status not in FAILED_STATUSES:
            return {
                "order_id": str(order_id),
                "payment_link_id": existing.get("payment_link_id"),
                "short_url": existing.get("short_url"),
                "amount": existing.get("amount"),
                "currency": existing.get("currency", "INR"),
                "status": existing_status,
                "reused": True,
            }

    # Razorpay expects the amount in the smallest currency unit (paise for INR).
    amount_paise = int(round(total_amount * 100))

    payload = {
        "amount": amount_paise,
        "currency": "INR",
        "accept_partial": False,
        "description": f"Payment for order {order_id}",
        # reference_id must be unique per link; tag the real order id in notes
        # so the webhook can always map the payment back to the order.
        "reference_id": f"{order_id}-{int(datetime.now().timestamp())}",
        "reminder_enable": True,
        "notes": {"order_id": str(order_id)},
    }

    customer = _customer_contact(order)
    if customer:
        payload["customer"] = customer
        payload["notify"] = {
            "sms": bool(customer.get("contact")),
            "email": bool(customer.get("email")),
        }

    _log_transaction("create_payment_link_request", order_id, request=payload)

    try:
        resp = requests.post(
            f"{RAZORPAY_BASE_URL}/payment_links",
            json=payload,
            auth=_auth(),
            timeout=30,
        )
    except requests.RequestException as e:
        _log_transaction("create_payment_link_error", order_id, error=str(e))
        raise HTTPException(status_code=502, detail=f"Failed to reach Razorpay: {e}")

    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}

    _log_transaction(
        "create_payment_link_response",
        order_id,
        status_code=resp.status_code,
        response=data,
    )

    if resp.status_code not in (200, 201):
        raise HTTPException(
            status_code=502,
            detail=f"Razorpay error ({resp.status_code}): {resp.text}",
        )

    orders_collection.update_one(
        {"_id": order["_id"]},
        {
            "$set": {
                "payment": {
                    "provider": "razorpay",
                    "payment_link_id": data.get("id"),
                    "short_url": data.get("short_url"),
                    "amount": amount_paise,
                    "currency": "INR",
                    "status": data.get("status"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            }
        },
    )

    return {
        "order_id": str(order_id),
        "payment_link_id": data.get("id"),
        "short_url": data.get("short_url"),
        "amount": amount_paise,
        "currency": "INR",
        "status": data.get("status"),
    }


@router.get("/order/{order_id}/payment_link")
def get_payment_link(order_id: str):
    """
    Return the stored payment link for an order, refreshing its live status
    from Razorpay so the caller sees `paid` / `cancelled` etc.
    """
    order = _get_order_or_404(order_id)
    payment = order.get("payment") or {}
    link_id = payment.get("payment_link_id")
    if not link_id:
        raise HTTPException(status_code=404, detail="No payment link found for this order")

    try:
        resp = requests.get(
            f"{RAZORPAY_BASE_URL}/payment_links/{link_id}",
            auth=_auth(),
            timeout=30,
        )
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Failed to reach Razorpay: {e}")

    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}

    _log_transaction(
        "get_payment_link_response",
        order_id,
        status_code=resp.status_code,
        response=data,
    )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Razorpay error ({resp.status_code}): {resp.text}",
        )

    status = data.get("status")
    orders_collection.update_one(
        {"_id": order["_id"]},
        {"$set": {"payment.status": status, "payment.updated_at": datetime.now()}},
    )

    return {
        "order_id": str(order_id),
        "payment_link_id": link_id,
        "short_url": data.get("short_url"),
        "amount": data.get("amount"),
        "currency": data.get("currency"),
        "status": status,
        "amount_paid": data.get("amount_paid"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Razorpay Orders + Checkout (in-page popup) flow
#
#   1. POST /payments/order/{order_id}/checkout
#        -> creates a Razorpay ORDER and returns the params the frontend needs to
#           open Razorpay Checkout (key_id, razorpay_order_id, amount, prefill…).
#   2. Frontend opens Checkout. On success Razorpay hands back
#      {razorpay_order_id, razorpay_payment_id, razorpay_signature}.
#   3. POST /payments/verify
#        -> verifies the signature, and on success creates the estimate in DRAFT
#           and marks the order paid. Returns {success: true/false} synchronously
#           so the page can show a success/failure animation.
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/order/{order_id}/config")
def order_payment_config(order_id: str):
    """Tell the Marketplace whether this order requires online payment (self-registered
    customer) and the minimum cart value before payment is allowed."""
    from .app_settings import get_min_order_value_self_registered

    order = _get_order_or_404(order_id)
    return {
        "is_self_registered": _is_self_registered_order(order),
        "min_order_value": get_min_order_value_self_registered(),
    }


@router.post("/order/{order_id}/cod")
async def cod_order(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Cash / Cheque on delivery: instead of paying online, the (self-registered)
    customer places the order and pays on delivery. Creates the Zoho estimate
    in DRAFT status (same as a salesperson submit) and tags the order's payment
    as COD. No sales order / invoice / customer payment is created — that
    happens through the normal back-office flow once the money is collected.
    """
    from .orders import finalise  # lazy import to avoid circular import
    from .app_settings import get_min_order_value_self_registered

    order = _get_order_or_404(order_id)

    # COD (like Pay Now) is exclusively for self-registered customers — everyone
    # else goes through the normal salesperson submit flow.
    if not _is_self_registered_order(order):
        raise HTTPException(
            status_code=403,
            detail="Cash/Cheque on delivery is only available for self-registered customers",
        )

    if (order.get("payment") or {}).get("status") == "paid":
        raise HTTPException(status_code=400, detail="This order has already been paid")

    total_amount = order.get("total_amount")
    try:
        total_amount = float(total_amount)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Order total amount is missing or invalid")
    if total_amount <= 0:
        raise HTTPException(status_code=400, detail="Order total amount must be greater than zero")

    # Same minimum-order rule as online payment (anti-bypass).
    min_value = get_min_order_value_self_registered()
    if min_value and total_amount < min_value:
        raise HTTPException(
            status_code=400,
            detail=f"A minimum order of ₹{int(min_value):,} is required to place an order.",
        )

    result = await finalise(
        {
            "order_id": str(order_id),
            "status": "draft",
            "create_stock": True,
            "create_pre_order": True,
            "extra_notes": "Cash/Cheque on Delivery — Payment Terms: Due on Receipt.",
        },
        request,
        background_tasks,
    )
    if result.get("status") != "success":
        raise HTTPException(status_code=502, detail=result.get("message", "Failed to place the order"))

    orders_collection.update_one(
        {"_id": order["_id"]},
        {
            "$set": {
                "payment.method": "cash_on_delivery",
                "payment.status": "cod",
                "payment.terms": "Due on Receipt",
                "payment.updated_at": datetime.now(),
            }
        },
    )
    _log_transaction("cod_order_placed", order_id)

    return {"success": True, "message": result.get("message", ""), "payment_status": "cod"}


@router.post("/order/{order_id}/checkout")
def create_checkout_order(order_id: str):
    """
    Create a Razorpay Order for the order total and return Checkout params.
    """
    from .app_settings import get_min_order_value_self_registered

    order = _get_order_or_404(order_id)

    if (order.get("payment") or {}).get("status") == "paid":
        raise HTTPException(status_code=400, detail="This order has already been paid")

    total_amount = order.get("total_amount")
    try:
        total_amount = float(total_amount)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Order total amount is missing or invalid")
    if total_amount <= 0:
        raise HTTPException(status_code=400, detail="Order total amount must be greater than zero")

    # Enforce the minimum order value for self-registered customers (anti-bypass).
    if _is_self_registered_order(order):
        min_value = get_min_order_value_self_registered()
        if min_value and total_amount < min_value:
            raise HTTPException(
                status_code=400,
                detail=f"A minimum order of ₹{int(min_value):,} is required before payment.",
            )

    amount_paise = int(round(total_amount * 100))

    payload = {
        "amount": amount_paise,
        "currency": "INR",
        "receipt": str(order_id),
        "notes": {"order_id": str(order_id)},
        # Capture automatically on authorization — otherwise the money sits in
        # "authorized" until someone captures it manually in the dashboard.
        "payment_capture": 1,
    }

    _log_transaction("create_order_request", order_id, request=payload)

    try:
        resp = requests.post(
            f"{RAZORPAY_BASE_URL}/orders",
            json=payload,
            auth=_auth(),
            timeout=30,
        )
    except requests.RequestException as e:
        _log_transaction("create_order_error", order_id, error=str(e))
        raise HTTPException(status_code=502, detail=f"Failed to reach Razorpay: {e}")

    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}

    _log_transaction("create_order_response", order_id, status_code=resp.status_code, response=data)

    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=502, detail=f"Razorpay error ({resp.status_code}): {resp.text}")

    prefill = {}
    contact = _customer_contact(order)
    if contact.get("name"):
        prefill["name"] = contact["name"]
    if contact.get("email"):
        prefill["email"] = contact["email"]
    if contact.get("contact"):
        prefill["contact"] = contact["contact"]

    orders_collection.update_one(
        {"_id": order["_id"]},
        {
            "$set": {
                "payment": {
                    "provider": "razorpay",
                    "razorpay_order_id": data.get("id"),
                    "amount": amount_paise,
                    "currency": "INR",
                    "status": data.get("status", "created"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            }
        },
    )

    return {
        "key_id": RAZORPAY_KEY,
        "razorpay_order_id": data.get("id"),
        "amount": amount_paise,
        "currency": "INR",
        "name": "Pupscribe",
        "description": f"Order {order_id}",
        "prefill": prefill,
        "notes": {"order_id": str(order_id)},
    }


@router.post("/verify")
async def verify_payment(body: dict, request: Request, background_tasks: BackgroundTasks):
    """
    Verify a Razorpay Checkout payment signature and, on success, create (draft)
    the order's estimate. Returns {success: bool} synchronously for the popup.

    Expected body: order_id, razorpay_order_id, razorpay_payment_id, razorpay_signature
    """
    order_id = body.get("order_id")
    rzp_order_id = body.get("razorpay_order_id")
    rzp_payment_id = body.get("razorpay_payment_id")
    rzp_signature = body.get("razorpay_signature")

    _log_transaction(
        "verify_request",
        order_id,
        razorpay_order_id=rzp_order_id,
        razorpay_payment_id=rzp_payment_id,
    )

    if not all([order_id, rzp_order_id, rzp_payment_id, rzp_signature]):
        raise HTTPException(status_code=400, detail="Missing payment verification fields")

    order = _get_order_or_404(order_id)

    # Razorpay checkout signature = HMAC_SHA256(order_id + "|" + payment_id, secret)
    expected = hmac.new(
        (RAZORPAY_SECRET or "").encode(),
        f"{rzp_order_id}|{rzp_payment_id}".encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, rzp_signature):
        _log_transaction("verify_failed", order_id, reason="signature_mismatch")
        orders_collection.update_one(
            {"_id": order["_id"]},
            {"$set": {"payment.status": "failed", "payment.updated_at": datetime.now()}},
        )
        return {"success": False, "detail": "Payment signature verification failed"}

    # Signature OK -> payment captured.
    orders_collection.update_one(
        {"_id": order["_id"]},
        {
            "$set": {
                "payment.status": "paid",
                "payment.razorpay_payment_id": rzp_payment_id,
                "payment.razorpay_order_id": rzp_order_id,
                "payment.updated_at": datetime.now(),
            }
        },
    )
    _log_transaction("verify_success", order_id, razorpay_payment_id=rzp_payment_id)

    # Kick off DRAFT estimate creation in the BACKGROUND so this endpoint returns
    # immediately (signature check is instant; the Zoho estimate call can take
    # tens of seconds). The frontend shows a loader and polls
    # GET /payments/order/{id}/status until the estimate is created.
    already_created = (
        order.get("estimate_created")
        or str(order.get("status", "")).lower() == "accepted"
    )
    # Self-registered orders always run the (idempotent) post-payment chain —
    # even when the estimate already exists it must be accepted and the sales
    # order / invoice / customer payment created if any step is still missing.
    if not already_created or _is_self_registered_order(order):
        # The customer confirmation is sent from the background task once the
        # estimate exists, so the message carries the estimate number.
        background_tasks.add_task(
            _safe_create_draft_estimate, order_id, request, background_tasks
        )
    else:
        # Estimate already exists -> confirm now (order already has its number).
        _notify_customer_payment_success(order)

    return {
        "success": True,
        "order_id": str(order_id),
        "payment_id": rzp_payment_id,
        "payment_status": "paid",
        "estimate_created": bool(already_created),
        "estimate_pending": not already_created,
    }


async def _safe_create_draft_estimate(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """Background wrapper around estimate creation — never raises (logged instead).
    Sends the customer confirmation afterwards (in `finally`) so the message uses
    the freshly-created estimate number; falls back to the order id if creation
    failed (payment already succeeded, so the customer must still be confirmed)."""
    try:
        result = await _create_draft_estimate_on_payment(order_id, request, background_tasks)
        _log_transaction("verify_estimate_accepted", order_id, result=result)
    except Exception as e:
        _log_transaction("verify_estimate_error", order_id, error=str(e))
        print(f"[razorpay] background estimate creation failed for order {order_id}: {e}")
    finally:
        try:
            order = orders_collection.find_one({"_id": ObjectId(order_id)})
            if order:
                _notify_customer_payment_success(order)
        except Exception as e:
            print(f"[razorpay] failed to send post-estimate confirmation for {order_id}: {e}")


@router.get("/order/{order_id}/status")
def order_payment_status(order_id: str):
    """
    Poll target for the Checkout popup. Reports the live payment + estimate state
    so the frontend can show a loader until the (backgrounded) estimate is ready.
    Estimate status is read from the `estimates` collection — the point of truth,
    kept current by the Zoho webhook.
    """
    order = _get_order_or_404(order_id)
    payment_status = (order.get("payment") or {}).get("status", "")
    estimate_created = bool(order.get("estimate_created") or order.get("pre_order_estimate_created"))
    est_number = order.get("estimate_number", "") or order.get("pre_order_estimate_number", "")

    estimate_status = ""
    if est_number:
        est = db.estimates.find_one({"estimate_number": est_number}, {"status": 1})
        if est:
            estimate_status = est.get("status", "")

    flow = order.get("zoho_flow") or {}

    if payment_status == "paid" and _is_self_registered_order(order):
        # Self-registered paid orders run the full Zoho chain (accepted estimate
        # -> sales order -> invoice -> customer payment). Only report done once
        # the chain finished (or definitively failed) so the confirmation popup
        # shows the final state — not the transient draft estimate.
        done = bool(flow.get("chain_completed_at")) or bool(flow.get("last_error"))
    else:
        # Resolved once payment is settled AND (for paid orders) the estimate exists.
        done = (payment_status == "paid" and estimate_created) or payment_status == "failed"

    return {
        "order_id": str(order_id),
        "payment_status": payment_status,
        "estimate_created": estimate_created,
        "estimate_number": est_number,
        "estimate_status": estimate_status,
        "salesorder_number": flow.get("salesorder_number", ""),
        "invoice_number": flow.get("invoice_number", ""),
        "customerpayment_number": flow.get("customerpayment_number", ""),
        "chain_completed": bool(flow.get("chain_completed_at")),
        "done": done,
    }


async def _create_draft_estimate_on_payment(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Payment succeeded -> finalise the order's Zoho estimate.

    Self-registered customers: the estimate is ACCEPTED and the full post-payment
    Zoho chain runs (draft sales order -> invoice marked sent -> customer payment
    applied, which marks the invoice paid).

    Everyone else: the estimate is created in DRAFT status as before (the
    accept/decline status push only runs for 'accepted'/'declined').
    Imported lazily to avoid an import cycle.
    """
    from .orders import finalise  # lazy import to avoid circular import

    order = orders_collection.find_one({"_id": ObjectId(order_id)})
    if order is not None and _is_self_registered_order(order):
        return await _run_post_payment_zoho_chain(order_id, request, background_tasks)

    return await finalise(
        {
            "order_id": str(order_id),
            "status": "draft",
            "create_stock": True,
            "create_pre_order": True,
        },
        request,
        background_tasks,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Post-payment Zoho chain (self-registered customers only)
#
# After a successful online payment:
#   1. Estimate  -> accepted (created first if it doesn't exist yet)
#   2. Sales order -> created as DRAFT from the estimate's line items
#   3. Invoice   -> created from the same line items, then marked SENT
#   4. Customer payment -> recorded with the Razorpay payment mode and applied
#      to the invoice, which flips the invoice to PAID in Zoho
#
# Every step is idempotent (guarded by ids persisted under order.zoho_flow) so
# the verify endpoint and the webhook can both trigger it safely; a coarse
# `chain_running` claim prevents the two from racing each other.
# ─────────────────────────────────────────────────────────────────────────────

# Line-item fields carried from the estimate onto the sales order / invoice.
_CHAIN_LINE_ITEM_KEYS = (
    "item_id",
    "name",
    "description",
    "rate",
    "quantity",
    "discount",
    "tax_id",
    "hsn_or_sac",
    "unit",
)


def _notify_customer_order_accepted(order: dict):
    """Customer-facing 'order accepted' WhatsApp + in-app notification, sent at
    most ONCE per order (atomic claim on lifecycle_notified.accepted). The paid
    chain calls this AFTER the payment confirmation so the messages arrive in
    the right order; the Zoho estimate webhook shares the same claim, so
    repeated webhook fires can never re-send it. Never raises."""
    try:
        claimed = orders_collection.find_one_and_update(
            {"_id": order["_id"], "lifecycle_notified.accepted": {"$ne": True}},
            {"$set": {"lifecycle_notified.accepted": True}},
        )
        if claimed is None:
            return
        customer = (
            customers_collection.find_one({"_id": ObjectId(order.get("customer_id"))})
            if order.get("customer_id")
            else None
        )
        contact_id = str(customer.get("contact_id", "")) if customer else ""
        est_number = order.get("estimate_number") or order.get("pre_order_estimate_number", "")
        from .webhooks import notify_customer_whatsapp  # lazy import, avoids cycle

        notify_customer_whatsapp(
            contact_id,
            "order_accepted",
            {
                "customer_name": order.get("customer_name", "Customer"),
                "estimate_number": est_number,
            },
            notif={
                "type": "order_accepted",
                "title": f"Order accepted — {est_number}",
                "body": f"Your order {est_number} has been accepted and is being processed.",
                "link": "/customer/orders",
            },
        )
    except Exception as e:
        print(f"[payments] failed to send order-accepted notification: {e}")


def _notify_admins_chain_completed(order: dict, flow: dict):
    """In-app notification to admins when the full post-payment Zoho chain has
    completed, carrying the created document identifiers. Never raises."""
    # Single recipient for now — will widen to all admins later.
    admin_emails = ["rkoalsi2000@gmail.com"]
    try:
        est_number = order.get("estimate_number") or order.get("pre_order_estimate_number", "")
        body = (
            f"Estimate {est_number} · Sales Order {flow.get('salesorder_number', '')} · "
            f"Invoice {flow.get('invoice_number', '')} · "
            f"Customer Payment {flow.get('customerpayment_number', '')} "
            f"(payment_id {flow.get('customerpayment_id', '')}) — "
            f"{order.get('customer_name', '')}, ₹{order.get('total_amount', '')}."
        )
        for email in admin_emails:
            admin = db.users.find_one({"email": email}, {"_id": 1})
            if admin:
                create_notification(
                    db,
                    str(admin["_id"]),
                    "payment_chain_completed",
                    f"Online payment processed — {est_number}",
                    body,
                    "/admin/orders",
                )
    except Exception as e:
        print(f"[payments] failed to notify admins of chain completion: {e}")


def _set_zoho_flow(order_oid, fields: dict):
    """Persist chain progress under order.zoho_flow.*"""
    orders_collection.update_one(
        {"_id": order_oid},
        {"$set": {f"zoho_flow.{k}": v for k, v in fields.items()}},
    )


async def _next_zoho_number(
    client: httpx.AsyncClient,
    headers: dict,
    module: str,
    list_key: str,
    number_field: str,
    prefix: str,
):
    """
    Reserve the next {prefix}/FY/NNNN document number for the current financial
    year, mirroring the estimate-numbering pattern in orders.finalise: fetch the
    latest numbers from Zoho, seed a Mongo counter with the highest FY sequence
    ($max), then atomically $inc it.

    Only numbers in the given prefix's series are considered — the books contain
    other series and stray manually-numbered documents (e.g. "TN-CP/26-27/0037",
    "WB-Oct-2023-24I", "Whoof-Whoof Dummy 02") that must never seed the sequence.

    Returns None when the sequence can't be derived — the caller then falls back
    to Zoho's auto-generated numbering.
    """
    try:
        now = datetime.now()
        fy_start = now.year if now.month >= 4 else now.year - 1
        fy_str = f"{str(fy_start)[-2:]}-{str(fy_start + 1)[-2:]}"
        # Narrow to this prefix+FY series server-side where Zoho supports the
        # *_startswith filter; harmless (ignored) where it doesn't. Without it
        # the plain "CP/" series can be buried past page 1 behind "WB-CP/…" /
        # "TN-CP/…" in the lexical sort.
        r = await client.get(
            f"{ZOHO_BOOKS_BASE}/{module}?organization_id={ZOHO_ORG_ID}"
            f"&per_page=200&sort_column={number_field}&sort_order=D"
            f"&{number_field}_startswith={prefix}/",
            headers=headers,
        )
        if r.status_code != 200:
            return None
        docs = (r.json() or {}).get(list_key, [])
        number_re = re.compile(rf"^{re.escape(prefix)}/(\d{{2}}-\d{{2}})/(\d+)$")
        matches = [
            m
            for d in docs
            if (m := number_re.match(str(d.get(number_field, ""))))
        ]
        fy_matches = [m for m in matches if m.group(1) == fy_str]
        if fy_matches:
            # Highest sequence this FY (don't trust the lexical sort order).
            ref = max(fy_matches, key=lambda m: int(m.group(2)))
            num_width, last_num = len(ref.group(2)), int(ref.group(2))
        elif matches:
            # New financial year: keep the series, restart the sequence.
            num_width, last_num = len(matches[0].group(2)), 0
        else:
            # Series not present in the latest 200 — start it fresh.
            num_width, last_num = 4, 0
        counter_id = f"{module}_counter_{fy_str}"
        db.counters.update_one({"_id": counter_id}, {"$max": {"seq": last_num}}, upsert=True)
        counter = db.counters.find_one_and_update(
            {"_id": counter_id}, {"$inc": {"seq": 1}}, return_document=True
        )
        number = f"{prefix}/{fy_str}/{str(counter['seq']).zfill(num_width)}"
        # Zoho caps document numbers at 16 chars — fall back to auto-numbering
        # rather than fail the create call.
        return number if len(number) <= 16 else None
    except Exception:
        return None


async def _fetch_and_store_razorpay_payment(
    client: httpx.AsyncClient, order_oid, rzp_payment_id: str
) -> dict:
    """Fetch the full Razorpay payment and persist the useful details onto
    order.payment.* (method, amounts, fee, payer contact, instrument info) so
    the admin can see exactly how the customer paid. Best-effort — a lookup
    failure never blocks recording the payment in Zoho."""
    if not rzp_payment_id:
        return {}
    try:
        r = await client.get(
            f"{RAZORPAY_BASE_URL}/payments/{rzp_payment_id}",
            auth=(RAZORPAY_KEY or "", RAZORPAY_SECRET or ""),
        )
        p = r.json() if r.status_code == 200 else {}
    except Exception:
        p = {}
    if not p or not p.get("id"):
        return {}

    def _rupees(paise):
        try:
            return round(int(paise) / 100, 2)
        except (TypeError, ValueError):
            return None

    details = {
        "payment.method": p.get("method", ""),
        "payment.amount_paid": _rupees(p.get("amount")),
        "payment.currency": p.get("currency", ""),
        "payment.fee": _rupees(p.get("fee")),
        "payment.fee_tax": _rupees(p.get("tax")),
        "payment.email": p.get("email", ""),
        "payment.contact": p.get("contact", ""),
        "payment.bank": p.get("bank") or "",
        "payment.wallet": p.get("wallet") or "",
        "payment.vpa": p.get("vpa") or "",
        "payment.acquirer_data": p.get("acquirer_data") or {},
    }
    card = p.get("card") or {}
    if card:
        details["payment.card_network"] = card.get("network", "")
        details["payment.card_last4"] = card.get("last4", "")
        details["payment.card_type"] = card.get("type", "")
    if p.get("created_at"):
        try:
            details["payment.paid_at"] = datetime.fromtimestamp(int(p["created_at"]))
        except (TypeError, ValueError, OSError):
            pass
    orders_collection.update_one({"_id": order_oid}, {"$set": details})
    return p


async def _ensure_razorpay_captured(client: httpx.AsyncClient, order_id, rzp_payment: dict):
    """If the Razorpay payment is still only authorized, capture it now so the
    money actually settles (backstop for payments created before auto-capture
    was enabled). Best-effort — never raises."""
    try:
        if not rzp_payment or rzp_payment.get("status") != "authorized":
            return rzp_payment
        pid = rzp_payment.get("id")
        r = await client.post(
            f"{RAZORPAY_BASE_URL}/payments/{pid}/capture",
            json={
                "amount": rzp_payment.get("amount"),
                "currency": rzp_payment.get("currency", "INR"),
            },
            auth=(RAZORPAY_KEY or "", RAZORPAY_SECRET or ""),
        )
        data = r.json() if r.content else {}
        _log_transaction(
            "razorpay_capture",
            order_id,
            razorpay_payment_id=pid,
            status_code=r.status_code,
            result_status=data.get("status", ""),
            error=data.get("error", {}).get("description", "") if r.status_code >= 400 else "",
        )
        if r.status_code == 200:
            return data
    except Exception as e:
        _log_transaction("razorpay_capture_error", order_id, error=str(e))
    return rzp_payment


def _is_duplicate_number_error(message: str) -> bool:
    return "already exists" in str(message or "").lower()


async def _run_post_payment_zoho_chain(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """Run the accepted-estimate -> sales order -> invoice -> payment chain.
    Raises on the first failing step (callers log it); already-completed steps
    are skipped, so re-running after a partial failure finishes the rest."""
    from .orders import finalise  # lazy import to avoid circular import
    from .helpers import get_access_token

    order = orders_collection.find_one({"_id": ObjectId(order_id)})
    if not order:
        raise RuntimeError(f"Order {order_id} not found")
    if (order.get("payment") or {}).get("status") != "paid":
        raise RuntimeError(f"Order {order_id} is not paid — chain aborted")

    # Coarse claim so verify + webhook can't run the chain concurrently.
    claimed = orders_collection.find_one_and_update(
        {"_id": order["_id"], "zoho_flow.chain_running": {"$ne": True}},
        {
            "$set": {
                "zoho_flow.chain_running": True,
                "zoho_flow.chain_started_at": datetime.now(),
            }
        },
    )
    if claimed is None:
        _log_transaction("zoho_chain_skipped", order_id, reason="already_running")
        return {"status": "success", "message": "Zoho chain already running"}

    message_parts = []
    try:
        # ── Step 1: estimate accepted (finalise creates it first if needed) ──
        needs_accept = (
            str(order.get("status", "")).lower() != "accepted"
            or not (order.get("estimate_created") or order.get("pre_order_estimate_created"))
        )
        if needs_accept:
            result = await finalise(
                {
                    "order_id": str(order_id),
                    "status": "accepted",
                    "create_stock": True,
                    "create_pre_order": True,
                },
                request,
                background_tasks,
            )
            _log_transaction("zoho_chain_estimate_accepted", order_id, result=result)
            if result.get("status") != "success":
                raise RuntimeError(f"Estimate accept failed: {result.get('message')}")
            message_parts.append(result.get("message", "Estimate accepted"))
            order = orders_collection.find_one({"_id": ObjectId(order_id)})

        flow = order.get("zoho_flow") or {}
        payment_info = order.get("payment") or {}
        rzp_payment_id = (
            payment_info.get("razorpay_payment_id") or payment_info.get("payment_id") or ""
        )

        customer = db.customers.find_one({"_id": ObjectId(order.get("customer_id"))})
        if not customer or not customer.get("contact_id"):
            raise RuntimeError("Order's customer has no Zoho contact_id")
        contact_id = customer["contact_id"]

        est_ids = [i for i in (order.get("estimate_id"), order.get("pre_order_estimate_id")) if i]
        est_numbers = [
            n
            for n in (order.get("estimate_number"), order.get("pre_order_estimate_number"))
            if n
        ]
        if not est_ids:
            raise RuntimeError("No estimate ids on the order — cannot build sales order")

        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        today = datetime.now().strftime("%Y-%m-%d")

        async with httpx.AsyncClient(timeout=httpx.Timeout(45.0, connect=10.0)) as client:
            # Pull line items + tax context from the created estimate(s) so the
            # sales order and invoice mirror exactly what was quoted and paid.
            line_items = []
            is_inclusive_tax = False
            place_of_supply = ""
            salesperson_id = ""
            for eid in est_ids:
                r = await client.get(
                    f"{ZOHO_BOOKS_BASE}/estimates/{eid}?organization_id={ZOHO_ORG_ID}",
                    headers=headers,
                )
                est = (r.json() or {}).get("estimate") if r.status_code == 200 else None
                if not est:
                    raise RuntimeError(f"Could not fetch estimate {eid} from Zoho")
                for li in est.get("line_items", []):
                    line_items.append({k: li.get(k) for k in _CHAIN_LINE_ITEM_KEYS})
                is_inclusive_tax = est.get("is_inclusive_tax", False)
                place_of_supply = est.get("place_of_supply", "") or place_of_supply
                salesperson_id = est.get("salesperson_id", "") or salesperson_id
            if not line_items:
                raise RuntimeError("Estimates have no line items")

            base_doc = {
                "customer_id": contact_id,
                "date": today,
                "line_items": line_items,
                "is_inclusive_tax": is_inclusive_tax,
                "place_of_supply": place_of_supply,
                "salesperson_id": salesperson_id,
                # Same location the estimates are created under (see orders.finalise).
                "location_id": "3220178000143298047",
            }

            async def _zoho_create(label, module, list_key, number_field, prefix, number_key, payload):
                """POST a Zoho document with a reserved sequence number, retrying
                with the next number if Zoho reports it already exists (the local
                counter can trail reality — e.g. documents created outside this
                flow). Falls back to Zoho auto-numbering when no number can be
                reserved."""
                last_message = ""
                for _ in range(4):
                    number = await _next_zoho_number(
                        client, headers, module, list_key, number_field, prefix
                    )
                    url = f"{ZOHO_BOOKS_BASE}/{module}?organization_id={ZOHO_ORG_ID}"
                    body = dict(payload)
                    if number:
                        body[number_key] = number
                        url += "&ignore_auto_number_generation=true"
                    r = await client.post(url, headers=headers, json=body)
                    rj = r.json() if r.content else {}
                    if r.status_code == 201 and rj.get("code", 0) == 0:
                        return rj
                    last_message = rj.get("message", r.text)
                    if not (number and _is_duplicate_number_error(last_message)):
                        raise RuntimeError(f"{label} creation failed: {last_message}")
                    _log_transaction(
                        f"zoho_chain_{module}_number_conflict",
                        order_id,
                        conflicting_number=number,
                    )
                    # Loop reserves the next number and retries.
                raise RuntimeError(f"{label} creation failed: {last_message}")

            # ── Step 2: DRAFT sales order from the estimate ──
            if not flow.get("salesorder_id"):
                rj = await _zoho_create(
                    "Sales order",
                    "salesorders",
                    "salesorders",
                    "salesorder_number",
                    "SO",
                    "salesorder_number",
                    {
                        **base_doc,
                        # Reference the estimate(s) this sales order was created from.
                        "reference_number": " / ".join(est_numbers),
                        "notes": f"Paid online via Razorpay ({rzp_payment_id}).",
                    },
                )
                so = rj["salesorder"]
                flow["salesorder_id"] = so["salesorder_id"]
                flow["salesorder_number"] = so.get("salesorder_number", "")
                _set_zoho_flow(
                    order["_id"],
                    {
                        "salesorder_id": flow["salesorder_id"],
                        "salesorder_number": flow["salesorder_number"],
                        "salesorder_created_at": datetime.now(),
                    },
                )
                _log_transaction(
                    "zoho_chain_salesorder_created",
                    order_id,
                    salesorder_number=flow["salesorder_number"],
                )
                message_parts.append(f"Sales order created: {flow['salesorder_number']}")

            # ── Step 3: invoice, marked SENT (required before applying payment) ──
            if not flow.get("invoice_id"):
                rj = await _zoho_create(
                    "Invoice",
                    "invoices",
                    "invoices",
                    "invoice_number",
                    "INV",
                    "invoice_number",
                    {
                        **base_doc,
                        # The invoice's order number = the sales order it fulfils.
                        "reference_number": flow.get("salesorder_number", ""),
                        "payment_terms": 0,
                        "payment_terms_label": "Due on Receipt",
                        "due_date": today,
                    },
                )
                inv = rj["invoice"]
                flow["invoice_id"] = inv["invoice_id"]
                flow["invoice_number"] = inv.get("invoice_number", "")
                flow["invoice_total"] = inv.get("total")
                _set_zoho_flow(
                    order["_id"],
                    {
                        "invoice_id": flow["invoice_id"],
                        "invoice_number": flow["invoice_number"],
                        "invoice_total": flow["invoice_total"],
                        "invoice_created_at": datetime.now(),
                    },
                )
                _log_transaction(
                    "zoho_chain_invoice_created",
                    order_id,
                    invoice_number=flow["invoice_number"],
                )
                message_parts.append(f"Invoice created: {flow['invoice_number']}")

            if not flow.get("invoice_sent"):
                r = await client.post(
                    f"{ZOHO_BOOKS_BASE}/invoices/{flow['invoice_id']}/status/sent?organization_id={ZOHO_ORG_ID}",
                    headers=headers,
                )
                rj = r.json() if r.content else {}
                # code 0 = marked sent; an already-sent invoice errors, which is fine.
                if r.status_code == 200 and rj.get("code", 0) == 0:
                    flow["invoice_sent"] = True
                    _set_zoho_flow(order["_id"], {"invoice_sent": True})

            # ── Step 4: customer payment applied to the invoice -> invoice PAID ──
            if not flow.get("customerpayment_id"):
                amount = flow.get("invoice_total")
                if amount is None:
                    amount = order.get("total_amount")
                rzp_payment = await _fetch_and_store_razorpay_payment(
                    client, order["_id"], rzp_payment_id
                )
                # Backstop: capture the payment if it's still only authorized
                # (payments created before auto-capture was enabled).
                rzp_payment = await _ensure_razorpay_captured(client, order_id, rzp_payment)
                rzp_method = rzp_payment.get("method", "")
                mode = RAZORPAY_METHOD_TO_ZOHO_MODE.get(rzp_method, "others")
                rj = await _zoho_create(
                    "Customer payment",
                    "customerpayments",
                    "customerpayments",
                    "payment_number",
                    "CP",
                    "payment_number",
                    {
                        "customer_id": contact_id,
                        "payment_mode": mode,
                        "amount": amount,
                        "date": today,
                        "reference_number": rzp_payment_id,
                        "invoices": [
                            {"invoice_id": flow["invoice_id"], "amount_applied": amount}
                        ],
                        "notes": f"Online payment via Razorpay ({rzp_method or 'unknown method'}).",
                    },
                )
                pay = rj["payment"]
                flow["customerpayment_id"] = pay["payment_id"]
                flow["customerpayment_number"] = pay.get("payment_number", "")
                _set_zoho_flow(
                    order["_id"],
                    {
                        "customerpayment_id": pay["payment_id"],
                        "customerpayment_number": pay.get("payment_number", ""),
                        "customerpayment_mode": mode,
                        "razorpay_method": rzp_method,
                        "customerpayment_created_at": datetime.now(),
                        "chain_completed_at": datetime.now(),
                        "last_error": None,
                    },
                )
                _log_transaction(
                    "zoho_chain_payment_recorded",
                    order_id,
                    zoho_payment_id=pay["payment_id"],
                    payment_mode=mode,
                )
                message_parts.append("Customer payment recorded — invoice marked paid")
                _notify_admins_chain_completed(order, flow)

        # Customer messaging in the correct order: payment confirmation FIRST,
        # then the order-accepted update. Both are claim-deduped, so the Zoho
        # webhook and the verify/webhook wrappers can never double-send.
        fresh = orders_collection.find_one({"_id": order["_id"]}) or order
        _notify_customer_payment_success(fresh)
        _notify_customer_order_accepted(fresh)

        return {"status": "success", "message": "\n".join(message_parts) or "Zoho chain already complete"}
    except Exception as e:
        _set_zoho_flow(order["_id"], {"last_error": str(e), "last_error_at": datetime.now()})
        raise
    finally:
        orders_collection.update_one(
            {"_id": order["_id"]}, {"$set": {"zoho_flow.chain_running": False}}
        )


@router.post("/webhook")
async def razorpay_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receive Razorpay webhook events and react:
      • payment_link.paid          -> mark paid + create the DRAFT estimate
      • payment_link.cancelled/expired -> mark failed, do NOT create an estimate

    Configure this URL in the Razorpay dashboard and set RAZORPAY_WEBHOOK_TEST_SECRET
    to the same secret to enable signature verification.
    """
    body = await request.body()

    if RAZORPAY_WEBHOOK_SECRET:
        signature = request.headers.get("X-Razorpay-Signature", "")
        expected = hmac.new(
            RAZORPAY_WEBHOOK_SECRET.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            _log_transaction("webhook_invalid_signature", None, raw=body.decode("utf-8", "replace"))
            raise HTTPException(status_code=400, detail="Invalid webhook signature")

    payload = await request.json()
    event = payload.get("event", "")
    p = payload.get("payload", {})
    link_entity = p.get("payment_link", {}).get("entity", {}) or {}
    payment_entity = p.get("payment", {}).get("entity", {}) or {}
    order_entity = p.get("order", {}).get("entity", {}) or {}

    link_id = link_entity.get("id")
    payment_id = payment_entity.get("id")
    rzp_order_id = order_entity.get("id") or payment_entity.get("order_id")

    # We always tag the order id in notes on the Razorpay order / payment link.
    # Razorpay copies those notes onto the payment, so it's available on
    # payment.* / order.* events too. reference_id is "{order_id}-{ts}".
    order_oid = (
        link_entity.get("notes", {}).get("order_id")
        or order_entity.get("notes", {}).get("order_id")
        or payment_entity.get("notes", {}).get("order_id")
    )
    ref = link_entity.get("reference_id") or order_entity.get("receipt") or ""
    if not order_oid and ref:
        order_oid = ref.split("-")[0]

    link_status = link_entity.get("status")
    # Outcome can come from the link status OR the event name (payment.* events
    # carry no payment_link entity, so we lean on the event there).
    paid = (link_status in PAID_STATUSES) or event in {
        "payment_link.paid", "payment.captured", "order.paid"
    }
    failed = (link_status in FAILED_STATUSES) or event in {
        "payment.failed", "payment_link.cancelled", "payment_link.expired"
    }
    status = link_status or ("paid" if paid else ("failed" if failed else event))

    # Resolve the order this event belongs to.
    order = None
    if order_oid and ObjectId.is_valid(order_oid):
        order = orders_collection.find_one({"_id": ObjectId(order_oid)})
    if not order and link_id:
        order = orders_collection.find_one({"payment.payment_link_id": link_id})
    if not order and rzp_order_id:
        order = orders_collection.find_one({"payment.razorpay_order_id": rzp_order_id})

    order_id = str(order["_id"]) if order else order_oid

    _log_transaction(
        "webhook_event",
        order_id,
        event=event,
        status=status,
        payment_link_id=link_id,
        payload=payload,
    )

    if not order:
        # Nothing to update, but we've recorded the event.
        return {"ok": True, "matched_order": False}

    # Always persist the latest payment status on the order.
    payment_set = {
        "payment.status": status,
        "payment.updated_at": datetime.now(),
        "payment.last_event": event,
    }
    if payment_id:
        payment_set["payment.payment_id"] = payment_id
    orders_collection.update_one({"_id": order["_id"]}, {"$set": payment_set})

    # ── Successful payment: create the DRAFT estimate (idempotent) ──
    if paid:
        already_accepted = (
            order.get("estimate_created")
            or str(order.get("status", "")).lower() == "accepted"
        )
        # Self-registered orders always run the (idempotent) post-payment chain
        # so a partially-completed sales order / invoice / payment gets finished.
        if already_accepted and not _is_self_registered_order(order):
            _log_transaction("webhook_estimate_skipped", order_id, reason="already_created")
            # Estimate already exists -> confirm now (order already has its number).
            _notify_customer_payment_success(order)
        else:
            try:
                result = await _create_draft_estimate_on_payment(order_id, request, background_tasks)
                _log_transaction("webhook_estimate_accepted", order_id, result=result)
            except Exception as e:
                # Record the failure; payment is captured so this needs manual follow-up.
                _log_transaction("webhook_estimate_error", order_id, error=str(e))
                print(f"[razorpay] estimate creation failed for order {order_id}: {e}")
            finally:
                # Confirm to the customer AFTER estimate creation so the message
                # carries the estimate number (idempotent vs the verify path).
                fresh = orders_collection.find_one({"_id": order["_id"]}) or order
                _notify_customer_payment_success(fresh)

    # ── Failed / cancelled / expired: do NOT create an estimate ──
    elif failed:
        _log_transaction("webhook_payment_failed", order_id, status=status)

    return {"ok": True, "matched_order": True, "status": status}
