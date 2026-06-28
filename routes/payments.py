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
        accepts the order's Zoho estimate (via orders.finalise). On failure /
        cancellation / expiry it records the event and does NOT create an estimate.

Every request sent to and response received from Razorpay (and every webhook
payload) is persisted to the `razorpay_transactions` collection for auditing.
"""

import os
import hmac
import hashlib
import requests
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from bson.objectid import ObjectId
from dotenv import load_dotenv

from ..config.root import get_database

load_dotenv()

RAZORPAY_KEY = os.getenv("RAZORPAY_API_TEST_KEY")
RAZORPAY_SECRET = os.getenv("RAZORPAY_API_TEST_SECRET")
# Set this to the same secret configured for the webhook in the Razorpay dashboard.
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_TEST_SECRET")

RAZORPAY_BASE_URL = "https://api.razorpay.com/v1"

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
#        -> verifies the signature, and on success creates + accepts the estimate
#           and marks the order paid. Returns {success: true/false} synchronously
#           so the page can show a success/failure animation.
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/order/{order_id}/checkout")
def create_checkout_order(order_id: str):
    """
    Create a Razorpay Order for the order total and return Checkout params.
    """
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

    amount_paise = int(round(total_amount * 100))

    payload = {
        "amount": amount_paise,
        "currency": "INR",
        "receipt": str(order_id),
        "notes": {"order_id": str(order_id)},
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
    Verify a Razorpay Checkout payment signature and, on success, create + accept
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

    # Create + accept the estimate (idempotent).
    estimate_message = ""
    already_accepted = (
        order.get("estimate_created")
        or str(order.get("status", "")).lower() == "accepted"
    )
    if not already_accepted:
        try:
            result = await _accept_estimate_on_payment(order_id, request, background_tasks)
            estimate_message = (result or {}).get("message", "") if isinstance(result, dict) else ""
            _log_transaction("verify_estimate_accepted", order_id, result=result)
        except Exception as e:
            _log_transaction("verify_estimate_error", order_id, error=str(e))
            print(f"[razorpay] estimate creation failed for order {order_id}: {e}")

    return {
        "success": True,
        "order_id": str(order_id),
        "payment_id": rzp_payment_id,
        "message": estimate_message,
    }


async def _accept_estimate_on_payment(order_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Payment succeeded -> create AND accept the order's Zoho estimate.

    Calling orders.finalise with status='accepted' on an order whose estimate
    has not been created yet will both create the estimate(s) and push them to
    'accepted' in Zoho. Imported lazily to avoid an import cycle.
    """
    from .orders import finalise  # lazy import to avoid circular import

    return await finalise(
        {
            "order_id": str(order_id),
            "status": "accepted",
            "create_stock": True,
            "create_pre_order": True,
        },
        request,
        background_tasks,
    )


@router.post("/webhook")
async def razorpay_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receive Razorpay webhook events and react:
      • payment_link.paid          -> mark paid + create & accept the estimate
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

    # ── Successful payment: create + accept the estimate (idempotent) ──
    if paid:
        already_accepted = (
            order.get("estimate_created")
            or str(order.get("status", "")).lower() == "accepted"
        )
        if already_accepted:
            _log_transaction("webhook_estimate_skipped", order_id, reason="already_created")
        else:
            try:
                result = await _accept_estimate_on_payment(order_id, request, background_tasks)
                _log_transaction("webhook_estimate_accepted", order_id, result=result)
            except Exception as e:
                # Record the failure; payment is captured so this needs manual follow-up.
                _log_transaction("webhook_estimate_error", order_id, error=str(e))
                print(f"[razorpay] estimate creation failed for order {order_id}: {e}")

    # ── Failed / cancelled / expired: do NOT create an estimate ──
    elif failed:
        _log_transaction("webhook_payment_failed", order_id, status=status)

    return {"ok": True, "matched_order": True, "status": status}
