import plivo
import os
import re
import datetime
from dotenv import load_dotenv
from plivo.utils.template import Template  # Import the Template class

load_dotenv()

PLIVO_AUTH_ID = os.getenv("PLIVO_AUTH_ID")
PLIVO_AUTH_TOKEN = os.getenv("PLIVO_AUTH_TOKEN")
FROM_NUMBER = os.getenv("FROM_NUMBER")
# Public base URL of THIS backend (e.g. https://api.pupscribe.in). Plivo POSTs
# delivery/read status reports here; without it, messages stay "queued" forever.
CALLBACK_BASE_URL = (os.getenv("CALLBACK_BASE_URL") or "").rstrip("/")
client = plivo.RestClient(PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN)


def _status_callback_url():
    """Build the delivery-status callback URL, or None if not configured."""
    if not CALLBACK_BASE_URL:
        return None
    return f"{CALLBACK_BASE_URL}/api/chats/callback"


def _get_chats_collection():
    from .root import get_database
    return get_database()["chats"]


def generate_whatsapp_template(template_doc: dict, dynamic_params: dict) -> Template:
    """
    Given a template document from MongoDB and a dictionary of dynamic parameters,
    generate a Plivo Template instance with a body component and optionally a button component.

    For example, if dynamic_params is:
      {
          "name": "Rohan",
          "order_id": "12345",
          "button_url": "https://your-dynamic-url.com"
      }

    The generated components will be:
      [
          {
              "type": "body",
              "parameters": [
                  {"type": "text", "text": "Rohan"},
                  {"type": "text", "text": "12345"}
              ]
          },
          {
              "type": "button",
              "sub_type": "url",
              "index": "0",
              "parameters": [
                  {"type": "text", "text": "https://your-dynamic-url.com"}
              ]
          }
      ]

    You can adjust this logic to support more buttons or different structures.
    """
    components = []

    # Prepare body parameters: exclude button-related keys (like 'button_url')
    body_params = [
        {"type": "text", "text": str(value)}
        for key, value in dynamic_params.items()
        if key != "button_url"
    ]

    # Only add a body component if we have body parameters
    if body_params:
        components.append(
            {
                "type": "body",
                "parameters": body_params,
            }
        )

    # Check if there's a dynamic URL for a button
    button_url = dynamic_params.get("button_url")
    if button_url:
        button_component = {
            "type": "button",
            "sub_type": "url",  # for URL button
            "index": "0",  # change this index if you have multiple buttons
            "parameters": [{"type": "text", "text": str(button_url)}],
        }
        components.append(button_component)

    # Return a Plivo Template instance using the collected components
    return Template(
        name=template_doc.get("name"),
        language=template_doc.get("language"),
        components=components,
    )


def _log_chat(chats_col, dst_phone: str, template_doc: dict, params: dict, message_uuid=None, status: str = "queued", error: str = None):
    doc = {
        "type": "outgoing",
        "from": FROM_NUMBER,
        "to": dst_phone,
        "template_name": template_doc.get("name"),
        "params": params,
        "message_uuid": message_uuid,
        "status": status,
        "created_at": datetime.datetime.now(),
    }
    if error:
        doc["error"] = error
    try:
        chats_col.insert_one(doc)
    except Exception as log_err:
        print(f"Failed to log WhatsApp chat: {log_err}")


def send_template_message(to: str, template_doc: dict, params: dict, campaign_id=None):
    """
    Like send_whatsapp(), but returns a structured result the campaign engine can
    persist per recipient: {"message_uuid", "status", "error", "dst"}. Still logs
    to `chats` (so the conversation view keeps working) and tags the chat row with
    campaign_id for cross-referencing.
    """
    dst_phone = _normalize_dst(to)
    if not dst_phone:
        return {"message_uuid": None, "status": "failed", "error": "invalid_phone", "dst": to}

    chats_col = _get_chats_collection()

    def _log(message_uuid=None, status="queued", error=None):
        doc = {
            "type": "outgoing",
            "from": FROM_NUMBER,
            "to": dst_phone,
            "template_name": template_doc.get("name"),
            "params": params,
            "message_uuid": message_uuid,
            "status": status,
            "created_at": datetime.datetime.now(),
        }
        if campaign_id:
            doc["campaign_id"] = str(campaign_id)
        if error:
            doc["error"] = error
        try:
            chats_col.insert_one(doc)
        except Exception as log_err:
            print(f"Failed to log campaign chat: {log_err}")

    try:
        create_kwargs = {
            "type_": "whatsapp",
            "src": FROM_NUMBER,
            "dst": dst_phone,
            "template": generate_whatsapp_template(template_doc, params),
        }
        callback_url = _status_callback_url()
        if callback_url:
            create_kwargs["url"] = callback_url
            create_kwargs["method"] = "POST"
        response = client.messages.create(**create_kwargs)
        uuid_val = _extract_uuid(response)
        _log(message_uuid=uuid_val, status="queued")
        return {"message_uuid": uuid_val, "status": "queued", "error": None, "dst": dst_phone}
    except plivo.exceptions.AuthenticationError as e:
        _log(status="failed", error=str(e))
        return {"message_uuid": None, "status": "failed", "error": str(e), "dst": dst_phone}
    except Exception as e:
        error_msg = str(e)
        status = "rate_limit_exceeded" if "rate limit" in error_msg.lower() else "failed"
        _log(status=status, error=error_msg)
        return {"message_uuid": None, "status": status, "error": error_msg, "dst": dst_phone}


def send_whatsapp(to: str, template_doc: dict, params: dict):
    # Resolve phone number before try/except so we can log failures
    dst_phone = _normalize_dst(to)
    if not dst_phone:
        print(f"Invalid phone number after cleaning: {to}")
        return None

    chats_col = _get_chats_collection()

    try:
        create_kwargs = {
            "type_": "whatsapp",
            "src": FROM_NUMBER,
            "dst": dst_phone,
            "template": generate_whatsapp_template(template_doc, params),
        }
        # Request delivery/read status reports so the chat status stops being "queued".
        callback_url = _status_callback_url()
        if callback_url:
            create_kwargs["url"] = callback_url
            create_kwargs["method"] = "POST"
        response = client.messages.create(**create_kwargs)

        raw_uuid = None
        if isinstance(response, dict):
            raw_uuid = response.get("message_uuid")
        elif hasattr(response, "message_uuid"):
            raw_uuid = response.message_uuid
        if isinstance(raw_uuid, list):
            uuid_val = raw_uuid[0] if raw_uuid else None
        elif isinstance(raw_uuid, str):
            uuid_val = raw_uuid
        else:
            uuid_val = None
        print(f"[whatsapp] sent to {dst_phone}, message_uuid={uuid_val}")
        _log_chat(chats_col, dst_phone, template_doc, params, message_uuid=uuid_val, status="queued")
        return response
    except plivo.exceptions.AuthenticationError as e:
        print("Authentication failed:", e)
        _log_chat(chats_col, dst_phone, template_doc, params, status="failed", error=str(e))
    except Exception as e:
        error_msg = str(e)
        print(f"An error occurred sending to {dst_phone}:", error_msg)
        status = "rate_limit_exceeded" if "rate limit" in error_msg.lower() else "failed"
        _log_chat(chats_col, dst_phone, template_doc, params, status=status, error=error_msg)


def _india_subscriber(digits: str):
    """Reduce a run of digits to a valid 10-digit Indian mobile, or None.
    Handles a '00' international prefix, a leading national '0', and a
    (possibly duplicated) '91' country code."""
    if digits.startswith("00"):           # 0091... international prefix
        digits = digits[2:]
    if len(digits) == 11 and digits.startswith("0"):  # 08104298709 national format
        digits = digits[1:]
    # Strip a country code, even when duplicated ('+91918104298709').
    while len(digits) > 10 and digits.startswith("91"):
        digits = digits[2:]
    if len(digits) == 10 and digits[0] in "6789":  # valid Indian mobile
        return digits
    return None


def _normalize_dst(to) -> str:
    """Normalize a raw phone value to a single E.164 number ('+91…' for bare Indian
    mobiles). Returns '' when no usable number can be extracted.

    Real customer data crams several numbers into one field separated by commas,
    slashes, spaces or hyphens (e.g. '+91-9819442211,9819445588'), duplicates the
    country code ('+91918104298709'), or carries a leading-0 national format. We
    split on separators and return the first candidate that yields a valid 10-digit
    Indian mobile; failing that, the first candidate that already looks like a
    complete international number. A leading '+' is NOT trusted verbatim — the value
    is always cleaned first, since the bad data that reaches Plivo starts with '+'."""
    if to is None:
        return ""
    raw = str(to).strip()
    if not raw:
        return ""
    candidates = [c for c in re.split(r"[,;/\s]+", raw) if c.strip()] or [raw]

    # Pass 1: first candidate that is a valid Indian mobile.
    for cand in candidates:
        digits = "".join(ch for ch in cand if ch.isdigit())
        if not digits:
            continue
        sub = _india_subscriber(digits)
        if sub:
            return f"+91{sub}"
    # Pass 2: first candidate that already carries a plausible country code.
    for cand in candidates:
        had_plus = cand.strip().startswith("+")
        digits = "".join(ch for ch in cand if ch.isdigit())
        if had_plus and 10 <= len(digits) <= 15:
            return f"+{digits}"
    return ""


def _extract_uuid(response) -> str:
    """Pull a single message_uuid string out of Plivo's response shapes."""
    raw_uuid = None
    if isinstance(response, dict):
        raw_uuid = response.get("message_uuid")
    elif hasattr(response, "message_uuid"):
        raw_uuid = response.message_uuid
    if isinstance(raw_uuid, list):
        return raw_uuid[0] if raw_uuid else None
    if isinstance(raw_uuid, str):
        return raw_uuid
    return None


def send_whatsapp_text(to: str, body: str, sent_by: str = None):
    """
    Send a free-form (session) WhatsApp text message. Only deliverable inside the
    24-hour customer-service window (i.e. after the user has messaged us recently);
    outside that window WhatsApp rejects free-form text and a template is required.
    Used for manual admin replies when there's no canned answer yet.
    """
    dst_phone = _normalize_dst(to)
    if not dst_phone:
        print(f"Invalid phone number after cleaning: {to}")
        return None

    chats_col = _get_chats_collection()

    def _log(message_uuid=None, status="queued", error=None):
        doc = {
            "type": "outgoing",
            "from": FROM_NUMBER,
            "to": dst_phone,
            "body": body,
            "is_free_form": True,
            "sent_by": sent_by,
            "message_uuid": message_uuid,
            "status": status,
            "created_at": datetime.datetime.now(),
        }
        if error:
            doc["error"] = error
        try:
            chats_col.insert_one(doc)
        except Exception as log_err:
            print(f"Failed to log WhatsApp text chat: {log_err}")

    try:
        create_kwargs = {
            "type_": "whatsapp",
            "src": FROM_NUMBER,
            "dst": dst_phone,
            "text": body,
        }
        callback_url = _status_callback_url()
        if callback_url:
            create_kwargs["url"] = callback_url
            create_kwargs["method"] = "POST"
        response = client.messages.create(**create_kwargs)

        uuid_val = _extract_uuid(response)
        print(f"[whatsapp] free-form sent to {dst_phone}, message_uuid={uuid_val}")
        _log(message_uuid=uuid_val, status="queued")
        return response
    except plivo.exceptions.AuthenticationError as e:
        print("Authentication failed:", e)
        _log(status="failed", error=str(e))
    except Exception as e:
        error_msg = str(e)
        print(f"An error occurred sending free-form text to {dst_phone}:", error_msg)
        status = "rate_limit_exceeded" if "rate limit" in error_msg.lower() else "failed"
        _log(status=status, error=error_msg)
    return None
