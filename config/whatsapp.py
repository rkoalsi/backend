import plivo
import os
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


def send_whatsapp(to: str, template_doc: dict, params: dict):
    # Resolve phone number before try/except so we can log failures
    phone_str = str(to).strip()
    if phone_str.startswith('+'):
        dst_phone = phone_str
    else:
        cleaned_phone = ''.join(char for char in phone_str if char.isdigit())
        if not cleaned_phone:
            print(f"Invalid phone number after cleaning: {to}")
            return None
        dst_phone = f"+91{cleaned_phone}"

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
