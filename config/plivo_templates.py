"""
Thin HTTP client for Plivo's WhatsApp Template management API.

Plivo exposes full template lifecycle under:
    https://api.plivo.com/v1/Account/{auth_id}/WhatsApp/Template/{waba_id}/

We use raw HTTP (basic auth = auth_id:auth_token) rather than the SDK so we
aren't coupled to a particular plivo-python version. These helpers only talk to
Plivo/Meta; the local `templates` collection mirror is maintained by the route
layer (routes/admin_templates.py).

Required env:
    PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN   (already used by config/whatsapp.py)
    WABA_ID                           (the WhatsApp Business Account id)
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

PLIVO_AUTH_ID = os.getenv("PLIVO_AUTH_ID")
PLIVO_AUTH_TOKEN = os.getenv("PLIVO_AUTH_TOKEN")
PLIVO_WABA_ID = os.getenv("WABA_ID") or os.getenv("PLIVO_WABA_ID")

_BASE = "https://api.plivo.com/v1/Account"
_TIMEOUT = 30


class PlivoTemplateError(Exception):
    """Raised when Plivo rejects a template request; carries the upstream detail."""

    def __init__(self, message, status_code=None, payload=None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def _require_config():
    missing = [
        name
        for name, val in (
            ("PLIVO_AUTH_ID", PLIVO_AUTH_ID),
            ("PLIVO_AUTH_TOKEN", PLIVO_AUTH_TOKEN),
            ("WABA_ID", PLIVO_WABA_ID),
        )
        if not val
    ]
    if missing:
        raise PlivoTemplateError(
            f"Plivo template API not configured; missing env: {', '.join(missing)}"
        )


def _url(template_id: str = None) -> str:
    base = f"{_BASE}/{PLIVO_AUTH_ID}/WhatsApp/Template/{PLIVO_WABA_ID}/"
    return f"{base}{template_id}/" if template_id else base


def _request(method: str, url: str, **kwargs):
    _require_config()
    resp = requests.request(
        method,
        url,
        auth=(PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN),
        timeout=_TIMEOUT,
        **kwargs,
    )
    # Plivo returns JSON for both success and most errors.
    try:
        data = resp.json() if resp.content else {}
    except ValueError:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        detail = data.get("error") or data.get("message") or data
        raise PlivoTemplateError(
            f"Plivo template API error ({resp.status_code}): {detail}",
            status_code=resp.status_code,
            payload=data,
        )
    return data


# ---------------------------------------------------------------------------
# Component builder: structured admin input -> Meta component array
# ---------------------------------------------------------------------------

def build_components(
    header: dict = None,
    body: str = "",
    footer: str = "",
    buttons: list = None,
) -> list:
    """
    Build the `components` array Meta expects from a simplified admin payload.

    header  -> {"format": "TEXT"|"IMAGE"|"VIDEO"|"DOCUMENT", "text": "...",
                "example": "..."} (text/example only used for TEXT)
    body    -> string with positional {{1}}, {{2}} placeholders
    footer  -> short string (no placeholders allowed by Meta)
    buttons -> [{"type": "URL", "text": "Shop", "url": "https://...{{1}}"},
                {"type": "QUICK_REPLY", "text": "Stop promotions"},
                {"type": "PHONE_NUMBER", "text": "Call", "phone_number": "+91..."}]
    """
    components = []

    if header:
        fmt = (header.get("format") or "TEXT").upper()
        comp = {"type": "HEADER", "format": fmt}
        if fmt == "TEXT":
            comp["text"] = header.get("text", "")
            example = header.get("example")
            if example:
                comp["example"] = {"header_text": [example]}
        components.append(comp)

    if body:
        components.append({"type": "BODY", "text": body})

    if footer:
        components.append({"type": "FOOTER", "text": footer})

    if buttons:
        components.append(
            {
                "type": "BUTTONS",
                "buttons": [_normalize_button(b) for b in buttons],
            }
        )

    return components


def _normalize_button(b: dict) -> dict:
    btype = (b.get("type") or "QUICK_REPLY").upper()
    out = {"type": btype, "text": b.get("text", "")}
    if btype == "URL":
        out["url"] = b.get("url", "")
    elif btype == "PHONE_NUMBER":
        out["phone_number"] = b.get("phone_number", "")
    return out


# ---------------------------------------------------------------------------
# Lifecycle operations
# ---------------------------------------------------------------------------

def create_template(
    name: str,
    language: str,
    category: str,
    components: list,
) -> dict:
    """Submit a new template to Meta (via Plivo) for approval. Returns Plivo's
    response, which includes the template id and initial status (usually PENDING)."""
    payload = {
        "name": name,
        "language": language,
        "category": category,
        "components": components,
    }
    return _request("POST", _url(), json=payload)


def update_template(template_id: str, components: list, category: str = None) -> dict:
    """Edit an existing template's content. Meta re-reviews edited templates, so the
    status typically returns to PENDING."""
    payload = {"components": components}
    if category:
        payload["category"] = category
    return _request("POST", _url(template_id), json=payload)


def list_templates(limit: int = 100, offset: int = 0) -> dict:
    return _request("GET", _url(), params={"limit": limit, "offset": offset})


def get_template(template_id: str) -> dict:
    return _request("GET", _url(template_id))


def delete_template(template_id: str, name: str) -> dict:
    return _request("DELETE", _url(template_id), params={"name": name})
