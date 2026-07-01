"""
Public shipment-tracking redirect.

WhatsApp "Track order" URL buttons can only carry a single fixed domain, so we
point the button at THIS endpoint and 302-redirect to whichever carrier's
tracking page is stored on the shipment. This keeps one stable button URL that
works across all carriers (SM Express, Maruti Air, Delhivery, …).

Mounted publicly (no JWT) because the link is opened from the customer's phone
with no session. It only reveals a tracking URL keyed by an opaque AWB/reference
number, so there's nothing sensitive to protect here.
"""
from urllib.parse import unquote

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse

from ..config.root import get_database

router = APIRouter()
db = get_database()


def _fallback_page(number: str) -> HTMLResponse:
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Track your order</title></head>
<body style="font-family:system-ui,sans-serif;max-width:480px;margin:48px auto;padding:0 20px;text-align:center;color:#1a365d">
<h2>We couldn't find a tracking link</h2>
<p>Tracking reference <b>{number}</b> isn't available yet. It may take a little
while after dispatch for the carrier link to appear.</p>
<p>Please check back shortly or contact us for help.</p>
</body></html>"""
    return HTMLResponse(content=html, status_code=404)


@router.get("/{tracking_number}")
def track_shipment(tracking_number: str):
    """Redirect to the carrier tracking page stored on the matching shipment."""
    number = unquote(tracking_number).strip()
    if not number:
        return _fallback_page(number)

    shipment = db.shipments.find_one(
        {
            "$or": [
                {"reference_number": number},
                {"tracking_number": number},
            ]
        }
    )
    link = (shipment or {}).get("tracking_link")
    if link and isinstance(link, str) and link.strip():
        url = link.strip()
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        return RedirectResponse(url=url, status_code=302)

    return _fallback_page(number)
