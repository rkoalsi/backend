from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from datetime import datetime, timezone
import logging

from ..config.root import get_database, serialize_mongo_document

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

db = get_database()

# Scans are queried per card, newest first — index once at startup (idempotent).
try:
    db.business_card_scans.create_index([("card_id", 1), ("ts", -1)])
except Exception:
    pass


def _escape_vcard(value: str) -> str:
    """Escape special characters per the vCard 3.0 spec."""
    if not value:
        return ""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def _build_vcard(card: dict) -> str:
    """Serialize a card document into a vCard 3.0 (.vcf) string."""
    name = card.get("name", "") or ""
    # Split name into family/given for the structured N field.
    parts = name.split(" ", 1)
    given = parts[0] if parts else ""
    family = parts[1] if len(parts) > 1 else ""

    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"N:{_escape_vcard(family)};{_escape_vcard(given)};;;",
        f"FN:{_escape_vcard(name)}",
    ]
    if card.get("company"):
        lines.append(f"ORG:{_escape_vcard(card['company'])}")
    if card.get("title"):
        lines.append(f"TITLE:{_escape_vcard(card['title'])}")
    if card.get("phone"):
        lines.append(f"TEL;TYPE=CELL:{_escape_vcard(card['phone'])}")
    if card.get("whatsapp"):
        lines.append(f"TEL;TYPE=CELL,VOICE:{_escape_vcard(card['whatsapp'])}")
    if card.get("email"):
        lines.append(f"EMAIL;TYPE=INTERNET:{_escape_vcard(card['email'])}")
    if card.get("website"):
        lines.append(f"URL:{_escape_vcard(card['website'])}")
    if card.get("photo_url"):
        lines.append(f"PHOTO;VALUE=URI:{_escape_vcard(card['photo_url'])}")
    if card.get("bio"):
        lines.append(f"NOTE:{_escape_vcard(card['bio'])}")

    socials = card.get("socials") or {}
    for platform, url in socials.items():
        if url:
            lines.append(f"X-SOCIALPROFILE;TYPE={platform}:{_escape_vcard(url)}")

    lines.append("END:VCARD")
    return "\r\n".join(lines)


@router.get("/{slug}")
def get_card(slug: str):
    """Public endpoint: returns a single active business card by slug, or 404."""
    try:
        doc = db.business_cards.find_one({"slug": slug, "is_active": True})
        if not doc:
            return JSONResponse(content={"detail": "Card not found"}, status_code=404)
        return serialize_mongo_document(doc)
    except Exception as e:
        logger.error(f"Error fetching business card '{slug}': {e}")
        return JSONResponse(content={"detail": "Card not found"}, status_code=404)


@router.post("/{slug}/scan")
def record_scan(slug: str, request: Request):
    """Public endpoint: log one QR-code scan for a card.

    Called by the card page when it's opened via a QR code (?src=qr).
    Scans are anonymous — we store when it happened plus coarse client
    details (IP, user agent, referer) for the admin dashboard.
    """
    try:
        doc = db.business_cards.find_one({"slug": slug, "is_active": True}, {"_id": 1})
        if not doc:
            return JSONResponse(content={"detail": "Card not found"}, status_code=404)
        # Respect proxies (nginx/CDN) that pass the real client IP along.
        forwarded = request.headers.get("x-forwarded-for", "")
        ip = forwarded.split(",")[0].strip() if forwarded else (
            request.client.host if request.client else ""
        )
        db.business_card_scans.insert_one(
            {
                "card_id": doc["_id"],
                "slug": slug,
                "ts": datetime.now(timezone.utc),
                "ip": ip,
                "user_agent": request.headers.get("user-agent", "")[:512],
                "referer": request.headers.get("referer", "")[:512],
            }
        )
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error recording scan for '{slug}': {e}")
        # Tracking must never break the public page.
        return {"ok": False}


@router.get("/{slug}/vcard")
def get_card_vcard(slug: str):
    """Public endpoint: returns the card as a downloadable .vcf (Add to Contacts)."""
    try:
        doc = db.business_cards.find_one({"slug": slug, "is_active": True})
        if not doc:
            return JSONResponse(content={"detail": "Card not found"}, status_code=404)
        vcard = _build_vcard(doc)
        filename = f"{slug}.vcf"
        return Response(
            content=vcard,
            media_type="text/vcard; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.error(f"Error building vCard for '{slug}': {e}")
        return JSONResponse(content={"detail": "Card not found"}, status_code=404)
