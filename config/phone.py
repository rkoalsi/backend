"""Indian mobile number normalisation.

Customer records carry phone numbers in whatever shape Zoho received them:
bare 10-digit, 0-prefixed, +91-prefixed, and — often enough to matter — two
numbers crammed into one field. WhatsApp needs exactly one 10-digit mobile, so
every send path has to normalise first and refuse what it cannot resolve.

Deliberately does NOT do a blind `digits[-10:]`: for a field holding two numbers
that silently picks the second one and messages the wrong person.
"""
import re

# Indian mobile numbers are 10 digits starting 6-9. Landlines/short codes are not
# reachable on WhatsApp, so they are rejected rather than guessed at.
_MOBILE_RE = re.compile(r"^[6-9]\d{9}$")


def normalize_indian_mobile(raw) -> dict:
    """
    Resolve `raw` to a single 10-digit mobile.

    Returns {"phone": str|None, "valid": bool, "reason": str}. `reason` is written
    for an admin to read — it is surfaced in the UI before they try to send.
    """
    digits = re.sub(r"\D", "", str(raw or ""))

    if not digits:
        return {"phone": None, "valid": False, "reason": "No phone number on record"}

    candidate = digits

    # Strip the international/trunk prefixes we actually see in the data.
    # "00" is the international dialling prefix (00 91 98…), not a second number.
    if len(candidate) >= 12 and candidate.startswith("00"):
        candidate = candidate[2:]

    if len(candidate) == 13 and candidate.startswith("091"):
        candidate = candidate[3:]
    elif len(candidate) == 12 and candidate.startswith("91"):
        candidate = candidate[2:]
    elif len(candidate) == 11 and candidate.startswith("0"):
        candidate = candidate[1:]

    if _MOBILE_RE.match(candidate):
        return {"phone": candidate, "valid": True, "reason": ""}

    # Long values are almost always two numbers in one field ("98765.../91234...").
    # Guessing which one is meant is how you message the wrong customer.
    if len(digits) > 13:
        return {
            "phone": None,
            "valid": False,
            "reason": (
                f"Looks like more than one number ({len(digits)} digits). "
                "Split them and keep a single mobile."
            ),
        }

    if len(candidate) < 10:
        return {
            "phone": None,
            "valid": False,
            "reason": f"Too short for a mobile number ({len(digits)} digits)",
        }

    if len(candidate) > 10:
        return {
            "phone": None,
            "valid": False,
            "reason": f"Not a recognised Indian mobile format ({len(digits)} digits)",
        }

    return {
        "phone": None,
        "valid": False,
        "reason": "Indian mobile numbers must be 10 digits starting with 6-9",
    }


def to_whatsapp_number(raw) -> str:
    """Return the 10-digit mobile, or "" when it cannot be resolved."""
    return normalize_indian_mobile(raw)["phone"] or ""
