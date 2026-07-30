"""
Shared PDF rendering (WeasyPrint + Jinja2).

Lives in config/ rather than scripts/ because scripts/ is gitignored and this is
imported at request time by routes/distributor_portal.py.

WeasyPrint binds to cairo/pango, which pip cannot supply — see the apt-get line
in the Dockerfile. Import this module lazily from request handlers so a missing
system library can't take down app startup.
"""

import base64
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = BASE_DIR / "templates"
LOGO_PATH = BASE_DIR / "assets" / "pupscribe-wordmark-print.svg"

SELLER_NAME = "Pupscribe Enterprises Pvt Ltd"


def data_uri(path: Path) -> str:
    """Inline an asset so the PDF makes no external fetches at render time."""
    if not path.exists():
        return ""
    mime = "image/svg+xml" if path.suffix.lower() == ".svg" else "image/png"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )


def render(template_name: str, context: dict) -> bytes:
    from weasyprint import HTML

    html = _env().get_template(template_name).render(**context)
    return HTML(string=html, base_url=str(TEMPLATE_DIR)).write_pdf()


def _fmt_date(value) -> str:
    if not value:
        return "—"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value[:10]
    try:
        return value.strftime("%d %b %Y")
    except AttributeError:
        return "—"


def build_order_pdf(registration: dict, order: dict) -> bytes:
    """Single-order sheet for a distributor.

    `order` is already scoped and shaped by the caller (their line items only),
    so this function cannot widen what the distributor sees.
    """
    status = (order.get("status") or "").strip().lower()
    estimate_number = order.get("estimate_number") or ""
    return render(
        "distributor_order.html",
        {
            "brand_name": registration.get("brand_name") or "Your brand",
            "company_name": registration.get("company_name", ""),
            "seller_name": SELLER_NAME,
            "logo": data_uri(LOGO_PATH),
            "generated_at": datetime.now().strftime("%d %b %Y, %H:%M"),
            "order": {
                **order,
                "status": status,
                "date": _fmt_date(order.get("created_at")),
                # Falls back to the id tail so an order with no estimate still
                # has something quotable printed on it.
                "reference": estimate_number or f"#{str(order.get('_id',''))[-8:]}",
            },
        },
    )
