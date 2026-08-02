"""
Admin editor for a brand's own marketing site (thegoodtreatcompany.com).

The brand site renders its range from `GET /api/products/brand-site`, which is
built straight off the Zoho-synced `products` collection. The links this module
manages — the signed lab report PDF behind each recipe, and where to buy it —
are marketing artefacts with no Zoho equivalent, so they live in their own
`brand_site_settings` collection rather than on the product document, where the
catalogue sync would be free to overwrite them.

One document per brand:

    {
      "brand": "Jolly Pawps",
      "shop_url": "https://...",            # site-wide "Shop now"
      "lab_report_url": "https://...",      # optional catch-all report
      "products": {
        "<SKU>": {"lab_report_url": "...", "shop_url": "..."}
      }
    }

SKUs are alphanumeric, so they are safe to use as dotted `$set` paths.
"""

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import ast
import os
import re
import uuid

import boto3
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from pymongo import ASCENDING

from ..config.auth import get_current_user
from ..config.root import get_database

load_dotenv()

router = APIRouter()

db = get_database()
products_collection = db["products"]
settings_collection = db["brand_site_settings"]

SKU_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

# Lab reports are served from the brand site's own bucket/CDN
# (assets.thegoodtreatcompany.com), not the order form's — the PDFs are linked
# from thegoodtreatcompany.com and should live on that domain. Same AWS account,
# so the existing S3 credentials are reused unless overridden.
BRAND_SITE_BUCKET = os.getenv("BRAND_SITE_S3_BUCKET", "goodtreatcompany-assets")
BRAND_SITE_REGION = os.getenv(
    "BRAND_SITE_S3_REGION", os.getenv("S3_REGION", "ap-south-1")
)
BRAND_SITE_CDN_URL = os.getenv(
    "BRAND_SITE_CDN_URL", "https://assets.thegoodtreatcompany.com"
).rstrip("/")

MAX_REPORT_BYTES = 25 * 1024 * 1024

brand_site_s3 = boto3.client(
    "s3",
    region_name=BRAND_SITE_REGION,
    aws_access_key_id=os.getenv("BRAND_SITE_S3_ACCESS_KEY", os.getenv("S3_ACCESS_KEY")),
    aws_secret_access_key=os.getenv(
        "BRAND_SITE_S3_SECRET_KEY", os.getenv("S3_SECRET_KEY")
    ),
)


def _validate_url(value: Optional[str], field: str) -> Optional[str]:
    """Empty means "clear this link"; anything else must be an http(s) URL."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be a full http(s) URL",
        )
    return value


def _actor(token_payload: dict) -> Optional[str]:
    """Best-effort editor identity for the audit stamp."""
    user = (token_payload or {}).get("data") or {}
    if isinstance(user, dict):
        return user.get("email") or user.get("name")
    return None


def get_brand_settings(brand: str) -> dict:
    """The stored settings for a brand, or an empty shell if never edited."""
    doc = settings_collection.find_one({"brand": brand}) or {}
    products = doc.get("products") or {}
    return {
        "brand": brand,
        "shop_url": doc.get("shop_url") or "",
        "lab_report_url": doc.get("lab_report_url") or "",
        "products": products if isinstance(products, dict) else {},
        "updated_at": doc.get("updated_at"),
        "updated_by": doc.get("updated_by"),
    }


def _normalise_images(doc: dict) -> list:
    """Legacy rows store `images` as a Python repr string — same fix as /brand-site."""
    images = doc.get("images")
    if isinstance(images, str):
        try:
            images = ast.literal_eval(images)
        except (ValueError, SyntaxError):
            images = [images]
    if not isinstance(images, list):
        images = []
    images = [i for i in images if isinstance(i, str) and i.strip()]
    if not images and doc.get("image_url"):
        images = [doc["image_url"]]
    return images


class BrandSettingsUpdate(BaseModel):
    brand: str
    shop_url: Optional[str] = None
    lab_report_url: Optional[str] = None


class ProductLinksUpdate(BaseModel):
    brand: str
    lab_report_url: Optional[str] = None
    shop_url: Optional[str] = None


@router.get("")
def get_brand_site(
    brand: str = Query("Jolly Pawps", description="Brand name, e.g. 'Jolly Pawps'"),
):
    """
    Every active product for the brand, each with the links currently published
    on its brand site. Unedited products come back with empty strings so the
    admin table can render a full row either way.
    """
    settings = get_brand_settings(brand)
    overrides = settings["products"]

    docs = products_collection.find(
        {
            "brand": brand,
            "status": "active",
            "is_deleted": {"$exists": False},
        }
    ).sort("cf_sku_code", ASCENDING)

    products = []
    for doc in docs:
        sku = doc.get("cf_sku_code") or doc.get("sku") or ""
        images = _normalise_images(doc)
        links = overrides.get(sku) or {}
        name = doc.get("name", "")
        products.append(
            {
                "sku": sku,
                "name": name,
                "display_name": re.sub(
                    r"\s*-\s*\d+\s*g\s*$",
                    "",
                    re.sub(r"^%s\s+" % re.escape(brand), "", name),
                ).strip(),
                "series": doc.get("series"),
                "image": images[0] if images else None,
                "lab_report_url": links.get("lab_report_url") or "",
                "shop_url": links.get("shop_url") or "",
            }
        )

    return {
        "brand": brand,
        "settings": {
            "shop_url": settings["shop_url"],
            "lab_report_url": settings["lab_report_url"],
            "updated_at": settings["updated_at"],
            "updated_by": settings["updated_by"],
        },
        "count": len(products),
        "products": products,
    }


@router.put("/settings")
def update_brand_settings(
    payload: BrandSettingsUpdate, user: dict = Depends(get_current_user)
):
    """Site-wide links: the "Shop now" destination and a fallback lab report."""
    update = {
        "updated_at": datetime.now(timezone.utc),
        "updated_by": _actor(user),
    }
    if payload.shop_url is not None:
        update["shop_url"] = _validate_url(payload.shop_url, "shop_url")
    if payload.lab_report_url is not None:
        update["lab_report_url"] = _validate_url(payload.lab_report_url, "lab_report_url")

    settings_collection.update_one(
        {"brand": payload.brand},
        {"$set": update, "$setOnInsert": {"brand": payload.brand}},
        upsert=True,
    )
    return {"message": "Brand site settings updated", **get_brand_settings(payload.brand)}


@router.put("/products/{sku}")
def update_product_links(
    sku: str, payload: ProductLinksUpdate, user: dict = Depends(get_current_user)
):
    """Per-recipe links: its signed lab report PDF and its buy-now destination."""
    if not SKU_PATTERN.match(sku):
        raise HTTPException(status_code=400, detail="Invalid SKU")

    product = products_collection.find_one(
        {"brand": payload.brand, "$or": [{"cf_sku_code": sku}, {"sku": sku}]}
    )
    if not product:
        raise HTTPException(
            status_code=404, detail=f"No {payload.brand} product with SKU {sku}"
        )

    update = {
        "updated_at": datetime.now(timezone.utc),
        "updated_by": _actor(user),
    }
    if payload.lab_report_url is not None:
        update[f"products.{sku}.lab_report_url"] = _validate_url(
            payload.lab_report_url, "lab_report_url"
        )
    if payload.shop_url is not None:
        update[f"products.{sku}.shop_url"] = _validate_url(payload.shop_url, "shop_url")

    settings_collection.update_one(
        {"brand": payload.brand},
        {"$set": update, "$setOnInsert": {"brand": payload.brand}},
        upsert=True,
    )

    links = get_brand_settings(payload.brand)["products"].get(sku, {})
    return {
        "message": "Product links updated",
        "sku": sku,
        "lab_report_url": links.get("lab_report_url") or "",
        "shop_url": links.get("shop_url") or "",
    }


@router.post("/upload")
async def upload_lab_report(
    file: UploadFile = File(...),
    brand: str = Query("Jolly Pawps"),
    sku: str = Query(..., description="Catalogue SKU the report belongs to"),
    user: dict = Depends(get_current_user),
):
    """
    Push a signed lab report PDF to the brand site's own bucket and record the
    resulting CDN link against the recipe.

    Each upload gets its own key (`reports/<SKU>-<random>.pdf`) rather than
    overwriting the last one: the URL is stored in Mongo, so a fresh key means a
    replacement report goes live immediately instead of waiting out a CDN cache,
    and any link already shared keeps resolving.
    """
    if not SKU_PATTERN.match(sku):
        raise HTTPException(status_code=400, detail="Invalid SKU")

    product = products_collection.find_one(
        {"brand": brand, "$or": [{"cf_sku_code": sku}, {"sku": sku}]}
    )
    if not product:
        raise HTTPException(
            status_code=404, detail=f"No {brand} product with SKU {sku}"
        )

    filename = file.filename or ""
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

    body = await file.read()
    if not body:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")
    if len(body) > MAX_REPORT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Report is larger than {MAX_REPORT_BYTES // (1024 * 1024)} MB.",
        )
    if not body.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="That file is not a valid PDF.")

    key = f"reports/{sku}-{uuid.uuid4().hex[:8]}.pdf"
    # A readable name for anyone who saves the PDF out of their browser.
    download_name = re.sub(r"[^A-Za-z0-9._-]+", "-", f"{brand}-{sku}-lab-report").strip("-")

    try:
        brand_site_s3.put_object(
            Bucket=BRAND_SITE_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/pdf",
            CacheControl="public, max-age=31536000, immutable",
            ContentDisposition=f'inline; filename="{download_name}.pdf"',
        )
    except Exception as exc:  # noqa: BLE001 — surfaced verbatim to the admin UI
        raise HTTPException(
            status_code=502,
            detail=f"Could not upload to {BRAND_SITE_BUCKET}: {exc}",
        )

    file_url = f"{BRAND_SITE_CDN_URL}/{key}"
    settings_collection.update_one(
        {"brand": brand},
        {
            "$set": {
                f"products.{sku}.lab_report_url": file_url,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": _actor(user),
            },
            "$setOnInsert": {"brand": brand},
        },
        upsert=True,
    )

    return {"message": "Lab report uploaded", "sku": sku, "lab_report_url": file_url}


@router.delete("/products/{sku}")
def clear_product_links(
    sku: str,
    brand: str = Query("Jolly Pawps"),
):
    """Drops both links for a recipe — the brand site falls back to its defaults."""
    if not SKU_PATTERN.match(sku):
        raise HTTPException(status_code=400, detail="Invalid SKU")

    settings_collection.update_one(
        {"brand": brand},
        {"$unset": {f"products.{sku}": ""}},
    )
    return {"message": "Product links cleared", "sku": sku}
