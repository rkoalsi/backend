from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import HTMLResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo import ASCENDING, DESCENDING
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional
from pydantic import BaseModel
import os, json

router = APIRouter()

db = get_database()
products_collection = db["products"]
notify_requests_collection = db["product_notify_requests"]
customers_collection = db["customers"]


class NotifyMeRequest(BaseModel):
    product_id: str
    order_id: Optional[str] = None
    customer_id: Optional[str] = None
    quantity: int = 1


def get_product(product_id: str, collection: Collection):
    product = collection.find_one({"_id": ObjectId(product_id)})
    if not product:
        return "Product Not Found"
    return serialize_mongo_document(product)


@router.get("/counts")
def get_product_counts():
    """
    Returns the number of active products grouped by brand and category.
    Also includes a count of new products under a special "New Arrivals" brand.
    """
    try:
        base_query = {
            "stock": {"$gt": 0},
            "is_deleted": {"$exists": False},
            "status": "active",
        }

        pipeline = [
            {"$match": base_query},
            {
                "$group": {
                    "_id": {"brand": "$brand", "category": "$category"},
                    "count": {"$sum": 1},
                }
            },
        ]

        counts = list(db.products.aggregate(pipeline))

        hidden_brands = {
            doc["name"]
            for doc in db.brands.find({"hidden": True}, {"name": 1})
            if doc.get("name")
        }

        # Format counts into a nested dict: { brand: { category: count, ... }, ... }
        result = {}
        for item in counts:
            group_id = item["_id"]
            # Skip if either 'brand' or 'category' is missing or null
            if not group_id.get("brand") or not group_id.get("category"):
                continue
            brand = group_id["brand"]
            if brand in hidden_brands:
                continue
            category = group_id["category"]
            if brand not in result:
                result[brand] = {}
            result[brand][category] = item["count"]

        # Add count for "New Arrivals" - products created in last 3 months
        three_months_ago = datetime.now() - relativedelta(months=3)
        new_products_count = db.products.count_documents(
            {
                "stock": {"$gt": 0},
                "is_deleted": {"$exists": False},
                "status": "active",
                "created_at": {"$gte": three_months_ago},
            }
        )

        result["New Arrivals"] = {"All Products": new_products_count}

        # Add count for "Pre Orders" — products marked pre_order=true (no stock filter)
        pre_orders_count = db.products.count_documents(
            {
                "pre_order": True,
                "is_deleted": {"$exists": False},
            }
        )
        result["Pre Orders"] = {"All Products": pre_orders_count}

        return result

    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="Failed to get product counts")


@router.get("/brands")
def get_all_brands():
    """
    Retrieve a list of all distinct brands.
    """
    try:
        # Get distinct brand names
        brand_names = products_collection.distinct(
            "brand",
            {"stock": {"$gt": 0}, "status": "active", "is_deleted": {"$exists": False}},
        )
        brands = []
        for brand_name in brand_names:
            if brand_name != "":
                # Find brand document in brands collection
                brand_doc = db.brands.find_one(
                    {"name": {"$regex": brand_name, "$options": "i"}}
                )

                if brand_doc:
                    if brand_doc.get("hidden"):
                        continue
                    brand = {
                        "brand": brand_name,
                        "image": brand_doc.get("image_url"),
                        "secondary_image_url": brand_doc.get("secondary_image_url"),
                        "description": brand_doc.get("description"),
                    }
                else:
                    brand = {
                        "brand": brand_name,
                        "image": None,
                        "secondary_image_url": None,
                        "description": '',
                    }

                brands.append(brand)

        return {"brands": brands}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch brands.")


@router.get("/categories", response_model=dict)
def get_categories_for_brand(brand: str):
    """
    Retrieve a list of all distinct categories for a given brand.

    - **brand**: The name of the brand to fetch categories for.
    """
    try:
        # Fetch distinct categories for the specified brand
        categories = products_collection.distinct(
            "category",
            {
                "brand": brand,
                "stock": {"$gt": 0},
                "status": "active",
                "is_deleted": {"$exists": False},
            },
        )

        # Remove empty or null categories
        categories = [category for category in categories if category]
        return {"categories": categories}
    except Exception as e:
        # Log the exception details (ensure logging is set up in your application)
        print(f"Error fetching categories for brand '{brand}': {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch categories.")


@router.get("/catalogue_pages")
def get_catalogue_pages(brand: str):
    """
    Returns the distinct catalogue page numbers available from products.
    """
    try:
        # Fetch distinct catalogue_page values that exist and are not null.
        pages = db.products.distinct(
            "catalogue_page",
            {
                "catalogue_page": {"$exists": True, "$ne": None},
                "brand": brand,
                "stock": {"$gt": 0},
                "is_deleted": {"$exists": False},
            },
        )
        # Sort the pages (assuming they are numeric)
        pages = sorted(pages)
        return {"catalogue_pages": pages}
    except Exception as e:
        print(f"Error fetching catalogue pages: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def extract_base_name(product_name: str) -> str:
    """
    Extract the base name from a product name by removing size indicators.
    This matches the frontend logic in groupProducts.ts
    """
    import re

    base_name = product_name

    # Define size patterns (both abbreviated and full words)
    # IMPORTANT: Longer sizes must come first to avoid partial matches (XXXXL before XXXL before XXL before XL before L)
    size_pattern_abbrev = "(XXXXL|XXXL|XXL|XL|XXXXS|XXXS|XXS|XS|S|M|L)"
    size_pattern_full = "(X-Large|X-Small|XX-Large|XXX-Large|Extra Large|Extra Small|Large|Medium|Small)"

    # NEW: Remove (SIZE/measurement) pattern first - e.g., (XXL/62CM), (M/32CM), （XL/48CM）
    # Handles both regular parentheses () and full-width parentheses （）
    base_name = re.sub(
        rf"[（(]\s*{size_pattern_abbrev}\s*/\s*\d+\s*[Cc]?[Mm]\s*[)）]",
        "",
        base_name,
        flags=re.IGNORECASE,
    )

    # Remove various size patterns (matching frontend logic)
    # First, handle full word size patterns
    full_word_patterns = [
        rf"\s+-\s+{size_pattern_full}$",  # " - Large", " - Medium", " - X-Large" at end
        rf"\s+{size_pattern_full}$",  # " Large", " Medium" at end
        rf"-{size_pattern_full}$",  # "-Large", "-Medium" at end
        rf"\s+-\s+{size_pattern_full}\s+",  # " - Large ", " - Medium " in middle
        rf"\s+{size_pattern_full}\s+",  # " Large ", " Medium " in middle
    ]

    for pattern in full_word_patterns:
        base_name = re.sub(pattern, "", base_name, flags=re.IGNORECASE)

    # Then handle abbreviated size patterns
    # Pattern 1: Special handling to keep the color part
    pattern1 = rf"-([A-Za-z][^-]+)-{size_pattern_abbrev}$"
    base_name = re.sub(pattern1, r" \1", base_name, flags=re.IGNORECASE)

    # Remaining patterns - replace with space
    abbrev_patterns = [
        rf"\s+{size_pattern_abbrev}\s+-\s+",  # Pattern 2
        rf"\s+-\s+{size_pattern_abbrev}\s+",  # Pattern 3
        rf"-{size_pattern_abbrev}-",  # Pattern 4
        rf"-{size_pattern_abbrev}\s+",  # Pattern 5
        rf"-{size_pattern_abbrev}$",  # Pattern 6
        rf"\s+{size_pattern_abbrev}\s+",  # Pattern 7
        rf"\s+{size_pattern_abbrev}$",  # Pattern 8
        rf"^{size_pattern_abbrev}\s+",  # Pattern 9
        rf"\s*\({size_pattern_abbrev}\)$",  # Pattern 10
        rf"\s*#\d+\s*",  # Pattern 11 - shoe sizes
        rf"\s*\d+\.?\d*mm\s*",  # Pattern 12 - measurements in mm
        rf"\s*\d+\.?\d*[Mm]\s*",  # Pattern 13 - measurements in meters (3M, 5M)
        rf"\s+{size_pattern_abbrev}-",  # Pattern 14
    ]

    for pattern in abbrev_patterns:
        base_name = re.sub(pattern, " ", base_name, flags=re.IGNORECASE)

    # Remove weight indicators
    base_name = re.sub(r"\s*\(Max\s+\d+kgs?\)", "", base_name, flags=re.IGNORECASE)
    base_name = re.sub(r"\s*\(\d+-\d+kgs?\)", "", base_name, flags=re.IGNORECASE)

    # Clean up extra spaces and dashes
    base_name = re.sub(r"\s*-+\s*$", "", base_name)  # Remove trailing dashes
    base_name = re.sub(r"^\s*-+\s*", "", base_name)  # Remove leading dashes
    base_name = re.sub(r"\s*-\s*", " - ", base_name)  # Normalize spacing around dashes
    base_name = re.sub(r"\s+", " ", base_name).strip()

    return base_name


@router.get("")
def get_products(
    role: str = "salesperson",
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    catalogue_page: Optional[int] = Query(
        None, description="Catalogue page number for catalogue mode"
    ),
    per_page: int = Query(25, ge=1, le=100, description="Number of items per page"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search term for name or SKU code"),
    sort: Optional[str] = Query(
        "default", description="Sort order: default, price_asc, price_desc, catalogue"
    ),
    group_by_name: Optional[bool] = Query(
        False, description="Group products by base name"
    ),
    new_only: Optional[bool] = Query(
        False, description="Filter to show only new products (created in last 3 months)"
    ),
    pre_order: Optional[bool] = Query(
        False, description="Filter to show only pre-order products"
    ),
):
    """
    Retrieves paginated products with optional filters.
    When sort is "catalogue", the `catalogue_page` parameter is used to filter
    products by their catalogue_page field rather than for skipping documents.
    """
    # Define base query — skip stock filter for pre-orders (they may have zero stock by design)
    query = {"is_deleted": {"$exists": False}}
    if not pre_order:
        query["stock"] = {"$gt": 0}

    # Don't filter by brand when pre_order or new_only is true
    if pre_order:
        query["pre_order"] = True
    elif not new_only and brand:
        query["brand"] = brand

    if category:
        query["category"] = category

    if search:
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

    if role == "salesperson" and not pre_order:
        query["status"] = "active"

    three_months_ago = datetime.now() - relativedelta(months=3)

    # Build the aggregation pipeline
    pipeline = [
        {"$match": query},
        {
            "$addFields": {
                "new": {
                    "$cond": [
                        {"$gte": ["$created_at", three_months_ago]},
                        True,
                        False,
                    ]
                }
            }
        },
    ]

    # Filter to only new products if new_only is True
    if new_only:
        pipeline.append({"$match": {"created_at": {"$gte": three_months_ago}}})

    # Adjust query and sort based on sort order
    if sort == "price_asc":
        sort_stage = {"rate": ASCENDING}
    elif sort == "price_desc":
        sort_stage = {"rate": DESCENDING}
    elif sort == "catalogue":
        # Use the catalogue_page parameter to filter documents.
        if catalogue_page is not None and not search:
            query["catalogue_page"] = catalogue_page
            query["catalogue_order"] = {"$exists": True, "$ne": None}
        sort_stage = {"catalogue_order": ASCENDING}
    else:
        # Stage 1: Extract color and size
        pipeline.append(
            {
                "$addFields": {
                    # Extract color (last word before parenthesis or end)
                    "extracted_color": {
                        "$arrayElemAt": [
                            {
                                "$split": [
                                    {
                                        "$trim": {
                                            "input": {
                                                "$arrayElemAt": [
                                                    {"$split": ["$name", "("]},
                                                    0,
                                                ]
                                            }
                                        }
                                    },
                                    " ",
                                ]
                            },
                            -1,
                        ]
                    },
                    # Extract size - match XS, S, M, L, XL, XXL, XXXL, XXXXL with word boundaries
                    "extracted_size": {
                        "$regexFind": {
                            "input": "$name",
                            "regex": r"\b(XXXXL|XXXL|XXL|XL|XXXXS|XXXS|XXS|XS|S|M|L)\b",  # XXXXL must come before XXXL before XXL and XL
                        }
                    },
                }
            }
        )

        # Stage 2: Create size_for_sort from extracted_size
        pipeline.append(
            {
                "$addFields": {
                    "size_for_sort": {"$ifNull": ["$extracted_size.match", "ZZZ"]},
                }
            }
        )

        # Stage 3: Create size_order from size_for_sort
        pipeline.append(
            {
                "$addFields": {
                    "size_order": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": ["$size_for_sort", "XXXXS"]},
                                    "then": 1,
                                },
                                {
                                    "case": {"$eq": ["$size_for_sort", "XXXS"]},
                                    "then": 2,
                                },
                                {"case": {"$eq": ["$size_for_sort", "XXS"]}, "then": 3},
                                {"case": {"$eq": ["$size_for_sort", "XS"]}, "then": 4},
                                {"case": {"$eq": ["$size_for_sort", "S"]}, "then": 5},
                                {"case": {"$eq": ["$size_for_sort", "M"]}, "then": 6},
                                {"case": {"$eq": ["$size_for_sort", "L"]}, "then": 7},
                                {"case": {"$eq": ["$size_for_sort", "XL"]}, "then": 8},
                                {"case": {"$eq": ["$size_for_sort", "XXL"]}, "then": 9},
                                {
                                    "case": {"$eq": ["$size_for_sort", "XXXL"]},
                                    "then": 10,
                                },
                                {
                                    "case": {"$eq": ["$size_for_sort", "XXXXL"]},
                                    "then": 11,
                                },
                            ],
                            "default": 99,
                        }
                    },
                }
            }
        )

        sort_stage = {
            "brand": ASCENDING,
            "new": DESCENDING,
            "category": ASCENDING,
            "sub_category": ASCENDING,
            "series": ASCENDING,
            "extracted_color": ASCENDING,  # COLOR FIRST - groups colors together
            "size_order": ASCENDING,  # SIZE SECOND - sorts sizes within each color
            "rate": ASCENDING,
            "name": ASCENDING,
        }

    # Add sort stage
    pipeline.append({"$sort": sort_stage})

    # Only use skip if not in catalogue mode, because catalogue_page is used as a filter
    if sort != "catalogue":
        pipeline.append({"$skip": (page - 1) * per_page})

    pipeline.append({"$limit": per_page})

    try:
        fetched_products = list(db.products.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    all_products = [serialize_mongo_document(doc) for doc in fetched_products]

    # Enrich pre-order products with upcoming stock from purchase orders
    pre_order_prods = [p for p in all_products if p.get("pre_order")]
    if pre_order_prods:
        brand_names = list({p.get("brand") for p in pre_order_prods if p.get("brand")})
        item_id_to_vendor = {}
        if brand_names:
            brand_to_vendor = {
                b["name"]: b.get("vendor_id")
                for b in db.brands.find({"name": {"$in": brand_names}}, {"name": 1, "vendor_id": 1})
                if b.get("vendor_id")
            }
            item_id_to_vendor = {
                p["item_id"]: brand_to_vendor[p["brand"]]
                for p in pre_order_prods
                if p.get("item_id") and p.get("brand") in brand_to_vendor
            }
        if item_id_to_vendor:
            vendor_ids = list(set(item_id_to_vendor.values()))
            pos = list(db.purchase_orders.find(
                {"vendor_id": {"$in": vendor_ids}, "status": {"$nin": ["cancelled"]},
                 "line_items": {"$elemMatch": {"item_id": {"$in": list(item_id_to_vendor.keys())}}}},
                {"vendor_id": 1, "line_items": 1, "date": 1, "purchaseorder_number": 1}
            ).sort("date", -1))
            upcoming_by_item: dict = {}
            po_number_by_item: dict = {}
            seen_iids: set = set()
            for po in pos:
                for li in po.get("line_items", []):
                    iid = li.get("item_id")
                    if not iid or iid in seen_iids or item_id_to_vendor.get(iid) != po.get("vendor_id"):
                        continue
                    qty = float(li.get("quantity") or 0)
                    qty_received = float(li.get("quantity_received") or 0)
                    upcoming_by_item[iid] = max(0, int(qty - qty_received))
                    if po.get("purchaseorder_number"):
                        po_number_by_item[iid] = po["purchaseorder_number"]
                    seen_iids.add(iid)
            # Join to brand_orders (logistics tracking) for inward / ETA-at-port dates
            dates_by_po: dict = {}
            po_numbers = list(set(po_number_by_item.values()))
            if po_numbers:
                for bo in db.brand_orders.find(
                    {"purchaseorder_number": {"$in": po_numbers}},
                    {"purchaseorder_number": 1, "inward_date": 1, "eta_port_date": 1, "_id": 0}
                ):
                    dates_by_po[bo["purchaseorder_number"]] = {
                        "inward_date": bo.get("inward_date"),
                        "eta_port_date": bo.get("eta_port_date"),
                    }
            for p in all_products:
                iid = p.get("item_id")
                if p.get("pre_order") and iid in upcoming_by_item:
                    p["upcoming_stock"] = upcoming_by_item[iid]
                    dates = dates_by_po.get(po_number_by_item.get(iid))
                    if dates:
                        p["inward_date"] = dates.get("inward_date")
                        p["eta_port_date"] = dates.get("eta_port_date")

    try:
        total_products = db.products.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    total_pages = (
        ((total_products + per_page - 1) // per_page) if total_products > 0 else 1
    )

    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    # Handle grouping if requested
    if group_by_name:
        # Preserve the original sort order from the aggregation pipeline
        # Build groups while maintaining the order products appear in the list

        result_items = []  # Mixed list of groups and individual products in order
        seen_base_names = {}  # Track which base names we've seen and their position

        for product in all_products:
            base_name = extract_base_name(product["name"])
            base_name_key = base_name.lower()

            # Check if we've already started a group for this base name
            if base_name_key in seen_base_names:
                # Add to existing group at the position where it first appeared
                position = seen_base_names[base_name_key]
                result_items[position]["products"].append(product)
                result_items[position]["is_group"] = True  # Mark as multi-product group
            else:
                # First time seeing this base name - add at current position
                position = len(result_items)
                seen_base_names[base_name_key] = position
                result_items.append(
                    {
                        "is_group": False,  # Will be set to True if more products added
                        "groupId": f"group-{base_name_key.replace(' ', '-')}",
                        "baseName": base_name,
                        "products": [product],
                        "primaryProduct": product,
                    }
                )

        # Convert to final format maintaining exact order
        items_in_order = []

        for item in result_items:
            if item["is_group"]:
                # This is a true group (2+ products)
                items_in_order.append(
                    {
                        "type": "group",
                        "groupId": item["groupId"],
                        "baseName": item["baseName"],
                        "products": item["products"],
                        "primaryProduct": item["primaryProduct"],
                    }
                )
            else:
                # Single product
                items_in_order.append(
                    {
                        "type": "product",
                        "product": item["products"][0],
                    }
                )

        return {
            "items": items_in_order,  # Ordered list with type indicators
            "total": total_products,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "brand": brand,
            "category": category,
            "search": search,
        }

    return {
        "products": all_products,
        "total": total_products,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "brand": brand,
        "category": category,
        "search": search,
    }


@router.get("/all")
def get_all_products(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(25, ge=1, le=100, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search term for name or SKU code"),
):
    # Define base query
    query = {"is_deleted": {"$exists": False}}

    if search:
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

    three_months_ago = datetime.now() - relativedelta(months=3)

    # Build the aggregation pipeline
    pipeline = [
        {"$match": query},
        {
            "$addFields": {
                "new": {
                    "$cond": [
                        {"$gte": ["$created_at", three_months_ago]},
                        True,
                        False,
                    ]
                }
            }
        },
    ]

    try:
        fetched_products = list(db.products.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    all_products = [serialize_mongo_document(doc) for doc in fetched_products]

    try:
        total_products = db.products.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    total_pages = (
        ((total_products + per_page - 1) // per_page) if total_products > 0 else 1
    )

    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    return {
        "products": all_products,
        "total": total_products,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "search": search,
    }


@router.get("/all_categories", response_model=dict)
def get_all_product_categories():
    """
    Retrieve a sorted list of all distinct product categories (across all brands).
    Only categories for products with stock > 0 and not marked as deleted are returned.
    """
    try:
        # Use distinct to fetch unique categories across all products meeting the criteria
        categories = products_collection.distinct(
            "category", {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}}
        )
        # Filter out any empty or null values
        categories = [category for category in categories if category]
        # Sort the categories alphabetically
        return {"categories": sorted(categories)}
    except Exception as e:
        print(f"Error fetching all product categories: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch all product categories."
        )


@router.get("/batch")
def get_products_batch(ids: str = Query(..., description="Comma-separated product IDs")):
    """
    Fetch multiple products by their IDs in a single request.
    Returns a dict keyed by product_id for easy lookup, replacing N individual fetches.
    """
    try:
        id_list = [i.strip() for i in ids.split(",") if i.strip()]
        if not id_list:
            return {"products": {}}

        object_ids = []
        for pid in id_list:
            try:
                object_ids.append(ObjectId(pid))
            except Exception:
                pass

        products = list(products_collection.find({"_id": {"$in": object_ids}}))
        result = {}
        for product in products:
            serialized = serialize_mongo_document(product)
            result[serialized["_id"]] = serialized

        return {"products": result}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch products batch")


@router.get("/catalogue/all_products")
def get_all_products_catalogue(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(50, ge=1, le=200, description="Number of items per page"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search term for name or SKU code"),
    group_by_name: Optional[bool] = Query(
        True, description="Group products by base name"
    ),
    new_only: Optional[bool] = Query(
        None, description="Filter only new arrivals products"
    ),
):
    """
    PUBLIC ROUTE: Retrieves all active products for the public catalogue.
    No authentication required. Returns products with basic information for display.
    Special handling for new_only=true - shows all products created in last 3 months regardless of brand.
    """
    query = {
        "stock": {"$gt": 0},
        "is_deleted": {"$exists": False},
        "status": "active",
    }

    # Don't filter by brand when new_only is true
    if not new_only and brand:
        query["brand"] = brand

    if category:
        query["category"] = category

    if search:
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [{"name": regex}, {"cf_sku_code": regex}]

    # Calculate three months ago for new products filter
    three_months_ago = datetime.now() - relativedelta(months=3)

    # Build the aggregation pipeline
    pipeline = [
        {"$match": query},
        # Add "new" field computed from created_at
        {
            "$addFields": {
                "new": {
                    "$cond": [
                        {"$gte": ["$created_at", three_months_ago]},
                        True,
                        False,
                    ]
                }
            }
        },
    ]

    # Filter to only new products if new_only is True
    if new_only:
        pipeline.append({"$match": {"created_at": {"$gte": three_months_ago}}})

    # Add default sorting (same as main products route)
    # Stage 1: Extract color and size
    pipeline.append(
        {
            "$addFields": {
                "extracted_color": {
                    "$arrayElemAt": [
                        {
                            "$split": [
                                {
                                    "$trim": {
                                        "input": {
                                            "$arrayElemAt": [
                                                {"$split": ["$name", "("]},
                                                0,
                                            ]
                                        }
                                    }
                                },
                                " ",
                            ]
                        },
                        -1,
                    ]
                },
                "extracted_size": {
                    "$regexFind": {
                        "input": "$name",
                        "regex": r"\b(XXXXL|XXXL|XXL|XL|XXXXS|XXXS|XXS|XS|S|M|L)\b",
                    }
                },
            }
        }
    )

    # Stage 2: Create size_for_sort from extracted_size
    pipeline.append(
        {
            "$addFields": {
                "size_for_sort": {"$ifNull": ["$extracted_size.match", "ZZZ"]},
            }
        }
    )

    # Stage 3: Create size_order from size_for_sort
    pipeline.append(
        {
            "$addFields": {
                "size_order": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$size_for_sort", "XXXXS"]}, "then": 1},
                            {"case": {"$eq": ["$size_for_sort", "XXXS"]}, "then": 2},
                            {"case": {"$eq": ["$size_for_sort", "XXS"]}, "then": 3},
                            {"case": {"$eq": ["$size_for_sort", "XS"]}, "then": 4},
                            {"case": {"$eq": ["$size_for_sort", "S"]}, "then": 5},
                            {"case": {"$eq": ["$size_for_sort", "M"]}, "then": 6},
                            {"case": {"$eq": ["$size_for_sort", "L"]}, "then": 7},
                            {"case": {"$eq": ["$size_for_sort", "XL"]}, "then": 8},
                            {"case": {"$eq": ["$size_for_sort", "XXL"]}, "then": 9},
                            {"case": {"$eq": ["$size_for_sort", "XXXL"]}, "then": 10},
                            {"case": {"$eq": ["$size_for_sort", "XXXXL"]}, "then": 11},
                        ],
                        "default": 99,
                    }
                },
            }
        }
    )

    # Use exact same sort as main products route (lines 373-383)
    sort_stage = {
        "brand": ASCENDING,
        "category": ASCENDING,
        "sub_category": ASCENDING,
        "series": ASCENDING,
        "extracted_color": ASCENDING,
        "size_order": ASCENDING,
        "rate": ASCENDING,
        "name": ASCENDING,
    }

    # Add sort stage
    pipeline.append({"$sort": sort_stage})
    pipeline.append({"$skip": (page - 1) * per_page})
    pipeline.append({"$limit": per_page})

    try:
        fetched_products = list(db.products.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    all_products = [serialize_mongo_document(doc) for doc in fetched_products]

    try:
        total_products = db.products.count_documents(query)
    except Exception as e:
        print(f"Error counting documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    total_pages = (
        ((total_products + per_page - 1) // per_page) if total_products > 0 else 1
    )

    if page > total_pages and total_pages != 0:
        raise HTTPException(status_code=400, detail="Page number out of range")

    # Get list of distinct brands in new arrivals
    brands = db.products.distinct("brand", query)
    brands = [b for b in brands if b]  # Remove empty brands

    # Handle grouping if requested (same logic as main products route)
    if group_by_name:
        result_items = []
        seen_base_names = {}

        for product in all_products:
            base_name = extract_base_name(product["name"])
            base_name_key = base_name.lower()

            if base_name_key in seen_base_names:
                position = seen_base_names[base_name_key]
                result_items[position]["products"].append(product)
                result_items[position]["is_group"] = True
            else:
                position = len(result_items)
                seen_base_names[base_name_key] = position
                result_items.append(
                    {
                        "is_group": False,
                        "groupId": f"group-{base_name_key.replace(' ', '-')}",
                        "baseName": base_name,
                        "products": [product],
                        "primaryProduct": product,
                    }
                )

        items_in_order = []
        for item in result_items:
            if item["is_group"]:
                items_in_order.append(
                    {
                        "type": "group",
                        "groupId": item["groupId"],
                        "baseName": item["baseName"],
                        "products": item["products"],
                        "primaryProduct": item["primaryProduct"],
                    }
                )
            else:
                items_in_order.append(
                    {
                        "type": "product",
                        "product": item["products"][0],
                    }
                )

        return {
            "items": items_in_order,
            "total": total_products,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "brands": sorted(brands),
            "brand": brand,
            "category": category,
            "search": search,
        }

    return {
        "products": all_products,
        "total": total_products,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "brands": sorted(brands),
        "brand": brand,
        "category": category,
        "search": search,
    }


@router.get("/catalogue/init")
def get_catalogue_init(brand: Optional[str] = Query(None, description="Active brand to preload products for")):
    """
    Combined init endpoint: returns brands, product counts, categories for the given brand,
    and the first page of grouped products — all in one request.
    Eliminates the brands→categories→products waterfall on initial catalogue load.
    """
    try:
        from concurrent.futures import ThreadPoolExecutor

        active_brand = brand or "New Arrivals"

        def fetch_brands():
            brand_names = products_collection.distinct(
                "brand",
                {"stock": {"$gt": 0}, "status": "active", "is_deleted": {"$exists": False}},
            )
            result = []
            hidden_brands = {
                doc["name"]
                for doc in db.brands.find({"hidden": True}, {"name": 1})
                if doc.get("name")
            }
            for brand_name in brand_names:
                if not brand_name or brand_name in hidden_brands:
                    continue
                brand_doc = db.brands.find_one({"name": {"$regex": brand_name, "$options": "i"}})
                if brand_doc:
                    if brand_doc.get("hidden"):
                        continue
                    result.append({
                        "brand": brand_name,
                        "image": brand_doc.get("image_url"),
                        "secondary_image_url": brand_doc.get("secondary_image_url"),
                        "description": brand_doc.get("description"),
                    })
                else:
                    result.append({"brand": brand_name, "image": None, "secondary_image_url": None, "description": ""})
            return result

        def fetch_counts():
            base_query = {"stock": {"$gt": 0}, "is_deleted": {"$exists": False}, "status": "active"}
            pipeline = [
                {"$match": base_query},
                {"$group": {"_id": {"brand": "$brand", "category": "$category"}, "count": {"$sum": 1}}},
            ]
            counts = list(db.products.aggregate(pipeline))
            hidden_brands = {
                doc["name"]
                for doc in db.brands.find({"hidden": True}, {"name": 1})
                if doc.get("name")
            }
            result = {}
            for item in counts:
                gid = item["_id"]
                if not gid.get("brand") or not gid.get("category"):
                    continue
                b = gid["brand"]
                if b in hidden_brands:
                    continue
                c = gid["category"]
                if b not in result:
                    result[b] = {}
                result[b][c] = item["count"]
            three_months_ago = datetime.now() - relativedelta(months=3)
            new_count = db.products.count_documents({
                "stock": {"$gt": 0}, "is_deleted": {"$exists": False},
                "status": "active", "created_at": {"$gte": three_months_ago},
            })
            result["New Arrivals"] = {"All Products": new_count}
            return result

        def fetch_categories():
            if active_brand == "New Arrivals":
                return ["All Products"]
            cats = products_collection.distinct(
                "category",
                {"brand": active_brand, "stock": {"$gt": 0}, "status": "active", "is_deleted": {"$exists": False}},
            )
            return [c for c in cats if c]

        def fetch_products():
            return get_all_products_catalogue(
                page=1,
                per_page=200,
                brand=None if active_brand == "New Arrivals" else active_brand,
                category=None,
                search=None,
                group_by_name=True,
                new_only=True if active_brand == "New Arrivals" else None,
            )

        with ThreadPoolExecutor(max_workers=4) as executor:
            brands_future = executor.submit(fetch_brands)
            counts_future = executor.submit(fetch_counts)
            categories_future = executor.submit(fetch_categories)
            products_future = executor.submit(fetch_products)

            brands_list = brands_future.result()
            counts = counts_future.result()
            categories = categories_future.result()
            products_data = products_future.result()

        return {
            "brands": brands_list,
            "counts": counts,
            "categories": categories,
            "active_brand": active_brand,
            "items": products_data.get("items", []),
            "total": products_data.get("total", 0),
            "total_pages": products_data.get("total_pages", 1),
        }

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to fetch catalogue init data")


@router.get("/out-of-stock")
def get_out_of_stock_products(
    brand: Optional[str] = Query(None, description="Filter by brand"),
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """
    Retrieves products that are out of stock (stock <= 0).
    Optional filters for brand and category.
    """
    try:
        # Define base query for out of stock products
        query = {
            "$or": [{"stock": {"$lte": 0}}, {"stock": {"$exists": False}}],
            "is_deleted": {"$exists": False},
            "status": "active",
        }

        if brand:
            query["brand"] = brand

        if category:
            query["category"] = category

        # Fetch out of stock products
        out_of_stock_products = list(
            products_collection.find(query).sort("name", ASCENDING)
        )

        # Serialize the products
        serialized_products = [
            serialize_mongo_document(doc) for doc in out_of_stock_products
        ]

        return {"products": serialized_products}

    except Exception as e:
        print(f"Error fetching out of stock products: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch out of stock products"
        )


@router.post("/notify-me")
async def notify_me_when_available(request: NotifyMeRequest):
    """
    Registers a customer's request to be notified when a product is back in stock.
    Creates a document in the product_notify_requests collection.
    """
    try:
        # Verify the product exists
        product = products_collection.find_one({"_id": ObjectId(request.product_id)})
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")

        # Check if a similar request already exists
        existing_request = notify_requests_collection.find_one(
            {
                "product_id": request.product_id,
                "customer_id": request.customer_id,
                "notified": {"$ne": True},
            }
        )
        customer = customers_collection.find_one({"_id": ObjectId(request.customer_id)})

        if existing_request:
            # Update quantity on existing request
            notify_requests_collection.update_one(
                {"_id": existing_request["_id"]},
                {"$set": {"quantity": request.quantity, "updated_at": datetime.now()}},
            )
            return {
                "message": "Pre-order quantity updated successfully",
                "request_id": str(existing_request["_id"]),
            }

        # Create the notify request document
        notify_document = {
            "product_id": ObjectId(request.product_id),
            "customer_id": ObjectId(request.customer_id),
            "customer_name": customer.get("contact_name"),
            "product_name": product.get("name"),
            "product_brand": product.get("brand"),
            "quantity": request.quantity,
            "created_at": datetime.now(),
            "notified": False,
        }
        if request.order_id:
            notify_document["order_id"] = ObjectId(request.order_id)

        result = notify_requests_collection.insert_one(notify_document)

        return {
            "message": "Successfully registered for notification",
            "request_id": str(result.inserted_id),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating notify request: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to register for notification"
        )


@router.get("/notify-requests")
def get_notify_requests(
    notified: Optional[bool] = Query(None, description="Filter by notified status"),
    product_id: Optional[str] = Query(None, description="Filter by product ID"),
):
    """
    Admin endpoint to retrieve notification requests.
    Can filter by notified status and product ID.
    """
    try:
        query = {}

        if notified is not None:
            query["notified"] = notified

        if product_id:
            query["product_id"] = product_id

        # Fetch notification requests
        requests = list(
            notify_requests_collection.find(query).sort("created_at", DESCENDING)
        )

        # Serialize the requests
        serialized_requests = [serialize_mongo_document(doc) for doc in requests]

        # Enrich with customer information if customer_id exists
        for req in serialized_requests:
            if req.get("customer_id"):
                customer = db.customers.find_one({"_id": ObjectId(req["customer_id"])})
                if customer:
                    req["customer_name"] = customer.get("customer_name")
                    req["customer_email"] = customer.get("email")
                    req["customer_phone"] = customer.get("cf_phone")

        return {"requests": serialized_requests, "total": len(serialized_requests)}

    except Exception as e:
        print(f"Error fetching notify requests: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch notification requests"
        )


@router.get("/{product_id}")
def get_product_by_id(product_id: str):
    """
    Retrieve an product by its ID.
    """
    product = get_product(product_id, products_collection)
    if not product:
        raise HTTPException(status_code=404, detail="Order not found")
    return product


@router.put("/{product_id}")
async def update_product(product_id: str, product: dict):
    # Ensure '_id' is not in the update data
    update_data = {k: v for k, v in product.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # Perform the update
    result = products_collection.update_one(
        {"_id": ObjectId(product_id)},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product updated"}


@router.delete("/{product_id}")
async def delete_product(product_id: str):
    result = products_collection.update_one(
        {"_id": ObjectId(product_id)}, {"$set": {"is_deleted": True}}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product deleted"}


# @router.get("/", response_class=HTMLResponse)
# def index():
#     return "<h1>Backend is running<h1>"
