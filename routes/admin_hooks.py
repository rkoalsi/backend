from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from ..config.root import get_database, serialize_mongo_document 
from bson.objectid import ObjectId
from dotenv import load_dotenv
import math, datetime, io, openpyxl

load_dotenv()
router = APIRouter()

db = get_database()


@router.get("")
async def get_hooks(page: int = Query(0, ge=0), limit: int = Query(25, ge=1)):
    skip = page * limit

    pipeline = [
        {"$sort": {"created_at": -1}},
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
        {"$skip": skip},
        {"$limit": limit},
    ]

    try:
        shop_hooks_cursor = db.shop_hooks.aggregate(pipeline)
        shop_hooks = list(shop_hooks_cursor)
        total_count = db.shop_hooks.count_documents({})
        total_pages = math.ceil(total_count / limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse(
        status_code=200,
        content={
            "shop_hooks": serialize_mongo_document(shop_hooks),
            "total_count": total_count,
            "total_pages": total_pages,
        },
    )


@router.get("/report")
def get_shop_hooks_report():
    # Corrected query definition with proper lookup for created_by
    query = [
        {"$sort": {"created_at": -1}},
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
    ]

    # Fetch matching hooks
    hooks_cursor = db.shop_hooks.aggregate(query)
    hooks = [serialize_mongo_document(doc) for doc in hooks_cursor]

    # Create an Excel workbook using openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Shop Hooks Report"

    # Define the header row with customer info and hooks information
    headers = [
        "Customer Name",
        "Address",
        "City",
        "State",
        "ZIP",
        "Country",
        "Created By",
        "Created At",
        "Category",
        "Hooks Available",
        "Total Hooks",
    ]
    ws.append(headers)

    for hook_doc in hooks:
        # Format address properly
        customer_address = hook_doc.get("customer_address", {})
        formatted_address = f"{customer_address.get('address', '')}, {customer_address.get('street2', '')}"
        formatted_address = formatted_address.strip(", ")

        # Format creation date
        created_at = hook_doc.get("created_at", "")
        if created_at:
            created_at = (
                created_at.strftime("%Y-%m-%d %H:%M:%S")
                if hasattr(created_at, "strftime")
                else created_at
            )

        # Get creator name
        created_by_name = hook_doc.get("created_by_info", {}).get("name", "")

        # For each hook in the hooks array, create a separate row
        hooks_array = hook_doc.get("hooks", [])

        if hooks_array:
            for hook_item in hooks_array:
                row = [
                    hook_doc.get("customer_name", ""),
                    formatted_address,
                    customer_address.get("city", ""),
                    customer_address.get("state", ""),
                    customer_address.get("zip", ""),
                    customer_address.get("country", ""),
                    created_by_name,
                    created_at,
                    hook_item.get("category_name", ""),
                    hook_item.get("hooksAvailable", ""),
                    hook_item.get("totalHooks", ""),
                ]
                ws.append(row)
        else:
            # If no hooks, still add customer info
            row = [
                hook_doc.get("customer_name", ""),
                formatted_address,
                customer_address.get("city", ""),
                customer_address.get("state", ""),
                customer_address.get("zip", ""),
                customer_address.get("country", ""),
                created_by_name,
                created_at,
                "",
                "",
                "",
            ]
            ws.append(row)

    # Save the workbook to a binary stream
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=shop_hooks_report.xlsx"},
    )


@router.post("")
def create_hook_category(shop_hooks: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in shop_hooks.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        result = db.shop_hooks.insert_one(
            {**update_data, "created_at": datetime.datetime.now()}
        )
        if result:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Document Created Successfully",
                },
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{hook_category_id}")
def update_hook_category(hook_category_id: str, hook_category: dict):
    """
    Update the daily_visit with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in hook_category.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.shop_hooks.update_one(
            {"_id": ObjectId(hook_category_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.hook_category.find_one(
                {"_id": ObjectId(hook_category_id)}
            )
            return serialize_mongo_document(updated_catalogue)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{hook_category_id}")
def delete_daily_visit(hook_category_id: str):
    """
    Delete a daily_visit by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        result = db.shop_hooks.delete_one({"_id": ObjectId(hook_category_id)})
        if result:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Hooks Category not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
