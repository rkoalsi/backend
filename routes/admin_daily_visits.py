from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople
from dotenv import load_dotenv
import math, datetime, io, openpyxl

load_dotenv()
router = APIRouter()

client, db = connect_to_mongo()

IST_OFFSET = 19800000


@router.get("")
async def get_daily_visits(request: Request):
    page = int(request.query_params.get("page", 0))
    limit = int(request.query_params.get("limit", 25))
    skip = page * limit
    # Get date filter parameters
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")

    # Initialize filter condition
    filter_condition = {}

    # Add date filtering if parameters are provided
    if start_date or end_date:
        filter_condition["created_at"] = {}

        if start_date:
            # Convert string to datetime and set to start of day (00:00:00)
            start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            filter_condition["created_at"]["$gte"] = start_datetime

        if end_date:
            # Convert string to datetime and set to end of day (23:59:59)
            end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            filter_condition["created_at"]["$lt"] = end_datetime

    pipeline = [
        {"$match": filter_condition},
        {"$sort": {"created_at": -1}},
        {
            "$lookup": {
                "from": "users",
                "localField": "created_by",
                "foreignField": "_id",
                "as": "created_by_info",
            }
        },
        # Unwind the created_by_info array to get a single object (if available)
        {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
        {"$skip": skip},
        {"$limit": limit},
        {
            "$addFields": {
                "created_at": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "date": {"$add": ["$created_at", IST_OFFSET]},
                    }
                },
                "updated_at": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "date": {"$add": ["$updated_at", IST_OFFSET]},
                    }
                },
                "updates": {
                    "$map": {
                        "input": {"$ifNull": ["$updates", []]},
                        "as": "update",
                        "in": {
                            "$mergeObjects": [
                                "$$update",
                                {
                                    "created_at": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d %H:%M:%S",
                                            "date": {
                                                "$add": [
                                                    "$$update.created_at",
                                                    IST_OFFSET,
                                                ]
                                            },
                                        }
                                    },
                                    "updated_at": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d %H:%M:%S",
                                            "date": {
                                                "$add": [
                                                    "$$update.updated_at",
                                                    IST_OFFSET,
                                                ]
                                            },
                                        }
                                    },
                                },
                            ]
                        },
                    }
                },
            }
        },
    ]

    try:
        daily_visits_cursor = db.daily_visits.aggregate(pipeline)
        daily_visits = list(daily_visits_cursor)
        total_count = db.daily_visits.count_documents(filter_condition)
        total_pages = math.ceil(total_count / limit)
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    # Optionally, merge the lookup field into the root document
    for visit in daily_visits:
        if "created_by_info" in visit and visit["created_by_info"]:
            # Assuming the user document has a "name" field.
            visit["created_by"] = visit["created_by_info"].get("name", "N/A")
        else:
            visit["created_by"] = "N/A"
        # Remove the lookup field.
        if "created_by_info" in visit:
            del visit["created_by_info"]
    return JSONResponse(
        status_code=200,
        content={
            "daily_visits": serialize_mongo_document(daily_visits),
            "total_count": total_count,
            "total_pages": total_pages,
        },
    )


@router.get("/report")
def get_daily_visits_report(request: Request):
    # Get date filter parameters from query
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")

    # Base query
    match_query = {}

    # Add date filtering if parameters are provided
    if start_date or end_date:
        match_query["created_at"] = {}

        if start_date:
            # Convert string to datetime and set to start of day (00:00:00)
            start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            match_query["created_at"]["$gte"] = start_datetime

        if end_date:
            # Convert string to datetime and set to end of day (23:59:59)
            end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            match_query["created_at"]["$lt"] = end_datetime

    # Start with match stage if we have date filters
    query = []
    if match_query:
        query.append({"$match": match_query})

    # Add the rest of the pipeline
    query.extend(
        [
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_info",
                }
            },
            {
                "$unwind": {
                    "path": "$created_by_info",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$addFields": {
                    "created_at": {
                        "$dateToString": {
                            "format": "%Y-%m-%d %H:%M:%S",
                            "date": {"$add": ["$created_at", IST_OFFSET]},
                        }
                    },
                    "updated_at": {
                        "$dateToString": {
                            "format": "%Y-%m-%d %H:%M:%S",
                            "date": {"$add": ["$updated_at", IST_OFFSET]},
                        }
                    },
                    "updates": {
                        "$map": {
                            "input": {"$ifNull": ["$updates", []]},
                            "as": "update",
                            "in": {
                                "$mergeObjects": [
                                    "$$update",
                                    {
                                        "created_at": {
                                            "$dateToString": {
                                                "format": "%Y-%m-%d %H:%M:%S",
                                                "date": {
                                                    "$add": [
                                                        "$$update.created_at",
                                                        IST_OFFSET,
                                                    ]
                                                },
                                            }
                                        },
                                        "updated_at": {
                                            "$dateToString": {
                                                "format": "%Y-%m-%d %H:%M:%S",
                                                "date": {
                                                    "$add": [
                                                        "$$update.updated_at",
                                                        IST_OFFSET,
                                                    ]
                                                },
                                            }
                                        },
                                    },
                                ]
                            },
                        }
                    },
                }
            },
            {"$sort": {"created_at": -1}},
        ]
    )

    # Fetch matching daily visits
    daily_visits_cursor = db.daily_visits.aggregate(query)
    daily_visits = [serialize_mongo_document(doc) for doc in daily_visits_cursor]

    # Create an Excel workbook using openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Daily Visits Report"

    # Find the maximum number of updates across all visits
    max_updates = 0
    for dv in daily_visits:
        updates_count = len(dv.get("updates", []))
        if updates_count > max_updates:
            max_updates = updates_count

    # Define fields to exclude from update columns
    keys_to_exclude = {
        "_id",
        # "uploaded_by",
        "images",
        "customer_id",
        # Exclude all potential customer fields as requested
        "potential_customer",
        "potential_customer_name",
        "potential_customer_address",
        "potential_customer_tier",
    }

    # Collect all possible update keys for dynamic columns
    update_keys = set()
    for dv in daily_visits:
        for update in dv.get("updates", []):
            # Only add keys that aren't in the exclude list
            update_keys.update([k for k in update.keys() if k not in keys_to_exclude])

    # Define the base header row
    base_headers = ["Created By", "Selfie", "Created At", "Updated At"]

    # Create dynamic headers for each update
    headers = base_headers.copy()
    for i in range(max_updates):
        # Add a single "Customer" column that will contain either customer_name or "Potential Customer"
        headers.append(f"Update {i+1} - Customer")

        # Add the rest of the columns
        for key in sorted(update_keys):
            headers.append(f"Update {i+1} - {key}")

    ws.append(headers)

    # Add data rows
    for dv in daily_visits:
        # Base row data
        row = [
            dv.get("created_by_info", {}).get("name", ""),
            dv.get("selfie", ""),
            dv.get("created_at", ""),
            dv.get("updated_at", ""),
        ]

        # Add update data
        updates = dv.get("updates", [])
        for i in range(max_updates):
            if i < len(updates):
                update = updates[i]

                # Handle customer column - use "Potential Customer" if potential_customer is True,
                # otherwise use customer_name
                if update.get("potential_customer") is True:
                    row.append("Potential Customer")
                else:
                    row.append(update.get("customer_name", ""))

                # Add other fields
                for key in sorted(update_keys):
                    row.append(
                        str(update.get(key, "")) if update.get(key) is not None else ""
                    )
            else:
                # Fill empty cells for missing updates
                # +1 for the customer column
                for _ in range(len(update_keys) + 1):
                    row.append("")

        ws.append(row)

    # Auto-adjust column width for better readability
    for column in ws.columns:
        max_length = 0
        column_letter = openpyxl.utils.get_column_letter(column[0].column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2) if max_length < 50 else 50
        ws.column_dimensions[column_letter].width = adjusted_width

    # Save the workbook to a binary stream
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    # Add date range to filename if dates are selected
    filename = "daily_visits_report"
    if start_date and end_date:
        filename += f"_{start_date}_to_{end_date}"
    elif start_date:
        filename += f"_from_{start_date}"
    elif end_date:
        filename += f"_until_{end_date}"
    filename += ".xlsx"

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.delete("/{daily_visit_id}")
def delete_daily_visit(daily_visit_id: str):
    """
    Delete a daily_visit by its ID.
    This example performs a hard delete.
    For a soft delete (mark as inactive), you can update the document instead.
    """
    try:
        doc = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)},
            {"$set": {"is_active": not doc.get("is_active")}},
        )
        if result.modified_count == 1:
            return {"detail": "Catalogue deleted successfully (soft delete)"}
        else:
            raise HTTPException(status_code=404, detail="Catalogue not found")

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("")
def create_daily_visit(daily_visits: dict):
    """
    Update the catalogue with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in daily_visits.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        result = db.daily_visits.insert_one(
            {**update_data, "created_at": datetime.datetime.now()}
        )

        if result:
            # Fetch and return the updated document.
            template = db.templates.find_one({"name": "update_notification_1"})
            notify_all_salespeople(db, template, {})
            return "Document Created"
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Announcement not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{daily_visit_id}")
def update_daily_visit(daily_visit_id: str, daily_visit: dict):
    """
    Update the daily_visit with the provided fields.
    Only the fields sent in the request will be updated.
    """
    try:
        # Build a dictionary of fields to update (skip any that are None)
        update_data = {k: v for k, v in daily_visit.items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)}, {"$set": update_data}
        )

        if result.modified_count == 1:
            # Fetch and return the updated document.
            updated_catalogue = db.daily_visits.find_one(
                {"_id": ObjectId(daily_visit_id)}
            )
            return serialize_mongo_document(updated_catalogue)
        else:
            # It’s possible that the document was not found or that no changes were made.
            raise HTTPException(
                status_code=404, detail="Announcement not found or no changes applied"
            )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
