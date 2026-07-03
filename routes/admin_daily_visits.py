from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ..config.root import get_database, serialize_mongo_document
from bson.objectid import ObjectId
from .helpers import notify_all_salespeople
from ..config.whatsapp import send_whatsapp
from .notifications import create_notification
from dotenv import load_dotenv
import math, datetime, io, openpyxl

load_dotenv()
router = APIRouter()

# Use shared database instance
db = get_database()

IST_OFFSET = 19800000


@router.post("/{daily_visit_id}/admin_comments")
async def add_admin_comment(daily_visit_id: str, request: Request):
    """
    Add an admin comment to a daily visit.
    Can be at visit level or shop level.
    Sends WhatsApp template to the salesperson who created the daily visit.
    """
    try:
        body = await request.json()
        comment_text = body.get("comment")
        admin_id = body.get("admin_id")
        admin_name = body.get("admin_name")
        shop_id = body.get("shop_id")  # Optional: if provided, comment is for specific shop

        if not comment_text:
            raise HTTPException(status_code=400, detail="Comment text is required")

        comment = {
            "_id": ObjectId(),
            "text": comment_text,
            "admin_id": ObjectId(admin_id) if admin_id else None,
            "admin_name": admin_name or "Admin",
            "created_at": datetime.datetime.now(),
            "shop_id": shop_id  # None for visit-level, shop id for shop-level
        }

        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)},
            {
                "$push": {"admin_comments": comment},
                "$set": {"updated_at": datetime.datetime.now()}
            }
        )

        if result.modified_count == 1:
            # Get the daily visit to find the creator
            daily_visit = db.daily_visits.find_one({"_id": ObjectId(daily_visit_id)})
            if daily_visit and daily_visit.get("created_by"):
                # Get the salesperson who created the daily visit
                salesperson = db.users.find_one({"_id": daily_visit.get("created_by")})

                # Get the template for admin comment notification
                template = db.templates.find_one({"name": "admin_comment_daily_visit"})

                if salesperson and template:
                    # send_whatsapp(
                    #     salesperson.get("phone"),
                    #     {**template},
                    #     {
                    #         "name": salesperson.get("first_name", ""),
                    #         "admin_name": admin_name or "Admin",
                    #         "button_url": f"{daily_visit_id}"
                    #     }
                    # )
                    # In-app notification to salesperson
                    create_notification(
                        db,
                        str(salesperson["_id"]),
                        "daily_visit_comment",
                        f"Comment on your daily visit",
                        f"{admin_name or 'Admin'} commented on your daily visit.",
                        f"/daily_visits/{daily_visit_id}",
                    )

            return JSONResponse(
                status_code=200,
                content={"message": "Comment added successfully", "comment_id": str(comment["_id"])}
            )
        else:
            raise HTTPException(status_code=404, detail="Daily visit not found")

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{daily_visit_id}/admin_comments/{comment_id}")
async def delete_admin_comment(daily_visit_id: str, comment_id: str):
    """
    Delete an admin comment from a daily visit.
    """
    try:
        result = db.daily_visits.update_one(
            {"_id": ObjectId(daily_visit_id)},
            {
                "$pull": {"admin_comments": {"_id": ObjectId(comment_id)}},
                "$set": {"updated_at": datetime.datetime.now()}
            }
        )

        if result.modified_count == 1:
            return JSONResponse(
                status_code=200,
                content={"message": "Comment deleted successfully"}
            )
        else:
            raise HTTPException(status_code=404, detail="Daily visit or comment not found")

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{daily_visit_id}/admin_comments/{comment_id}")
async def update_admin_comment(daily_visit_id: str, comment_id: str, request: Request):
    """
    Update an admin comment.
    """
    try:
        body = await request.json()
        comment_text = body.get("comment")

        if not comment_text:
            raise HTTPException(status_code=400, detail="Comment text is required")

        result = db.daily_visits.update_one(
            {
                "_id": ObjectId(daily_visit_id),
                "admin_comments._id": ObjectId(comment_id)
            },
            {
                "$set": {
                    "admin_comments.$.text": comment_text,
                    "admin_comments.$.updated_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now()
                }
            }
        )

        if result.modified_count == 1:
            return JSONResponse(
                status_code=200,
                content={"message": "Comment updated successfully"}
            )
        else:
            raise HTTPException(status_code=404, detail="Daily visit or comment not found")

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/{daily_visit_id}/admin_comments/{comment_id}/reply")
async def add_reply_to_comment(daily_visit_id: str, comment_id: str, request: Request):
    """
    Add a reply to an admin comment. Only one reply allowed per comment.
    Can be used by salesperson or admin.
    """
    try:
        body = await request.json()
        reply_text = body.get("reply")
        user_id = body.get("user_id")
        user_name = body.get("user_name")
        user_role = body.get("user_role", "salesperson")  # "admin" or "salesperson"

        if not reply_text:
            raise HTTPException(status_code=400, detail="Reply text is required")

        reply = {
            "_id": ObjectId(),
            "text": reply_text,
            "user_id": ObjectId(user_id) if user_id and user_id != "" else None,
            "user_name": user_name or "User",
            "user_role": user_role,
            "created_at": datetime.datetime.now(),
        }

        result = db.daily_visits.update_one(
            {
                "_id": ObjectId(daily_visit_id),
                "admin_comments._id": ObjectId(comment_id)
            },
            {
                "$set": {
                    "admin_comments.$.reply": reply,
                    "updated_at": datetime.datetime.now()
                }
            }
        )

        if result.matched_count == 1:
            return JSONResponse(
                status_code=200,
                content={"message": "Reply added successfully", "reply_id": str(reply["_id"])}
            )
        else:
            return JSONResponse(
                status_code=404,
                content={"error": "Daily visit or comment not found"}
            )

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/{daily_visit_id}/admin_comments/{comment_id}/reply")
async def update_reply(daily_visit_id: str, comment_id: str, request: Request):
    """
    Update a reply to a comment.
    """
    try:
        body = await request.json()
        reply_text = body.get("reply")

        if not reply_text:
            raise HTTPException(status_code=400, detail="Reply text is required")

        result = db.daily_visits.update_one(
            {
                "_id": ObjectId(daily_visit_id),
                "admin_comments._id": ObjectId(comment_id)
            },
            {
                "$set": {
                    "admin_comments.$.reply.text": reply_text,
                    "admin_comments.$.reply.updated_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now()
                }
            }
        )

        if result.modified_count == 1:
            return JSONResponse(
                status_code=200,
                content={"message": "Reply updated successfully"}
            )
        else:
            raise HTTPException(status_code=404, detail="Daily visit or comment not found")

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{daily_visit_id}/admin_comments/{comment_id}/reply")
async def delete_reply(daily_visit_id: str, comment_id: str):
    """
    Delete a reply from a comment.
    """
    try:
        result = db.daily_visits.update_one(
            {
                "_id": ObjectId(daily_visit_id),
                "admin_comments._id": ObjectId(comment_id)
            },
            {
                "$unset": {"admin_comments.$.reply": ""},
                "$set": {"updated_at": datetime.datetime.now()}
            }
        )

        if result.modified_count == 1:
            return JSONResponse(
                status_code=200,
                content={"message": "Reply deleted successfully"}
            )
        else:
            raise HTTPException(status_code=404, detail="Daily visit or comment not found")

    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/salespeople")
async def get_daily_visit_salespeople():
    """
    Return distinct salespeople (name + _id) who have created daily visits.
    Used to populate the salesperson filter dropdown.
    """
    try:
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "user_info",
                }
            },
            {"$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": False}},
            {
                "$group": {
                    "_id": "$user_info._id",
                    "name": {"$first": "$user_info.name"},
                }
            },
            {"$sort": {"name": 1}},
        ]
        results = list(db.daily_visits.aggregate(pipeline))
        salespeople = [
            {"_id": str(r["_id"]), "name": r.get("name", "N/A")}
            for r in results
            if r.get("name")
        ]
        return JSONResponse(status_code=200, content={"salespeople": salespeople})
    except Exception as e:
        print(str(e))
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("")
async def get_daily_visits(request: Request):
    page = int(request.query_params.get("page", 0))
    limit = int(request.query_params.get("limit", 25))
    skip = page * limit
    # Get date filter parameters
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")
    salesperson_name = request.query_params.get("salesperson_name")

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

    # Add salesperson filter if provided (look up by name)
    if salesperson_name:
        matched_users = list(
            db.users.find(
                {"name": {"$regex": salesperson_name, "$options": "i"}},
                {"_id": 1},
            )
        )
        user_ids = [u["_id"] for u in matched_users]
        filter_condition["created_by"] = {"$in": user_ids}

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
                "admin_comments": {
                    "$map": {
                        "input": {"$ifNull": ["$admin_comments", []]},
                        "as": "comment",
                        "in": {
                            "$mergeObjects": [
                                "$$comment",
                                {
                                    "created_at": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d %H:%M:%S",
                                            "date": {
                                                "$add": [
                                                    "$$comment.created_at",
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
        for dv in daily_visits:
            updates = dv.get("updates", [])
            created_by = dv.get("created_by", "")
            if len(updates) > 0:
                for update in updates:
                    potential_customer = update.get("potential_customer")
                    if not potential_customer:
                        customer_id = update.get("customer_id")
                        address_id = update.get("address", {"address_id": ""}).get(
                            "address_id"
                        )
                        shop_hooks = list(
                            db.shop_hooks.find(
                                {
                                    "customer_id": ObjectId(customer_id),
                                    "created_by": ObjectId(created_by),
                                    "customer_address.address_id": address_id,
                                }
                            ).sort({"created_at": -1})
                        )
                        update["shop_hooks"] = (
                            shop_hooks[0].get("hooks") if len(shop_hooks) > 0 else []
                        )
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


def _get_fy_info():
    """Return current FY start years and month/year info."""
    now = datetime.datetime.utcnow() + datetime.timedelta(seconds=IST_OFFSET / 1000)
    current_year = now.year
    current_month = now.month
    current_fy_start_year = current_year if current_month >= 4 else current_year - 1
    last_fy_start_year = current_fy_start_year - 1
    previous_fy_start_year = current_fy_start_year - 2
    return {
        "current_year": current_year,
        "current_month": current_month,
        "current_fy_start_year": current_fy_start_year,
        "last_fy_start_year": last_fy_start_year,
        "previous_fy_start_year": previous_fy_start_year,
        "current_month_name": now.strftime("%B %Y"),
    }


def _fetch_financial_data(customer_mongo_ids, fy):
    """
    Returns dict keyed by mongo customer _id string with financial metrics
    aggregated from the invoices collection.
    """
    if not customer_mongo_ids:
        return {}

    customers = list(
        db.customers.find(
            {"_id": {"$in": [ObjectId(cid) for cid in customer_mongo_ids]}},
            {"_id": 1, "contact_id": 1},
        )
    )
    contact_to_mongo = {c["contact_id"]: str(c["_id"]) for c in customers if c.get("contact_id")}
    contact_ids = list(contact_to_mongo.keys())
    if not contact_ids:
        return {}

    cy = fy["current_year"]
    cm = fy["current_month"]
    cfy = fy["current_fy_start_year"]
    lfy = fy["last_fy_start_year"]
    pfy = fy["previous_fy_start_year"]

    pipeline = [
        {"$match": {"customer_id": {"$in": contact_ids}}},
        {
            "$addFields": {
                "parsedYear": {"$year": {"$dateFromString": {"dateString": "$date"}}},
                "parsedMonth": {"$month": {"$dateFromString": {"dateString": "$date"}}},
            }
        },
        {
            "$addFields": {
                "isCurrentMonth": {
                    "$and": [{"$eq": ["$parsedYear", cy]}, {"$eq": ["$parsedMonth", cm]}]
                },
                "isCurrentFY": {
                    "$or": [
                        {"$and": [{"$eq": ["$parsedYear", cfy]}, {"$gte": ["$parsedMonth", 4]}]},
                        {"$and": [{"$eq": ["$parsedYear", cfy + 1]}, {"$lte": ["$parsedMonth", 3]}]},
                    ]
                },
                "isLastFY": {
                    "$or": [
                        {"$and": [{"$eq": ["$parsedYear", lfy]}, {"$gte": ["$parsedMonth", 4]}]},
                        {"$and": [{"$eq": ["$parsedYear", lfy + 1]}, {"$lte": ["$parsedMonth", 3]}]},
                    ]
                },
                "isPreviousFY": {
                    "$or": [
                        {"$and": [{"$eq": ["$parsedYear", pfy]}, {"$gte": ["$parsedMonth", 4]}]},
                        {"$and": [{"$eq": ["$parsedYear", pfy + 1]}, {"$lte": ["$parsedMonth", 3]}]},
                    ]
                },
                "isDue": {
                    "$not": {"$in": ["$status", ["void", "draft", "sent", "paid"]]}
                },
            }
        },
        {
            "$group": {
                "_id": "$customer_id",
                "currentMonthSale": {
                    "$sum": {
                        "$cond": [
                            "$isCurrentMonth",
                            {"$toDouble": {"$ifNull": ["$total", 0]}},
                            0,
                        ]
                    }
                },
                "currentFYSale": {
                    "$sum": {
                        "$cond": [
                            "$isCurrentFY",
                            {"$toDouble": {"$ifNull": ["$total", 0]}},
                            0,
                        ]
                    }
                },
                "lastFYSale": {
                    "$sum": {
                        "$cond": [
                            "$isLastFY",
                            {"$toDouble": {"$ifNull": ["$total", 0]}},
                            0,
                        ]
                    }
                },
                "previousFYSale": {
                    "$sum": {
                        "$cond": [
                            "$isPreviousFY",
                            {"$toDouble": {"$ifNull": ["$total", 0]}},
                            0,
                        ]
                    }
                },
                "outstandingBalance": {
                    "$sum": {
                        "$cond": [
                            "$isDue",
                            {"$toDouble": {"$ifNull": ["$balance", 0]}},
                            0,
                        ]
                    }
                },
            }
        },
    ]

    results = list(db.invoices.aggregate(pipeline, allowDiskUse=True))
    financial_data = {}
    for r in results:
        mongo_id = contact_to_mongo.get(r["_id"])
        if mongo_id:
            financial_data[mongo_id] = {
                "current_month_sale": round(r["currentMonthSale"], 2),
                "current_fy_sale": round(r["currentFYSale"], 2),
                "last_fy_sale": round(r["lastFYSale"], 2),
                "previous_fy_sale": round(r["previousFYSale"], 2),
                "outstanding_balance": round(r["outstandingBalance"], 2),
            }
    return financial_data


@router.get("/report")
def get_daily_visits_report(request: Request):
    start_date = request.query_params.get("start_date")
    end_date = request.query_params.get("end_date")
    salesperson_name = request.query_params.get("salesperson_name")

    match_query = {}
    if start_date or end_date:
        match_query["created_at"] = {}
        if start_date:
            match_query["created_at"]["$gte"] = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            match_query["created_at"]["$lt"] = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    if salesperson_name:
        matched_users = list(
            db.users.find({"name": {"$regex": salesperson_name, "$options": "i"}}, {"_id": 1})
        )
        match_query["created_by"] = {"$in": [u["_id"] for u in matched_users]}

    pipeline = []
    if match_query:
        pipeline.append({"$match": match_query})
    pipeline.extend(
        [
            {"$sort": {"created_at": 1}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by_info",
                }
            },
            {"$unwind": {"path": "$created_by_info", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "created_at_ist": {
                        "$dateToString": {
                            "format": "%Y-%m-%d %H:%M:%S",
                            "date": {"$add": ["$created_at", IST_OFFSET]},
                        }
                    },
                }
            },
        ]
    )

    daily_visits = [serialize_mongo_document(doc) for doc in db.daily_visits.aggregate(pipeline)]

    # Collect all unique customer mongo _ids for financial lookup
    all_customer_ids = set()
    for dv in daily_visits:
        for shop in dv.get("shops", []):
            cid = shop.get("customer_id")
            if cid and not shop.get("potential_customer"):
                all_customer_ids.add(cid)

    fy = _get_fy_info()
    financial_data = _fetch_financial_data(list(all_customer_ids), fy)

    # Dynamic FY label headers
    cfy = fy["current_fy_start_year"]
    lfy = fy["last_fy_start_year"]
    pfy = fy["previous_fy_start_year"]
    current_fy_label = f"{cfy}-{cfy + 1}"
    last_fy_label = f"{lfy}-{lfy + 1}"
    previous_fy_label = f"{pfy}-{pfy + 1}"

    headers = [
        "Created By",
        "Created Date & Time",
        "Customer Name",
        "Remarks",
        "Selfie",
        "Selfie Link",
        "Selfie Date & Time",
        "Shop Photo",
        "Shop Photo Link",
        "Shop Photo Date & Time",
        f"Current Month Sale ({fy['current_month_name']})",
        f"Current Year Sale ({current_fy_label})",
        f"Previous Year Sale ({last_fy_label})",
        f"Last Year Sale ({previous_fy_label})",
        "Outstanding Balance",
        "Final Remarks (For office Team)",
        "Has Update",
        "Update Text",
        "Update Date & Time",
    ]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Daily Visits Report"
    ws.append(headers)

    for dv in daily_visits:
        created_by_name = (dv.get("created_by_info") or {}).get("name", "")
        created_at = dv.get("created_at_ist", "")
        selfie_url = dv.get("selfie") or ""
        has_selfie = bool(selfie_url)

        # Build a map: customer_id/potential_customer_id → first update with images
        update_photo_map = {}
        # Build a map: customer_id/potential_customer_id → list of (text, time) for all updates
        update_text_map = {}
        for upd in dv.get("updates", []):
            images = upd.get("images") or []
            # updates.created_at is stored as a UTC string; convert to IST
            raw_upd_time = upd.get("created_at") or ""
            if raw_upd_time:
                try:
                    # stored as either "2026-06-15 HH:MM:SS..." or "2026-06-15T..."
                    normalized = str(raw_upd_time).replace("T", " ")[:19]
                    upd_dt = datetime.datetime.strptime(
                        normalized, "%Y-%m-%d %H:%M:%S"
                    ) + datetime.timedelta(seconds=IST_OFFSET / 1000)
                    upd_time = upd_dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    upd_time = str(raw_upd_time)
            else:
                upd_time = ""
            cid = upd.get("customer_id")
            pcid = upd.get("potential_customer_id")
            key = cid or pcid
            if not key:
                continue

            if images:
                first_image = images[0].get("url", "")
                if key not in update_photo_map:
                    update_photo_map[key] = (first_image, upd_time)

            upd_text = upd.get("text", "")
            if upd_text:
                update_text_map.setdefault(key, []).append((upd_text, upd_time))

        # Admin comments as final remarks (all comments on this visit)
        admin_comments = dv.get("admin_comments") or []
        final_remarks = "; ".join(
            c.get("text", "") for c in admin_comments if c.get("text")
        )

        for shop in dv.get("shops", []):
            is_potential = bool(shop.get("potential_customer"))
            if is_potential:
                customer_name = shop.get("potential_customer_name", "")
                customer_key = shop.get("potential_customer_id", "")
            else:
                customer_name = shop.get("customer_name", "")
                customer_key = shop.get("customer_id", "")

            remarks = shop.get("reason", "")

            photo_url, photo_time = update_photo_map.get(customer_key, ("", ""))
            has_photo = bool(photo_url)

            customer_updates = update_text_map.get(customer_key, [])
            has_update = bool(customer_updates)
            update_text = "; ".join(text for text, _ in customer_updates)
            update_time = customer_updates[-1][1] if customer_updates else ""

            if is_potential:
                fin = {}
            else:
                fin = financial_data.get(customer_key, {})

            row = [
                created_by_name,
                created_at,
                customer_name,
                remarks,
                has_selfie,
                selfie_url,
                created_at,  # selfie time = visit created_at
                has_photo,
                photo_url,
                photo_time,
                fin.get("current_month_sale", ""),
                fin.get("current_fy_sale", ""),
                fin.get("last_fy_sale", ""),
                fin.get("previous_fy_sale", ""),
                fin.get("outstanding_balance", ""),
                final_remarks,
                has_update,
                update_text,
                update_time,
            ]
            ws.append(row)

    # Style boolean columns
    bool_cols = [5, 8, 17]  # Selfie, Shop Photo, Has Update (1-indexed)
    for row in ws.iter_rows(min_row=2):
        for col_idx in bool_cols:
            cell = row[col_idx - 1]
            cell.value = "Yes" if cell.value is True else ("No" if cell.value is False else cell.value)

    # Auto-adjust column widths
    for col in ws.columns:
        col_letter = openpyxl.utils.get_column_letter(col[0].column)
        max_len = max(
            (len(str(cell.value)) for cell in col if cell.value is not None), default=10
        )
        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)

    filename = "daily_visits_report"
    if salesperson_name:
        filename += f"_{salesperson_name.replace(' ', '_')}"
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
            # template = db.templates.find_one({"name": "update_notification_1"})
            # notify_all_salespeople(db, template, {})
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
