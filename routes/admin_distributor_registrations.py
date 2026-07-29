from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from bson import ObjectId
from ..config.root import get_database, serialize_mongo_document
from .distributor_registrations import CARD_ICONS, CARD_ACCENTS, DEFAULT_CARDS
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
db = get_database()
distributor_registrations_collection = db["distributor_registrations"]
page_cards_collection = db["distributor_page_cards"]

STATUSES = ("not_contacted", "contacted", "onboarded", "declined")


class UpdateDistributorRegistrationRequest(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None


@router.get("")
def get_distributor_registrations(
    page: int = Query(0, ge=0, description="0-based page index"),
    limit: int = Query(10, ge=1, description="Number of items per page"),
    search: Optional[str] = Query(
        None, description="Search by company, brand, contact person, email or phone"
    ),
    status: Optional[str] = Query(None, description="Filter by follow-up status"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    state: Optional[str] = Query(None, description="Filter by distribution state"),
):
    try:
        match_statement: dict = {}
        if search and search.strip():
            term = search.strip()
            match_statement["$or"] = [
                {"company_name": {"$regex": term, "$options": "i"}},
                {"brand_name": {"$regex": term, "$options": "i"}},
                {"contact_person_name": {"$regex": term, "$options": "i"}},
                {"email": {"$regex": term, "$options": "i"}},
                {"phone": {"$regex": term, "$options": "i"}},
            ]
        if status:
            match_statement["status"] = status
        if category:
            match_statement["categories"] = category
        if state:
            match_statement["distribution_states"] = state

        pipeline = [
            {"$match": match_statement},
            {"$sort": {"created_at": -1}},
            {"$skip": page * limit},
            {"$limit": limit},
        ]
        total_count = distributor_registrations_collection.count_documents(
            match_statement
        )
        cursor = distributor_registrations_collection.aggregate(pipeline)
        registrations = [serialize_mongo_document(doc) for doc in cursor]
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        if page > total_pages and total_pages != 0:
            raise HTTPException(status_code=400, detail="Page number out of range")
        return {
            "distributor_registrations": registrations,
            "total_count": total_count,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ── Page cards ────────────────────────────────────────────────────────────────
# The "who we are looking for" tiles on the public /distributors page. Kept in
# the database so the categories we are recruiting for can change without a
# deploy — both the marketplace and pupscribe.in read them from
# GET /api/distributor_registrations/cards.
#
# Routed under /cards before the /{registration_id} handlers so "cards" is never
# swallowed as an ObjectId.


class PageCardRequest(BaseModel):
    title: str
    text: str = ""
    icon: str = "pets"
    accent: str = "indigo"
    order: Optional[int] = None
    active: bool = True


def _validate_card(body: PageCardRequest):
    if not body.title.strip():
        raise HTTPException(status_code=400, detail="Title is required")
    if body.icon not in CARD_ICONS:
        raise HTTPException(status_code=400, detail=f"Icon must be one of: {', '.join(CARD_ICONS)}")
    if body.accent not in CARD_ACCENTS:
        raise HTTPException(status_code=400, detail=f"Accent must be one of: {', '.join(CARD_ACCENTS)}")


@router.get("/cards")
def list_page_cards():
    try:
        cursor = page_cards_collection.find({}).sort("order", 1)
        cards = [serialize_mongo_document(doc) for doc in cursor]
        return {"cards": cards, "icons": CARD_ICONS, "accents": CARD_ACCENTS}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/cards/seed")
def seed_page_cards():
    """Copy the built-in defaults into the collection so an admin has something
    to edit instead of starting from a blank list. No-op once cards exist."""
    try:
        if page_cards_collection.count_documents({}, limit=1):
            raise HTTPException(status_code=400, detail="Cards already configured")
        page_cards_collection.insert_many(
            [{**card, "order": i, "active": True} for i, card in enumerate(DEFAULT_CARDS)]
        )
        return {"success": True, "inserted": len(DEFAULT_CARDS)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/cards")
def create_page_card(body: PageCardRequest):
    try:
        _validate_card(body)
        order = body.order
        if order is None:
            last = page_cards_collection.find_one(sort=[("order", -1)])
            order = (last.get("order", -1) + 1) if last else 0
        doc = {
            "title": body.title.strip(),
            "text": body.text.strip(),
            "icon": body.icon,
            "accent": body.accent,
            "order": order,
            "active": body.active,
        }
        result = page_cards_collection.insert_one(doc)
        return {"success": True, "id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.put("/cards/{card_id}")
def update_page_card(card_id: str, body: PageCardRequest):
    try:
        _validate_card(body)
        update = {
            "title": body.title.strip(),
            "text": body.text.strip(),
            "icon": body.icon,
            "accent": body.accent,
            "active": body.active,
        }
        if body.order is not None:
            update["order"] = body.order
        result = page_cards_collection.update_one(
            {"_id": ObjectId(card_id)}, {"$set": update}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Card not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/cards/{card_id}")
def delete_page_card(card_id: str):
    try:
        result = page_cards_collection.delete_one({"_id": ObjectId(card_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Card not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ── Applications ──────────────────────────────────────────────────────────────


@router.patch("/{registration_id}")
def update_distributor_registration(
    registration_id: str,
    body: UpdateDistributorRegistrationRequest,
):
    try:
        update_fields = {}
        if body.status is not None:
            if body.status not in STATUSES:
                raise HTTPException(status_code=400, detail="Invalid status value")
            update_fields["status"] = body.status
        if body.notes is not None:
            update_fields["notes"] = body.notes

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = distributor_registrations_collection.update_one(
            {"_id": ObjectId(registration_id)},
            {"$set": update_fields},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Registration not found")

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
