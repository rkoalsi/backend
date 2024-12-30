from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import HTMLResponse
from backend.config.root import connect_to_mongo, disconnect_on_exit, parse_data  # type: ignore
from .helpers import validate_file, process_upload, get_access_token

router = APIRouter()

client, db = connect_to_mongo()


@router.get("/")
def get_customers():
    customers = parse_data(db.customers.find().limit(10))
    return {"customers": customers}


# @router.get("/", response_class=HTMLResponse)
# def index():
#     return "<h1>Backend is running<h1>"
