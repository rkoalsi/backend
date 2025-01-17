from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import os, requests

load_dotenv()

org_id = os.getenv("ORG_ID")

router = APIRouter()

client, db = connect_to_mongo()


@router.post("/estimate")
def estimate(data: dict):
    print(data)
    return "DB Updation with Zoho Routes"
