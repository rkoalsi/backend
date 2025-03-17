from fastapi import APIRouter, BackgroundTasks
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from dotenv import load_dotenv
from fastapi.responses import JSONResponse

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()


@router.get("/in_and_out")
def in_and_out():
    print("Request Received")
    return JSONResponse(content={"message": "Request Received"}, status_code=200)
