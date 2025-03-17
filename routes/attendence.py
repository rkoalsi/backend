from fastapi import APIRouter, Request
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from dotenv import load_dotenv
from fastapi.responses import JSONResponse

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()


@router.get("/in_and_out")
def in_and_out(request: Request):
    text = request.query_params.get("text")  # Get 'text' from query params
    if text:
        print(f"Received text: {text}")  # Print to console as well
    else:
        print("No 'text' query parameter received.")

    return JSONResponse(content={"message": "Request Received"}, status_code=200)
