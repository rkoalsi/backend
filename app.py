from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from helpers import process_upload, validate_file
import logging, uvicorn, threading
from db import connect_to_mongo, disconnect_on_exit, parse_data

# Set up logging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.propagate = False

app = FastAPI()
client, db = connect_to_mongo()


@app.get("/")
def index():
    users = parse_data(db.users.find())
    return {"users": users}


@app.get("/hello")
def hello_world():
    return {"data": "Hello, World!"}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...), email: str = Form(...)):
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")

    if file.filename == "":
        raise HTTPException(status_code=400, detail="No selected file")

    # Validate file
    validation_result = validate_file(file)
    status = validation_result.get("status")
    message = validation_result.get("message")
    if status == "error":
        raise HTTPException(
            status_code=400, detail=f"Error in file uploaded, {message}"
        )

    try:
        # Start processing in a separate thread
        threading.Thread(target=process_upload, args=(file, email)).start()

        # Return a response immediately
        return {
            "message": f"Processing started.\nAn email will be sent to {email} once the task is completed."
        }

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")


@app.get("/download")
def download():
    name = "Template.xlsx"
    try:
        return FileResponse(
            name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=name,
        )
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1000)
    app.add_event_handler("shutdown", disconnect_on_exit(client))
