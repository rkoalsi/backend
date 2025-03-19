from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from datetime import datetime
from dotenv import load_dotenv
import re
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson import ObjectId
from datetime import datetime

load_dotenv()

router = APIRouter()

client, _ = connect_to_mongo()


@router.get("/in_and_out")
def in_and_out(request: Request):
    text = request.query_params.get("text")  # Get 'text' from query params
    if text:
        print(f"Received text: {text}")  # Print to console as well
        pattern = r"Dear Sir,?\s*([\w\s]+)\s+(\d+)\s+has punched attendance on\s+([\d-]+\s+[\d:]+)"
        match = re.search(pattern, text)

        if match:
            name = match.group(1).strip()
            mobile = match.group(2).strip()
            swipe_datetime_str = match.group(3).strip()
            print(f"Name: {name}, Mobile: {mobile}, DateTime: {swipe_datetime_str}")

            try:
                # Convert string to datetime
                swipe_datetime = datetime.strptime(
                    swipe_datetime_str, "%d-%m-%Y %H:%M:%S"
                )

                db = client.get_database("attendance")
                employees_collection = db.get_collection("employees")
                attendance_collection = db.get_collection("attendance")

                # Fetch employee details
                employee = employees_collection.find_one({"phone": mobile})
                if not employee:
                    return JSONResponse(
                        content={"error": "Employee not found"}, status_code=404
                    )

                employee_id = str(employee["_id"])
                employee_name = employee["name"]
                device_id = employee.get("device_id", "Unknown")

                print(f"Employee ID: {employee_id}, Name: {employee_name}")

                # Insert attendance record
                attendance_record = {
                    "employee_id": ObjectId(employee_id),
                    "employee_name": employee_name,
                    "employee_number": employee_name,
                    "swipe_datetime": swipe_datetime,  # Now in datetime format
                    "device_name": ObjectId(device_id),
                    "created_at": datetime.utcnow(),
                }
                attendance_collection.insert_one(attendance_record)

                return JSONResponse(
                    content={
                        "message": "Attendance recorded",
                        "employee": serialize_mongo_document(employee),
                    },
                    status_code=201,
                )

            except ValueError:
                return JSONResponse(
                    content={"error": "Invalid date format"},
                    status_code=400,
                )

            except Exception as e:
                print(f"Error: {str(e)}")
                return JSONResponse(
                    content={"error": "Database error", "details": str(e)},
                    status_code=500,
                )

            finally:
                pass
        else:
            print("No match found.")

    else:
        print("No 'text' query parameter received.")

    return JSONResponse(content={"message": "Request Received"}, status_code=200)
