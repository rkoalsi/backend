from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from bson import ObjectId
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA  # Note: This is SHA1, not SHA256
from Crypto.PublicKey import RSA
import base64
import http.client
import urllib.parse
from pathlib import Path

load_dotenv()

router = APIRouter()

client, pupscribe_db = connect_to_mongo()

attendance_db = client.get_database("attendance")
employees_collection = attendance_db.get_collection("employees")
attendance_collection = attendance_db.get_collection("attendance")
device_collection = attendance_db.get_collection("devices")
BASE_DIR = (
    Path(__file__).resolve().parent.parent
)  # Get the directory of the current script
CERTIFICATE = BASE_DIR / "certificate.pem"  # Adjust based on where the file is stored


def send_attendance_to_greythr(door, employee_number, is_in=True):
    """
    Sends attendance data to GreyTHR API.

    Args:
        employee_number (str): The employee number
        is_in (bool): True for check-in, False for check-out

    Returns:
        tuple: (success, message)
    """
    try:
        # Configuration
        gthost = "pupscribe.greythr.com"
        gtapiid = "fd121436-d9c9-401b-bb8b-8dbbdac445b3"
        Attendpoint = "/v2/attendance/asca/swipes"

        # Current time in IST format (UTC+5:30)
        now_utc = datetime.now()
        now_ist = now_utc + timedelta(hours=5, minutes=30)
        timestamp = now_ist.strftime("%Y-%m-%dT%H:%M:%S.654+05:30")

        # Create single swipe data (1 for in, 0 for out)
        swipe_type = "1" if is_in else "0"
        swipes = f"{timestamp},{employee_number},{door},{swipe_type}"

        # Load private key
        try:
            with open(CERTIFICATE, "r") as f:
                key = RSA.importKey(f.read())
        except Exception as e:
            return False, f"Failed to load certificate: {str(e)}"

        # Generate signature (SHA1)
        h = SHA.new(swipes.encode("ascii"))
        signer = PKCS1_v1_5.new(key)
        signature = signer.sign(h)

        # Base64 encode and URL encode components separately
        gtsign = base64.b64encode(signature).decode("utf-8")
        encoded_swipes = urllib.parse.quote(swipes, safe="")
        encoded_sign = urllib.parse.quote(gtsign, safe="")

        # Build payload string manually
        payload = f"id={gtapiid}&swipes={encoded_swipes}&sign={encoded_sign}"

        # Create connection and headers
        conn = http.client.HTTPSConnection(gthost)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
        }

        # Make request
        conn.request("POST", Attendpoint, body=payload, headers=headers)
        res = conn.getresponse()
        data = res.read()
        response_text = data.decode("utf-8")

        # Handle response
        if res.status != 200 or response_text:
            return False, f"Error ({res.status}): {response_text}"
        else:
            return True, "Swipe recorded successfully"

    except Exception as e:
        return False, f"Failed to send attendance: {str(e)}"


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
                swipe_datetime = datetime.strptime(
                    swipe_datetime_str, "%d-%m-%Y %H:%M:%S"
                )

                # Fetch employee details
                employee = employees_collection.find_one({"phone": int(mobile)})
                if not employee:
                    return JSONResponse(
                        content={"error": "Employee not found"}, status_code=404
                    )

                employee_id = str(employee["_id"])
                employee_name = employee["name"]
                employee_number = employee["employee_number"]
                device_id = employee.get("device_id", "Unknown")
                device = device_collection.find_one({"_id": ObjectId(device_id)})
                print(f"Employee ID: {employee_id}, Name: {employee_name}")

                # Check if employee has already swiped in today
                today_start = swipe_datetime.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                today_end = today_start + timedelta(days=1)

                # Find today's attendance records for this employee
                today_records = list(
                    attendance_collection.find(
                        {
                            "employee_id": ObjectId(employee_id),
                            "swipe_datetime": {"$gte": today_start, "$lt": today_end},
                        }
                    ).sort("swipe_datetime", 1)
                )  # Sort by time ascending

                # Determine if this is a check-in or check-out
                is_check_in = (
                    len(today_records) % 2 == 0
                )  # Even count means this is a check-in

                # Insert attendance record locally
                attendance_record = {
                    "employee_id": ObjectId(employee_id),
                    "employee_name": employee_name,
                    "employee_number": employee_number,
                    "swipe_datetime": swipe_datetime,
                    "device_name": (
                        ObjectId(device_id)
                        if isinstance(device_id, str) and len(device_id) == 24
                        else device_id
                    ),
                    "created_at": datetime.now(),
                    "is_check_in": is_check_in,
                }
                attendance_collection.insert_one(attendance_record)

                # Send to GreyTHR
                success, message = send_attendance_to_greythr(
                    door=device.get("name"),
                    employee_number=employee_number,
                    is_in=is_check_in,
                )

                return JSONResponse(
                    content={
                        "message": "Attendance recorded",
                        "employee": serialize_mongo_document(employee),
                        "greythr_success": success,
                        "greythr_message": message,
                        "is_check_in": is_check_in,
                    },
                    status_code=201,
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


@router.post("/check_in")
async def check_in(data: dict):
    try:
        print(data)
        # Fetch employee details using userId
        user_collection = pupscribe_db.get_collection("users")
        employee = user_collection.find_one({"_id": ObjectId(data.get("user_id"))})
        attendance_employee = employees_collection.find_one(
            {"phone": int(data.get("phone"))}
        )
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        employee_id = str(attendance_employee["_id"])
        employee_name = attendance_employee["name"]
        employee_number = attendance_employee["employee_number"]

        # Determine if it's check-in or check-out
        is_check_in = data.get("action") == "checkin"

        # Record attendance
        swipe_datetime = datetime.utcnow()
        attendance_record = {
            "employee_id": ObjectId(employee_id),
            "employee_name": employee_name,
            "employee_number": employee_number,
            "swipe_datetime": swipe_datetime,
            "device_name": "Pupscribe Order Form",
            "created_at": datetime.now(),
            "is_check_in": is_check_in,
        }
        attendance_collection.insert_one(attendance_record)

        # Send data to GreyTHR
        success, message = send_attendance_to_greythr(
            door="Pupscribe Order Form",
            employee_number=employee_number,
            is_in=is_check_in,
        )
        print(
            {
                "message": "Attendance recorded",
                "employee": {"id": employee_id, "name": employee_name},
                "greythr_success": success,
                "greythr_message": message,
                "is_check_in": is_check_in,
            }
        )
        return {
            "message": "Attendance recorded",
            "employee": {"id": employee_id, "name": employee_name},
            "greythr_success": success,
            "greythr_message": message,
            "is_check_in": is_check_in,
        }

    except Exception as e:
        print(e)
        return {"error": "Database error", "details": str(e), "status_code": 500}


@router.get("/status")
async def check_attendance_status(phone: str):
    try:
        now = datetime.now()  # Consider timezone if necessary
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

        employee = employees_collection.find_one({"phone": int(phone)})
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        # Find the latest attendance record for today
        record = attendance_collection.find_one(
            {
                "employee_id": ObjectId(employee["_id"]),
                "swipe_datetime": {"$gte": start_of_day, "$lte": end_of_day},
            },
            sort=[("swipe_datetime", -1)],
        )

        checked_in = record.get("is_check_in", False) if record else False
        return {"checked_in": checked_in}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database error")


from datetime import datetime, timedelta


@router.get("/employee_attendance")
async def get_attendance(phone: str):
    try:
        # Find the employee - this returns a cursor, not a document
        employee_cursor = employees_collection.find({"phone": int(phone)})
        # Convert cursor to list and get first document (if exists)
        employee_list = list(employee_cursor)

        if not employee_list:
            raise HTTPException(status_code=404, detail="Employee not found")

        # Get the first (and presumably only) employee that matches
        employee = serialize_mongo_document(employee_list[0])

        # Now query attendance using the employee's ObjectId
        all_attendance = attendance_collection.find(
            {"employee_id": ObjectId(employee["_id"])},
        ).sort("created_at", -1)

        # Convert cursor to a list of documents
        attendance_list = list(all_attendance)
        serialized_attendance = []

        for doc in attendance_list:
            serialized_doc = serialize_mongo_document(doc)

            # Convert created_at from UTC to IST if it exists
            if "created_at" in serialized_doc and serialized_doc["created_at"]:
                # Check if created_at is already a datetime object
                if isinstance(serialized_doc["created_at"], datetime):
                    utc_time = serialized_doc["created_at"]
                else:
                    # If it's a string, parse it to datetime
                    # Adjust the format string as needed for your actual date format
                    try:
                        # If the format is ISO
                        utc_time = datetime.fromisoformat(
                            serialized_doc["created_at"].replace("Z", "+00:00")
                        )
                    except:
                        try:
                            # Common MongoDB date string format
                            utc_time = datetime.strptime(
                                serialized_doc["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                            )
                        except:
                            # If parsing fails, keep the original
                            serialized_attendance.append(serialized_doc)
                            continue

                # Add 5 hours and 30 minutes to convert from UTC to IST
                ist_time = utc_time + timedelta(hours=5, minutes=30)
                # Convert back to the same format as the original
                if isinstance(serialized_doc["created_at"], str):
                    serialized_doc["created_at"] = ist_time.isoformat()
                else:
                    serialized_doc["created_at"] = ist_time

            serialized_attendance.append(serialized_doc)

        return {"attendance": serialized_attendance}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database error")
