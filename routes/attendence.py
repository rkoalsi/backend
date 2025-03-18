from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
import mysql.connector
from dotenv import load_dotenv
import os, re

load_dotenv()

router = APIRouter()


# Database connection function
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database="pupscribe",
    )


# @router.get("/in_and_out")
# def in_and_out(request: Request):
#     name = request.query_params.get("name")
#     mobile = request.query_params.get("mobile")
#     date = request.query_params.get("date")

#     if not mobile:
#         return JSONResponse(
#             content={"error": "No 'mobile' query parameter received"}, status_code=400
#         )

#     print(f"Received mobile: {mobile}")

#     db = None
#     cursor = None  # Initialize cursor to avoid UnboundLocalError

#     try:
#         db = get_db_connection()
#         cursor = db.cursor(dictionary=True)

#         # Fetch employee details
#         cursor.execute(
#             "SELECT id, name, device_id FROM employees WHERE mobile_number = %s",
#             (mobile,),
#         )
#         employee = cursor.fetchone()

#         if not employee:
#             return JSONResponse(
#                 content={"error": "Employee not found"}, status_code=404
#             )

#         employee_id = employee["id"]
#         employee_name = employee["name"]
#         employee_number = employee["name"]
#         device_name = employee.get("device_id", "Unknown")  # Handle missing device_id

#         print(f"Employee ID: {employee_id}, Name: {employee_name}")

#         # Insert attendance record
#         cursor.execute(
#             """
#             INSERT INTO attendance (employee_id, employee_name, employee_number, swipe_datetime, device_name, created_at)
#             VALUES (%s, %s, %s, %s, %s, NOW())
#             """,
#             (employee_id, employee_name, employee_number, date, device_name),
#         )
#         db.commit()

#         return JSONResponse(
#             content={"message": "Attendance recorded", "employee": employee},
#             status_code=201,
#         )

#     except Exception as e:
#         print(f"Error: {str(e)}")
#         return JSONResponse(
#             content={"error": "Database error", "details": str(e)}, status_code=500
#         )

#     finally:
#         if cursor:
#             cursor.close()
#         if db:
#             db.close()


@router.get("/in_and_out")
def in_and_out(request: Request):
    text = request.query_params.get("text")  # Get 'text' from query params
    if text:
        print(f"Received text: {text}")  # Print to console as well
        pattern = r"Dear Sir,?\s*([\w\s]+)\s+(\d+)\s+has punched attendance on\s+([\d-]+\s+[\d:]+)"
        match = re.search(pattern, text)

        if match:
            name = match.group(1)
            mobile = match.group(2)
            datetime = match.group(3)
            print(f"Name: {name}, Mobile: {mobile}, DateTime: {datetime}")
        else:
            print("No match found.")
        # name = match.group(1)
        # mobile = match.group(2)
        # date = match.group(3)
        # print(f"Received name: {name}\tmobile: {mobile}\tdate:{date}")
        # db = None
        # cursor = None  # Initialize cursor to avoid UnboundLocalError

        # try:
        #     db = get_db_connection()
        #     cursor = db.cursor(dictionary=True)

        #     # Fetch employee details
        #     cursor.execute(
        #         "SELECT id, name, device_id FROM employees WHERE mobile_number = %s",
        #         (mobile,),
        #     )
        #     employee = cursor.fetchone()

        #     if not employee:
        #         return JSONResponse(
        #             content={"error": "Employee not found"}, status_code=404
        #         )

        #     employee_id = employee["id"]
        #     employee_name = employee["name"]
        #     employee_number = employee["name"]
        #     device_name = employee.get(
        #         "device_id", "Unknown"
        #     )  # Handle missing device_id

        #     print(f"Employee ID: {employee_id}, Name: {employee_name}")

        #     # Insert attendance record
        #     cursor.execute(
        #         """
        #         INSERT INTO attendance (employee_id, employee_name, employee_number, swipe_datetime, device_name, created_at)
        #         VALUES (%s, %s, %s, %s, %s, NOW())
        #         """,
        #         (employee_id, employee_name, employee_number, date, device_name),
        #     )
        #     db.commit()

        #     return JSONResponse(
        #         content={"message": "Attendance recorded", "employee": employee},
        #         status_code=201,
        #     )

        # except Exception as e:
        #     print(f"Error: {str(e)}")
        #     return JSONResponse(
        #         content={"error": "Database error", "details": str(e)}, status_code=500
        #     )

        # finally:
        #     if cursor:
        #         cursor.close()
        #     if db:
        #         db.close()

    else:
        print("No 'text' query parameter received.")

    return JSONResponse(content={"message": "Request Received"}, status_code=200)
