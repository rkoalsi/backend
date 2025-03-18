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


@router.get("/in_and_out")
def in_and_out(request: Request):
    name = request.query_params.get("name")
    mobile = request.query_params.get("mobile")
    date = request.query_params.get("date")

    if not mobile:
        return JSONResponse(
            content={"error": "No 'mobile' query parameter received"}, status_code=400
        )

    print(f"Received mobile: {mobile}")

    db = None
    cursor = None  # Initialize cursor to avoid UnboundLocalError

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Fetch employee details
        cursor.execute(
            "SELECT id, name, device_id FROM employees WHERE mobile_number = %s",
            (mobile,),
        )
        employee = cursor.fetchone()

        if not employee:
            return JSONResponse(
                content={"error": "Employee not found"}, status_code=404
            )

        employee_id = employee["id"]
        employee_name = employee["name"]
        employee_number = employee["name"]
        device_name = employee.get("device_id", "Unknown")  # Handle missing device_id

        print(f"Employee ID: {employee_id}, Name: {employee_name}")

        # Insert attendance record
        cursor.execute(
            """
            INSERT INTO attendance (employee_id, employee_name, employee_number, swipe_datetime, device_name, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (employee_id, employee_name, employee_number, date, device_name),
        )
        db.commit()

        return JSONResponse(
            content={"message": "Attendance recorded", "employee": employee},
            status_code=201,
        )

    except Exception as e:
        print(f"Error: {str(e)}")
        return JSONResponse(
            content={"error": "Database error", "details": str(e)}, status_code=500
        )

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


@router.get("/in_and_out")
def in_and_out(request: Request):
    text = request.query_params.get("text")
    if not text:
        return JSONResponse(
            content={"error": "No 'mobile' query parameter received"}, status_code=400
        )
    # Regex pattern to extract <_name_>, <_mo_>, and <_date_>
    match = re.search(
        r"Dear Sir,\s*(.+?)\s+(\d+)\s+has punched attendance on\s+([\d-]+)\s+at", text
    )

    if not match:
        return JSONResponse(
            content={"error": "Invalid text format. Could not extract details."},
            status_code=400,
        )

    name = match.group(1)
    mobile = match.group(2)
    date = match.group(3)

    print(f"Extracted Name: {name}, Mobile: {mobile}, Date: {date}")

    print(f"Received mobile: {mobile}")

    db = None
    cursor = None  # Initialize cursor to avoid UnboundLocalError

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Fetch employee details
        cursor.execute(
            "SELECT id, name, device_id FROM employees WHERE mobile_number = %s",
            (mobile,),
        )
        employee = cursor.fetchone()

        if not employee:
            return JSONResponse(
                content={"error": "Employee not found"}, status_code=404
            )

        employee_id = employee["id"]
        employee_name = employee["name"]
        employee_number = employee["name"]
        device_name = employee.get("device_id", "Unknown")  # Handle missing device_id

        print(f"Employee ID: {employee_id}, Name: {employee_name}")

        # Insert attendance record
        cursor.execute(
            """
            INSERT INTO attendance (employee_id, employee_name, employee_number, swipe_datetime, device_name, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (employee_id, employee_name, employee_number, date, device_name),
        )
        db.commit()

        return JSONResponse(
            content={"message": "Attendance recorded", "employee": employee},
            status_code=201,
        )

    except Exception as e:
        print(f"Error: {str(e)}")
        return JSONResponse(
            content={"error": "Database error", "details": str(e)}, status_code=500
        )

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()
