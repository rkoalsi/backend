from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
import mysql.connector
from dotenv import load_dotenv
import os

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
    name = request.query_params.get("name")  # Get 'text' from query params
    mobile = request.query_params.get("mobile")  # Get 'text' from query params
    date = request.query_params.get("date")  # Get 'text' from query params
    if not mobile:
        return JSONResponse(
            content={"error": "No 'mobile' query parameter received"}, status_code=400
        )

    print(f"Received mobile: {mobile}")

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Step 1: Fetch employee details using mobile_number
        cursor.execute(
            "SELECT id, name FROM employees WHERE mobile_number = %s", (mobile,)
        )
        employee = cursor.fetchone()

        if not employee:
            return JSONResponse(
                content={"error": "Employee not found"}, status_code=404
            )

        employee_id = employee["id"]
        employee_name = employee["name"]
        employee_number = employee["name"]
        device_name = employee["device_id"]
        device_name = employee["device_id"]
        print(f"Employee ID: {employee_id}, Name: {employee['name']}")

        # Step 2: Insert attendance record
        cursor.execute(
            "INSERT INTO attendance (employee_id,employee_name, employee_number, swipe_datetime, device_name, created_at) VALUES (%s, NOW())",
            (
                employee_id,
                employee_name,
                employee_number,
                date,
                device_name,
            ),
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
        cursor.close()
        db.close()
