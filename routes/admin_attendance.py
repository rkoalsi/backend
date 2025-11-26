from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from datetime import datetime, timedelta
from config.root import get_client, get_database, serialize_mongo_document
from bson import ObjectId
import pandas as pd
from io import BytesIO
from typing import Optional, Dict, Any, List
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, EmailStr, validator
from bson import ObjectId
from datetime import datetime
from typing import Optional
import re

router = APIRouter()

# Use shared client and database instances
client = get_client()
pupscribe_db = get_database()

attendance_db = client.get_database("attendance")
employees_collection = attendance_db.get_collection("employees")
attendance_collection = attendance_db.get_collection("attendance")
device_collection = attendance_db.get_collection("devices")

# Cache for device locations (will persist during app lifetime)
DEVICE_CACHE = {}


def serialize_objectid_document(doc):
    """
    Helper function to recursively convert ObjectIds to strings in MongoDB documents
    """
    if isinstance(doc, dict):
        return {key: serialize_objectid_document(value) for key, value in doc.items()}
    elif isinstance(doc, list):
        return [serialize_objectid_document(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc


def get_optimized_attendance_data(name: Optional[str] = None, limit: int = 1000, skip: int = 0):
    """
    Optimized function to get attendance data using aggregation pipeline
    """
    # Build the aggregation pipeline
    pipeline = []
    
    # Stage 1: Match employees by name if provided
    if name:
        pipeline.append({
            "$match": {
                "name": {"$regex": name, "$options": "i"}
            }
        })
    
    # Stage 2: Lookup attendance records for each employee
    pipeline.extend([
        {
            "$lookup": {
                "from": "attendance",
                "localField": "_id",
                "foreignField": "employee_id",
                "as": "attendance_records"
            }
        },
        # Stage 3: Only include employees with attendance records
        {
            "$match": {
                "attendance_records": {"$ne": []}
            }
        },
        # Stage 4: Unwind attendance records to process each individually
        {
            "$unwind": "$attendance_records"
        },
        # Stage 5: Sort by attendance swipe_datetime/created_at (most recent first)
        {
            "$sort": {
                "attendance_records.swipe_datetime": -1,
                "attendance_records.created_at": -1
            }
        },
        # Stage 6: Group back by employee to collect attendance records
        {
            "$group": {
                "_id": "$_id",
                "name": {"$first": "$name"},
                "phone": {"$first": "$phone"},
                "email": {"$first": "$email"},
                "employee_number": {"$last": "$attendance_records.employee_number"},  # Get latest employee_number
                "attendance_records": {"$push": "$attendance_records"},
                "total_records": {"$sum": 1}
            }
        },
        # Stage 7: Sort employees by name
        {
            "$sort": {"name": 1}
        },
        # Stage 8: Apply pagination
        {"$skip": skip},
        {"$limit": limit}
    ])
    
    # Execute aggregation and serialize ObjectIds
    result = list(employees_collection.aggregate(pipeline))
    serialized_result = [serialize_objectid_document(doc) for doc in result]
    
    # Get total count for pagination (without limit/skip)
    count_pipeline = pipeline[:-2]  # Remove skip and limit
    count_pipeline.append({"$count": "total"})
    total_count = list(employees_collection.aggregate(count_pipeline))
    total_employees = total_count[0]["total"] if total_count else 0
    
    return serialized_result, total_employees


def get_all_devices_cached():
    """
    Cache all devices on first call to avoid repeated queries
    """
    global DEVICE_CACHE
    
    if not DEVICE_CACHE:
        devices = list(device_collection.find({}))
        for device in devices:
            # Serialize the device document first
            serialized_device = serialize_objectid_document(device)
            
            # Cache by both ObjectId string and name
            device_id = serialized_device["_id"]  # Already converted to string
            device_name = serialized_device.get("name", "Unknown Location")
            
            DEVICE_CACHE[device_id] = device_name
            if "name" in serialized_device:
                DEVICE_CACHE[serialized_device["name"]] = device_name
    
    return DEVICE_CACHE


def parse_swipe_datetime(swipe_datetime):
    """
    Parse swipe_datetime which can be either a string or datetime object
    """
    if isinstance(swipe_datetime, datetime):
        return swipe_datetime
    elif isinstance(swipe_datetime, str):
        try:
            # Try parsing format like "19-03-2025 09:57:00"
            return datetime.strptime(swipe_datetime, "%d-%m-%Y %H:%M:%S")
        except ValueError:
            try:
                # Try ISO format
                return datetime.fromisoformat(swipe_datetime.replace("Z", "+00:00"))
            except ValueError:
                return None
    elif isinstance(swipe_datetime, dict) and "$date" in swipe_datetime:
        # Handle MongoDB date format
        try:
            return datetime.fromisoformat(swipe_datetime["$date"].replace("Z", "+00:00"))
        except:
            return None
    return None


def get_device_name_from_record(device_name_field, device_cache):
    """
    Get device name handling both ObjectId and string formats
    """
    if isinstance(device_name_field, str):
        # Check if it's an ObjectId string
        if len(device_name_field) == 24:
            return device_cache.get(device_name_field, "Unknown Location")
        else:
            # It's already a device name
            return device_name_field
    elif isinstance(device_name_field, dict) and "$oid" in device_name_field:
        # Handle ObjectId format from MongoDB
        device_id = device_name_field["$oid"]
        return device_cache.get(device_id, "Unknown Location")
    else:
        return "Unknown Location"


def group_attendance_by_date(attendance_records: List[Dict]) -> List[Dict]:
    """
    Group attendance records by date and determine check-in/check-out times
    """
    device_cache = get_all_devices_cached()
    
    # Group by date
    date_groups = {}
    
    for record in attendance_records:
        # Parse swipe_datetime
        swipe_time = parse_swipe_datetime(record.get("swipe_datetime"))
        if not swipe_time:
            # Fallback to created_at
            swipe_time = parse_swipe_datetime(record.get("created_at"))
        
        if not swipe_time:
            continue
            
        # Convert to IST
        ist_time = convert_utc_to_ist(swipe_time)
        date_key = ist_time.strftime("%Y-%m-%d")
        
        if date_key not in date_groups:
            date_groups[date_key] = []
        
        # Add parsed time and other info to record
        record_copy = record.copy()
        record_copy["parsed_swipe_time"] = ist_time
        record_copy["date"] = date_key
        
        date_groups[date_key].append(record_copy)
    
    # Process each date group
    processed_records = []
    
    for date_key, day_records in date_groups.items():
        # Sort by time
        day_records.sort(key=lambda x: x["parsed_swipe_time"])
        
        # Find check-in (first record with is_check_in=True or first record if no is_check_in field)
        check_in_record = None
        check_out_record = None
        
        for record in day_records:
            is_check_in = record.get("is_check_in", True)  # Default to True if not specified
            
            if is_check_in and check_in_record is None:
                check_in_record = record
            elif not is_check_in:
                check_out_record = record  # Keep updating to get the last check-out
        
        # If no explicit check-in found, use first record
        if check_in_record is None and day_records:
            check_in_record = day_records[0]
        
        # Create the processed record
        if check_in_record:
            # Get device location
            device_name = get_device_name_from_record(
                check_in_record.get("device_name"), device_cache
            )
            
            processed_record = {
                "_id": check_in_record.get("_id"),
                "employee_id": check_in_record.get("employee_id"),
                "employee_name": check_in_record.get("employee_name"),
                "employee_number": check_in_record.get("employee_number"),
                "date": date_key,
                "check_in_time": check_in_record["parsed_swipe_time"],
                "check_out_time": check_out_record["parsed_swipe_time"] if check_out_record else None,
                "location": device_name,
                "status": "Present" if check_in_record else "Absent",
                "total_records_for_day": len(day_records),
                "created_at": check_in_record.get("created_at")
            }
            
            processed_records.append(processed_record)
    
    # Sort by date (most recent first)
    processed_records.sort(key=lambda x: x["date"], reverse=True)
    
    return processed_records


@router.get("/employee_attendance")
async def get_all_attendance(
    name: Optional[str] = Query(None, description="Filter by employee name"),
    limit: int = Query(1000, description="Number of employees to return", ge=1, le=5000),
    skip: int = Query(0, description="Number of employees to skip", ge=0)
):
    try:
        # Pre-load and cache all devices
        device_cache = get_all_devices_cached()
        
        # Get optimized attendance data
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(get_optimized_attendance_data, name, limit, skip)
            attendance_data, total_employees = future.result()
        
        if not attendance_data:
            return {
                "attendance": [], 
                "message": "No employees found",
                "pagination": {
                    "total_employees": 0,
                    "limit": limit,
                    "skip": skip,
                    "has_more": False
                }
            }
        
        # Process attendance records
        all_attendance_data = []
        
        for employee_data in attendance_data:
            # Group attendance records by date and process check-in/check-out
            processed_records = group_attendance_by_date(employee_data["attendance_records"])
            
            # Build employee attendance object
            employee_attendance = {
                "employee": {
                    "id": employee_data["_id"],  # Already a string
                    "name": employee_data.get("name", ""),
                    "phone": employee_data.get("phone", ""),
                    "email": employee_data.get("email", ""),
                    "employee_number": employee_data.get("employee_number", "")
                },
                "attendance_records": processed_records,
                "total_records": len(processed_records)
            }
            
            all_attendance_data.append(employee_attendance)
        
        has_more = skip + len(all_attendance_data) < total_employees
        
        return {
            "attendance": all_attendance_data,
            "pagination": {
                "total_employees": total_employees,
                "returned_employees": len(all_attendance_data),
                "limit": limit,
                "skip": skip,
                "has_more": has_more
            },
            "filter_applied": name if name else "None"
        }
        
    except Exception as e:
        print(f"Error in get_all_attendance: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@router.get("/employee_attendance/download")
async def download_attendance_report(
    name: Optional[str] = Query(None, description="Filter by employee name"),
    max_records: int = Query(10000, description="Maximum records to export", ge=1, le=50000)
):
    try:
        
        # Get all attendance data for export (without pagination)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(get_optimized_attendance_data, name, max_records, 0)
            attendance_data, total_employees = future.result()
        
        if not attendance_data:
            raise HTTPException(status_code=404, detail="No employees found")
        
        # Prepare data for Excel efficiently
        excel_data = []
        
        for employee_data in attendance_data:
            employee_info = {
                "name": employee_data.get("name", ""),
                "phone": employee_data.get("phone", ""),
                "email": employee_data.get("email", ""),
                "employee_number": employee_data.get("employee_number", "")
            }
            
            # Group attendance records by date
            processed_records = group_attendance_by_date(employee_data["attendance_records"])
            
            for record in processed_records:
                # Format times for Excel
                check_in_time = ""
                check_out_time = "Not checked out"
                date = record.get("date", "")
                
                if record.get("check_in_time"):
                    check_in_time = record["check_in_time"].strftime("%H:%M:%S")
                
                if record.get("check_out_time"):
                    check_out_time = record["check_out_time"].strftime("%H:%M:%S")
                
                excel_data.append({
                    "Employee Name": employee_info["name"],
                    "Employee Number": employee_info["employee_number"],
                    "Phone": employee_info["phone"],
                    "Email": employee_info["email"],
                    "Date": date,
                    "Check In Time": check_in_time,
                    "Check Out Time": check_out_time,
                    "Status": record.get("status", ""),
                    "Location": record.get("location", "Unknown Location"),
                    "Total Records for Day": record.get("total_records_for_day", 0)
                })
        
        if not excel_data:
            raise HTTPException(status_code=404, detail="No attendance records found")
        
        # Create Excel file efficiently
        df = pd.DataFrame(excel_data)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Attendance Report', index=False)
            
            # Optimize column widths
            worksheet = writer.sheets['Attendance Report']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        output.seek(0)
        
        # Generate filename
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"attendance_report_{current_date}.xlsx"
        if name:
            # Sanitize filename
            safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"attendance_report_{safe_name}_{current_date}.xlsx"
        
        # Return Excel file
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"Error in download_attendance_report: {e}")
        raise HTTPException(status_code=500, detail="Error generating report")


@lru_cache(maxsize=1000)
def get_device_location_cached(device_id: str) -> str:
    """
    LRU cached version of device location lookup
    """
    device_cache = get_all_devices_cached()
    return device_cache.get(device_id, "Unknown Location")


def convert_utc_to_ist(timestamp):
    """
    Optimized UTC to IST conversion
    """
    try:
        if isinstance(timestamp, datetime):
            # Add 5 hours and 30 minutes to convert from UTC to IST
            return timestamp + timedelta(hours=5, minutes=30)
        
        # Handle string timestamps
        if isinstance(timestamp, str):
            try:
                # ISO format with Z
                if timestamp.endswith('Z'):
                    utc_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                else:
                    utc_time = datetime.fromisoformat(timestamp)
                
                return utc_time + timedelta(hours=5, minutes=30)
            except ValueError:
                try:
                    # MongoDB date string format
                    utc_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                    return utc_time + timedelta(hours=5, minutes=30)
                except ValueError:
                    # Try dd-mm-yyyy HH:MM:SS format
                    try:
                        utc_time = datetime.strptime(timestamp, "%d-%m-%Y %H:%M:%S")
                        return utc_time + timedelta(hours=5, minutes=30)
                    except ValueError:
                        return timestamp
        
        return timestamp
        
    except Exception as e:
        print(f"Error converting timestamp: {e}")
        return timestamp

class CreateEmployeeRequest(BaseModel):
    name: str
    phone: str
    email: EmailStr
    employee_number: Optional[str] = None
    department: Optional[str] = None
    designation: Optional[str] = None
    joining_date: Optional[str] = None
    
    @validator('name')
    def validate_name(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError('Name must be at least 2 characters long')
        return v.strip()
    
    @validator('phone')
    def validate_phone(cls, v):
        # Remove any non-digit characters
        phone_digits = re.sub(r'\D', '', v)
        # Check if it's a valid Indian phone number (10 digits)
        if len(phone_digits) != 10:
            raise ValueError('Phone number must be 10 digits')
        if not phone_digits.startswith(('6', '7', '8', '9')):
            raise ValueError('Invalid Indian phone number format')
        return phone_digits
    
    @validator('employee_number')
    def validate_employee_number(cls, v):
        if v:
            v = v.strip()
            if len(v) < 3:
                raise ValueError('Employee number must be at least 3 characters long')
        return v

class EmployeeResponse(BaseModel):
    id: str
    name: str
    phone: str
    email: str
    employee_number: Optional[str]
    department: Optional[str]
    designation: Optional[str]
    joining_date: Optional[str]


@router.get("/employees")
async def list_employees(
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    department: Optional[str] = None,
):
    """
    List all employees with optional filtering and pagination
    """
    try:
        # Build filter query
        filter_query = {}
        
        if search:
            filter_query["$or"] = [
                {"name": {"$regex": search, "$options": "i"}},
                {"email": {"$regex": search, "$options": "i"}},
                {"employee_number": {"$regex": search, "$options": "i"}}
            ]
        
        if department:
            filter_query["department"] = {"$regex": department, "$options": "i"}
        
        # Get total count
        total_count = employees_collection.count_documents(filter_query)
        
        # Get employees with pagination
        employees = list(employees_collection.find(filter_query)
                        .sort("name", 1)
                        .skip(skip)
                        .limit(limit))
        
        # Serialize response
        employee_list = []
        for emp in employees:
            employee_list.append({
                "id": str(emp["_id"]),
                "name": emp["name"],
                "phone": emp["phone"],
                "email": emp["email"],
                "employee_number": emp.get("employee_number"),
                "department": emp.get("department"),
                "designation": emp.get("designation"),
                "joining_date": emp.get("joining_date"),
                "status": emp.get("status", "active"),
                # "created_at": emp["created_at"].isoformat(),
                # "updated_at": emp["updated_at"].isoformat()
            })
        
        return {
            "employees": employee_list,
            "pagination": {
                "total": total_count,
                "skip": skip,
                "limit": limit,
                "has_more": skip + len(employee_list) < total_count
            }
        }
        
    except Exception as e:
        print(f"Error listing employees: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.post("/employees", response_model=EmployeeResponse)
async def create_employee(employee_data: CreateEmployeeRequest):
    """
    Create a new employee in the attendance system
    """
    try:
        # Check if employee with same email already exists
        existing_email = employees_collection.find_one({"email": employee_data.email})
        if existing_email:
            raise HTTPException(
                status_code=400, 
                detail="Employee with this email already exists"
            )
        
        # Check if employee with same phone already exists
        existing_phone = employees_collection.find_one({"phone": employee_data.phone})
        if existing_phone:
            raise HTTPException(
                status_code=400, 
                detail="Employee with this phone number already exists"
            )
        
        # Check if employee number is provided and unique
        if employee_data.employee_number:
            existing_emp_num = employees_collection.find_one({
                "employee_number": employee_data.employee_number
            })
            if existing_emp_num:
                raise HTTPException(
                    status_code=400, 
                    detail="Employee with this employee number already exists"
                )
        else:
            # Generate employee number if not provided
            # Get the highest existing employee number and increment
            last_employee = employees_collection.find_one(
                {"employee_number": {"$regex": "^EMP"}}, 
                sort=[("employee_number", -1)]
            )
            
            if last_employee and "employee_number" in last_employee:
                try:
                    last_num = int(last_employee["employee_number"].replace("EMP", ""))
                    new_emp_num = f"EMP{last_num + 1:04d}"
                except:
                    # If parsing fails, start from EMP0001
                    new_emp_num = "EMP0001"
            else:
                new_emp_num = "EMP0001"
            
            employee_data.employee_number = new_emp_num
        
        # Prepare employee document
        current_time = datetime.utcnow()
        employee_doc = {
            "name": employee_data.name,
            "phone": employee_data.phone,
            "email": employee_data.email,
            "employee_number": employee_data.employee_number,
            "department": employee_data.department,
            "designation": employee_data.designation,
            "joining_date": employee_data.joining_date,
            "status": "active",  # Default status
            "created_at": current_time,
            "updated_at": current_time
        }
        
        # Insert into database
        result = employees_collection.insert_one(employee_doc)
        
        if not result.inserted_id:
            raise HTTPException(
                status_code=500, 
                detail="Failed to create employee"
            )
        
        # Return the created employee
        created_employee = employees_collection.find_one({"_id": result.inserted_id})
        
        return EmployeeResponse(
            id=str(created_employee["_id"]),
            name=created_employee["name"],
            phone=created_employee["phone"],
            email=created_employee["email"],
            employee_number=created_employee.get("employee_number"),
            department=created_employee.get("department"),
            designation=created_employee.get("designation"),
            joining_date=created_employee.get("joining_date"),
            created_at=created_employee["created_at"].isoformat(),
            updated_at=created_employee["updated_at"].isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating employee: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Internal server error while creating employee"
        )

@router.get("/employees/{employee_id}", response_model=EmployeeResponse)
async def get_employee(employee_id: str):
    """
    Get a specific employee by ID
    """
    try:
        if not ObjectId.is_valid(employee_id):
            raise HTTPException(status_code=400, detail="Invalid employee ID format")
        
        employee = employees_collection.find_one({"_id": ObjectId(employee_id)})
        
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        return EmployeeResponse(
            id=str(employee["_id"]),
            name=employee["name"],
            phone=employee["phone"],
            email=employee["email"],
            employee_number=employee.get("employee_number"),
            department=employee.get("department"),
            designation=employee.get("designation"),
            joining_date=employee.get("joining_date"),
            created_at=employee["created_at"].isoformat(),
            updated_at=employee["updated_at"].isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching employee: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.put("/employees/{employee_id}", response_model=EmployeeResponse)
async def update_employee(employee_id: str, employee_data: CreateEmployeeRequest):
    """
    Update an existing employee
    """
    try:
        if not ObjectId.is_valid(employee_id):
            raise HTTPException(status_code=400, detail="Invalid employee ID format")
        print
        # Check if employee exists
        existing_employee = employees_collection.find_one({"_id": ObjectId(employee_id)})
        if not existing_employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Check for duplicate email (excluding current employee)
        existing_email = employees_collection.find_one({
            "email": employee_data.email,
            "_id": {"$ne": ObjectId(employee_id)}
        })
        if existing_email:
            raise HTTPException(
                status_code=400, 
                detail="Another employee with this email already exists"
            )
        
        # Check for duplicate phone (excluding current employee)
        existing_phone = employees_collection.find_one({
            "phone": employee_data.phone,
            "_id": {"$ne": ObjectId(employee_id)}
        })
        if existing_phone:
            raise HTTPException(
                status_code=400, 
                detail="Another employee with this phone number already exists"
            )
        
        # Check for duplicate employee number (excluding current employee)
        if employee_data.employee_number:
            existing_emp_num = employees_collection.find_one({
                "employee_number": employee_data.employee_number,
                "_id": {"$ne": ObjectId(employee_id)}
            })
            if existing_emp_num:
                raise HTTPException(
                    status_code=400, 
                    detail="Another employee with this employee number already exists"
                )
        
        # Update employee document
        update_doc = {
            "$set": {
                "name": employee_data.name,
                "phone": employee_data.phone,
                "email": employee_data.email,
                "employee_number": employee_data.employee_number or existing_employee.get("employee_number"),
                "department": employee_data.department,
                "designation": employee_data.designation,
                "joining_date": employee_data.joining_date,
                "updated_at": datetime.now()
            }
        }
        
        result = employees_collection.update_one(
            {"_id": ObjectId(employee_id)}, 
            update_doc
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=500, 
                detail="Failed to update employee"
            )
        
        # Return updated employee
        updated_employee = employees_collection.find_one({"_id": ObjectId(employee_id)})
        
        return EmployeeResponse(
            id=str(updated_employee["_id"]),
            name=updated_employee["name"],
            phone=updated_employee["phone"],
            email=updated_employee["email"],
            employee_number=updated_employee.get("employee_number"),
            department=updated_employee.get("department"),
            designation=updated_employee.get("designation"),
            joining_date=updated_employee.get("joining_date"),
            # created_at=updated_employee["created_at"].isoformat(),
            # updated_at=updated_employee["updated_at"].isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating employee: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.delete("/employees/{employee_id}")
async def delete_employee(employee_id: str):
    """
    Delete an employee (soft delete by setting status to inactive)
    """
    try:
        if not ObjectId.is_valid(employee_id):
            raise HTTPException(status_code=400, detail="Invalid employee ID format")
        
        # Check if employee exists
        existing_employee = employees_collection.find_one({"_id": ObjectId(employee_id)})
        if not existing_employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Soft delete by updating status
        result = employees_collection.delete_one(
            {"_id": ObjectId(employee_id)},
        )
        
        if result.deleted_count == 1:
            raise HTTPException(
                status_code=500, 
                detail="Failed to delete employee"
            )
        
        return {"message": "Employee deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting employee: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
