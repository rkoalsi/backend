from fastapi import APIRouter, HTTPException, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse, FileResponse

from .helpers import validate_file, process_upload
import threading, logging
from ..config.root import get_database, serialize_mongo_document
import httpx

router = APIRouter()

logger = logging.getLogger(__name__)
logger.propagate = False

db = get_database()

# Comprehensive list of Indian cities (fallback + primary source)
INDIAN_CITIES = [
  'Agra',
  'Ahmedabad',
  'Ajmer',
  'Aligarh',
  'Allahabad',
  'Amaravati',
  'Amravati',
  'Amritsar',
  'Asansol',
  'Aurangabad',
  'Bangalore',
  'Bareilly',
  'Belgaum',
  'Bhavnagar',
  'Bhilai',
  'Bhopal',
  'Bhubaneswar',
  'Bikaner',
  'Chandigarh',
  'Chennai',
  'Coimbatore',
  'Cuttack',
  'Dehradun',
  'Delhi',
  'Dewas',
  'Dhanbad',
  'Durgapur',
  'Erode',
  'Faridabad',
  'Ghaziabad',
  'Goa',
  'Gorakhpur',
  'Guntur',
  'Gurgaon',
  'Guwahati',
  'Gwalior',
  'Hubli',
  'Hyderabad',
  'Indore',
  'Jabalpur',
  'Jaipur',
  'Jalandhar',
  'Jammu',
  'Jamnagar',
  'Jamshedpur',
  'Jodhpur',
  'Kanpur',
  'Kochi',
  'Kolhapur',
  'Kolkata',
  'Kota',
  'Kozhikode',
  'Lucknow',
  'Ludhiana',
  'Madurai',
  'Mangalore',
  'Meerut',
  'Moradabad',
  'Mumbai',
  'Mysore',
  'Nagpur',
  'Nashik',
  'Navi Mumbai',
  'Noida',
  'Patna',
  'Pune',
  'Raipur',
  'Rajkot',
  'Ranchi',
  'Salem',
  'Siliguri',
  'Solapur',
  'Srinagar',
  'Surat',
  'Thane',
  'Thiruvananthapuram',
  'Tiruchirappalli',
  'Tiruppur',
  'Udaipur',
  'Vadodara',
  'Varanasi',
  'Vasai-Virar',
  'Vijayawada',
  'Visakhapatnam',
  'Warangal',
];

# Cache for Indian cities from external API - will be populated on first request
INDIAN_CITIES_CACHE = None


@router.get("/")
def index():
    return "Application Running Successfully"


@router.get("/hello")
def hello_world():
    return {"data": "Hello, World!"}


@router.post("/upload")
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


@router.get("/download")
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


@router.get("/indian-cities")
async def get_indian_cities():
    """
    Returns list of Indian cities.

    Primary source: Hardcoded list of major Indian cities.
    Optional enhancement: Tries to fetch from countriesnow.space API for a more comprehensive list.
    If the external API fails, falls back to the hardcoded list.
    Results are cached after first successful external API request.
    """
    global INDIAN_CITIES_CACHE

    # Return cached data from external API if available
    if INDIAN_CITIES_CACHE is not None:
        logger.info(f"Returning cached Indian cities from API ({len(INDIAN_CITIES_CACHE)} cities)")
        return {"cities": INDIAN_CITIES_CACHE}

    # Try to fetch from external API for a more comprehensive list
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                "https://countriesnow.space/api/v0.1/countries/cities/q",
                params={"country": "India"},
                timeout=5.0  # Short timeout to fail fast
            )
            response.raise_for_status()
            data = response.json()

            if data.get("error") is False and data.get("data"):
                cities = sorted(data["data"])  # Sort alphabetically
                INDIAN_CITIES_CACHE = cities  # Cache the results
                logger.info(f"Successfully fetched {len(cities)} Indian cities from external API")
                return {"cities": cities}
            else:
                logger.warning("External API returned error, using fallback list")
                return {"cities": sorted(INDIAN_CITIES)}

    except Exception as e:
        # If external API fails for any reason, use the hardcoded list
        logger.warning(f"Could not fetch from external API ({str(e)}), using hardcoded list")
        return {"cities": sorted(INDIAN_CITIES)}
