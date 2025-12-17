from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, validator
from datetime import datetime, timedelta
import random
import re
import httpx
from typing import List, Optional
from ..config.root import get_database
from ..config.whatsapp import send_whatsapp

router = APIRouter()

# Cache for country codes to avoid repeated API calls
_country_codes_cache = None
_cache_timestamp = None
CACHE_DURATION = timedelta(hours=24)  # Cache for 24 hours


class CountryCode(BaseModel):
    code: str
    country: str
    maxLength: int


class SendOTPRequest(BaseModel):
    phone: str

    @validator('phone')
    def validate_phone(cls, v):
        v = v.strip()
        # Check if it starts with + and contains only digits after that
        if not re.match(r'^\+\d+$', v):
            raise ValueError('Phone number must be in international format (e.g., +919876543210)')
        # Validate length (between 10 and 16 characters total including +)
        if len(v) < 10 or len(v) > 16:
            raise ValueError('Invalid phone number length')
        return v


class VerifyOTPRequest(BaseModel):
    phone: str
    otp: str

    @validator('phone')
    def validate_phone(cls, v):
        v = v.strip()
        if not re.match(r'^\+\d+$', v):
            raise ValueError('Phone number must be in international format (e.g., +919876543210)')
        if len(v) < 10 or len(v) > 16:
            raise ValueError('Invalid phone number length')
        return v

    @validator('otp')
    def validate_otp(cls, v):
        v = v.strip()
        if not v.isdigit() or len(v) != 6:
            raise ValueError('OTP must be a 6-digit number')
        return v


def generate_otp():
    return str(random.randint(100000, 999999))


async def fetch_country_codes_from_api() -> List[dict]:
    """
    Fetch country codes from restcountries.com API
    Returns a curated list of popular WhatsApp-enabled countries
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get("https://restcountries.com/v3.1/all?fields=name,idd")

            if response.status_code != 200:
                raise Exception("Failed to fetch countries from API")

            countries_data = response.json()

            # Map of country codes to their typical phone number lengths
            # This is a curated list for better accuracy
            phone_length_map = {
                "+1": 10,      # USA, Canada
                "+7": 10,      # Russia, Kazakhstan
                "+20": 10,     # Egypt
                "+27": 9,      # South Africa
                "+30": 10,     # Greece
                "+31": 9,      # Netherlands
                "+32": 9,      # Belgium
                "+33": 9,      # France
                "+34": 9,      # Spain
                "+36": 9,      # Hungary
                "+39": 10,     # Italy
                "+40": 10,     # Romania
                "+41": 9,      # Switzerland
                "+43": 10,     # Austria
                "+44": 10,     # UK
                "+45": 8,      # Denmark
                "+46": 9,      # Sweden
                "+47": 8,      # Norway
                "+48": 9,      # Poland
                "+49": 10,     # Germany
                "+51": 9,      # Peru
                "+52": 10,     # Mexico
                "+53": 8,      # Cuba
                "+54": 10,     # Argentina
                "+55": 11,     # Brazil
                "+56": 9,      # Chile
                "+57": 10,     # Colombia
                "+58": 10,     # Venezuela
                "+60": 9,      # Malaysia
                "+61": 9,      # Australia
                "+62": 10,     # Indonesia
                "+63": 10,     # Philippines
                "+64": 9,      # New Zealand
                "+65": 8,      # Singapore
                "+66": 9,      # Thailand
                "+81": 10,     # Japan
                "+82": 10,     # South Korea
                "+84": 9,      # Vietnam
                "+86": 11,     # China
                "+90": 10,     # Turkey
                "+91": 10,     # India
                "+92": 10,     # Pakistan
                "+93": 9,      # Afghanistan
                "+94": 9,      # Sri Lanka
                "+95": 9,      # Myanmar
                "+98": 10,     # Iran
                "+212": 9,     # Morocco
                "+213": 9,     # Algeria
                "+216": 8,     # Tunisia
                "+218": 10,    # Libya
                "+220": 7,     # Gambia
                "+221": 9,     # Senegal
                "+222": 8,     # Mauritania
                "+223": 8,     # Mali
                "+224": 9,     # Guinea
                "+225": 10,    # Ivory Coast
                "+226": 8,     # Burkina Faso
                "+227": 8,     # Niger
                "+228": 8,     # Togo
                "+229": 8,     # Benin
                "+230": 8,     # Mauritius
                "+231": 7,     # Liberia
                "+232": 8,     # Sierra Leone
                "+233": 9,     # Ghana
                "+234": 10,    # Nigeria
                "+235": 8,     # Chad
                "+236": 8,     # Central African Republic
                "+237": 9,     # Cameroon
                "+238": 7,     # Cape Verde
                "+239": 7,     # Sao Tome and Principe
                "+240": 9,     # Equatorial Guinea
                "+241": 7,     # Gabon
                "+242": 9,     # Republic of the Congo
                "+243": 9,     # Democratic Republic of the Congo
                "+244": 9,     # Angola
                "+245": 7,     # Guinea-Bissau
                "+246": 7,     # British Indian Ocean Territory
                "+248": 7,     # Seychelles
                "+249": 9,     # Sudan
                "+250": 9,     # Rwanda
                "+251": 9,     # Ethiopia
                "+252": 8,     # Somalia
                "+253": 8,     # Djibouti
                "+254": 10,    # Kenya
                "+255": 9,     # Tanzania
                "+256": 9,     # Uganda
                "+257": 8,     # Burundi
                "+258": 9,     # Mozambique
                "+260": 9,     # Zambia
                "+261": 9,     # Madagascar
                "+262": 9,     # Réunion
                "+263": 9,     # Zimbabwe
                "+264": 9,     # Namibia
                "+265": 9,     # Malawi
                "+266": 8,     # Lesotho
                "+267": 8,     # Botswana
                "+268": 8,     # Eswatini
                "+269": 7,     # Comoros
                "+290": 4,     # Saint Helena
                "+291": 7,     # Eritrea
                "+297": 7,     # Aruba
                "+298": 6,     # Faroe Islands
                "+299": 6,     # Greenland
                "+350": 8,     # Gibraltar
                "+351": 9,     # Portugal
                "+352": 9,     # Luxembourg
                "+353": 9,     # Ireland
                "+354": 7,     # Iceland
                "+355": 9,     # Albania
                "+356": 8,     # Malta
                "+357": 8,     # Cyprus
                "+358": 9,     # Finland
                "+359": 9,     # Bulgaria
                "+370": 8,     # Lithuania
                "+371": 8,     # Latvia
                "+372": 7,     # Estonia
                "+373": 8,     # Moldova
                "+374": 8,     # Armenia
                "+375": 9,     # Belarus
                "+376": 6,     # Andorra
                "+377": 8,     # Monaco
                "+378": 10,    # San Marino
                "+380": 9,     # Ukraine
                "+381": 9,     # Serbia
                "+382": 8,     # Montenegro
                "+383": 8,     # Kosovo
                "+385": 9,     # Croatia
                "+386": 8,     # Slovenia
                "+387": 8,     # Bosnia and Herzegovina
                "+389": 8,     # North Macedonia
                "+420": 9,     # Czech Republic
                "+421": 9,     # Slovakia
                "+423": 7,     # Liechtenstein
                "+500": 5,     # Falkland Islands
                "+501": 7,     # Belize
                "+502": 8,     # Guatemala
                "+503": 8,     # El Salvador
                "+504": 8,     # Honduras
                "+505": 8,     # Nicaragua
                "+506": 8,     # Costa Rica
                "+507": 8,     # Panama
                "+508": 6,     # Saint Pierre and Miquelon
                "+509": 8,     # Haiti
                "+590": 9,     # Guadeloupe
                "+591": 8,     # Bolivia
                "+592": 7,     # Guyana
                "+593": 9,     # Ecuador
                "+594": 9,     # French Guiana
                "+595": 9,     # Paraguay
                "+596": 9,     # Martinique
                "+597": 7,     # Suriname
                "+598": 8,     # Uruguay
                "+599": 7,     # Curaçao
                "+670": 8,     # East Timor
                "+672": 6,     # Antarctica
                "+673": 7,     # Brunei
                "+674": 7,     # Nauru
                "+675": 8,     # Papua New Guinea
                "+676": 5,     # Tonga
                "+677": 7,     # Solomon Islands
                "+678": 7,     # Vanuatu
                "+679": 7,     # Fiji
                "+680": 7,     # Palau
                "+681": 6,     # Wallis and Futuna
                "+682": 5,     # Cook Islands
                "+683": 4,     # Niue
                "+685": 7,     # Samoa
                "+686": 8,     # Kiribati
                "+687": 6,     # New Caledonia
                "+688": 6,     # Tuvalu
                "+689": 8,     # French Polynesia
                "+690": 4,     # Tokelau
                "+691": 7,     # Micronesia
                "+692": 7,     # Marshall Islands
                "+850": 10,    # North Korea
                "+852": 8,     # Hong Kong
                "+853": 8,     # Macau
                "+855": 9,     # Cambodia
                "+856": 9,     # Laos
                "+880": 10,    # Bangladesh
                "+886": 9,     # Taiwan
                "+960": 7,     # Maldives
                "+961": 8,     # Lebanon
                "+962": 9,     # Jordan
                "+963": 9,     # Syria
                "+964": 10,    # Iraq
                "+965": 8,     # Kuwait
                "+966": 9,     # Saudi Arabia
                "+967": 9,     # Yemen
                "+968": 8,     # Oman
                "+970": 9,     # Palestine
                "+971": 9,     # UAE
                "+972": 9,     # Israel
                "+973": 8,     # Bahrain
                "+974": 8,     # Qatar
                "+975": 8,     # Bhutan
                "+976": 8,     # Mongolia
                "+977": 10,    # Nepal
                "+992": 9,     # Tajikistan
                "+993": 8,     # Turkmenistan
                "+994": 9,     # Azerbaijan
                "+995": 9,     # Georgia
                "+996": 9,     # Kyrgyzstan
                "+998": 9,     # Uzbekistan
            }

            country_codes = []

            for country in countries_data:
                name = country.get("name", {}).get("common", "")
                idd = country.get("idd", {})
                root = idd.get("root", "")
                suffixes = idd.get("suffixes", [])

                if root and suffixes:
                    for suffix in suffixes:
                        code = f"{root}{suffix}"
                        # Use the phone length from our map, default to 10 if not found
                        max_length = phone_length_map.get(code, 10)

                        country_codes.append({
                            "code": code,
                            "country": name,
                            "maxLength": max_length
                        })

            # Sort by code for easier lookup
            country_codes.sort(key=lambda x: x["code"])

            # Ensure India (+91) is in the list
            if not any(c["code"] == "+91" for c in country_codes):
                country_codes.insert(0, {"code": "+91", "country": "India", "maxLength": 10})

            return country_codes

    except Exception as e:
        print(f"Error fetching countries from API: {e}")
        # Return a fallback list of popular countries
        return [
            {"code": "+91", "country": "India", "maxLength": 10},
            {"code": "+1", "country": "USA/Canada", "maxLength": 10},
            {"code": "+44", "country": "UK", "maxLength": 10},
            {"code": "+971", "country": "UAE", "maxLength": 9},
            {"code": "+966", "country": "Saudi Arabia", "maxLength": 9},
            {"code": "+65", "country": "Singapore", "maxLength": 8},
            {"code": "+60", "country": "Malaysia", "maxLength": 9},
            {"code": "+61", "country": "Australia", "maxLength": 9},
            {"code": "+92", "country": "Pakistan", "maxLength": 10},
            {"code": "+880", "country": "Bangladesh", "maxLength": 10},
            {"code": "+94", "country": "Sri Lanka", "maxLength": 9},
            {"code": "+977", "country": "Nepal", "maxLength": 10},
        ]


@router.get("/country-codes")
async def get_country_codes():
    """
    Get list of all countries with their country codes and max phone number lengths
    Fetches from external API and caches for 24 hours
    """
    global _country_codes_cache, _cache_timestamp

    try:
        # Check if cache is valid
        if _country_codes_cache and _cache_timestamp:
            if datetime.now() - _cache_timestamp < CACHE_DURATION:
                return {
                    "success": True,
                    "data": _country_codes_cache,
                    "cached": True
                }

        # Fetch fresh data
        country_codes = await fetch_country_codes_from_api()

        # Update cache
        _country_codes_cache = country_codes
        _cache_timestamp = datetime.now()

        return {
            "success": True,
            "data": country_codes,
            "cached": False
        }

    except Exception as e:
        print(f"Error getting country codes: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get country codes: {str(e)}")


@router.post("/send-otp")
async def send_otp(request: SendOTPRequest):
    """
    Send OTP to international phone number via WhatsApp
    Now accepts full international phone numbers with country code (e.g., +919876543210)
    """
    try:
        db = get_database()
        # Phone is already validated by pydantic validator
        phone = request.phone  # International format: +919876543210

        otp = generate_otp()

        # Check if lead already exists with this phone number
        existing_lead = db.catalogue_leads.find_one({"phone": phone})

        if existing_lead:
            # Update existing lead with new OTP
            db.catalogue_leads.update_one(
                {"phone": phone},
                {
                    "$set": {
                        "otp": otp,
                        "otp_created_at": datetime.now(),
                        "otp_attempts": 0,
                        "updated_at": datetime.now()
                    }
                }
            )
        else:
            # Create new lead entry
            db.catalogue_leads.insert_one({
                "phone": phone,
                "otp": otp,
                "otp_created_at": datetime.now(),
                "otp_attempts": 0,
                "verified": False,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })

        # Get WhatsApp template
        template = db.templates.find_one({"name": "otp_verification"})

        if not template:
            raise HTTPException(status_code=500, detail="OTP template not found")

        # Send WhatsApp message with international number
        # Note: send_whatsapp function should handle international format
        send_whatsapp(
            to=phone,  # Send with full international number
            template_doc=template,
            params={"otp": otp}
        )

        return {
            "success": True,
            "message": "OTP sent successfully via WhatsApp",
            "phone": phone
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error sending OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send OTP: {str(e)}")


@router.post("/verify-otp")
async def verify_otp(request: VerifyOTPRequest):
    """
    Verify OTP for international phone number
    Accepts full international phone numbers with country code (e.g., +919876543210)
    """
    try:
        db = get_database()
        # Phone and OTP are already validated by pydantic validators
        phone = request.phone  # International format: +919876543210
        otp = request.otp

        # Find lead by international phone number
        lead = db.catalogue_leads.find_one({"phone": phone})

        if not lead:
            raise HTTPException(status_code=404, detail="No OTP request found for this phone number")

        # Check if already verified
        if lead.get("verified", False):
            return {
                "success": True,
                "message": "Phone number already verified",
                "verified": True
            }

        # Check if OTP exists
        otp_created_at = lead.get("otp_created_at")
        if not otp_created_at:
            raise HTTPException(status_code=400, detail="No OTP found. Please request a new OTP")

        # Check OTP expiry (10 minutes)
        time_diff = datetime.now() - otp_created_at
        if time_diff > timedelta(minutes=10):
            raise HTTPException(status_code=400, detail="OTP expired. Please request a new OTP")

        # Check attempt limit
        otp_attempts = lead.get("otp_attempts", 0)
        if otp_attempts >= 3:
            raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP")

        # Verify OTP
        stored_otp = lead.get("otp")

        if stored_otp != otp:
            # Increment failed attempts
            db.catalogue_leads.update_one(
                {"phone": phone},
                {"$inc": {"otp_attempts": 1}}
            )
            remaining_attempts = 3 - (otp_attempts + 1)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid OTP. {remaining_attempts} attempts remaining"
            )

        # OTP verified successfully - update lead
        db.catalogue_leads.update_one(
            {"phone": phone},
            {
                "$set": {
                    "verified": True,
                    "verified_at": datetime.now(),
                    "updated_at": datetime.now()
                },
                "$unset": {"otp": "", "otp_created_at": "", "otp_attempts": ""}
            }
        )

        return {
            "success": True,
            "message": "Phone number verified successfully",
            "verified": True
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error verifying OTP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to verify OTP: {str(e)}")
