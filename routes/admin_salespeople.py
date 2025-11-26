from fastapi import (
    APIRouter,
    HTTPException,
)
from config.root import get_database, serialize_mongo_document  
from bson.objectid import ObjectId
import re, os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()
router = APIRouter()
org_id = os.getenv("ORG_ID")
db = get_database()
products_collection = db["products"]
customers_collection = db["customers"]
orders_collection = db["orders"]
users_collection = db["users"]


@router.get("")
def home():
    # 1. Fetch all salespeople and gather their codes
    sales_people = list(db.users.find({"code": {"$exists": True}}))
    sales_people = serialize_mongo_document(sales_people)
    codes = [sp["code"] for sp in sales_people if sp.get("code")]

    # 2. Build a single regex pattern that includes all codes: \b(CODE1|CODE2)\b
    if codes:
        escaped_codes = [re.escape(c) for c in codes]
        combined_pattern = rf"\b({'|'.join(escaped_codes)})\b"
    else:
        combined_pattern = None

    # 3. Single query for all 'active' customers that are either:
    #    - "Defaulter"
    #    - "Company customers"
    #    - or match the combined regex (case-insensitive) in cf_sales_person
    or_conditions = [
        {"cf_sales_person": "Defaulter"},
        {"cf_sales_person": "Company customers"},
    ]
    if combined_pattern:
        or_conditions.append(
            {"cf_sales_person": {"$regex": combined_pattern, "$options": "i"}}
        )

    customers_cursor = db.customers.find({"status": "active", "$or": or_conditions})
    all_customers = serialize_mongo_document(list(customers_cursor))

    # 4. Group customers by salesperson code
    grouped_by_code = defaultdict(list)
    defaulters = []
    company_customers = []

    for cust in all_customers:
        cf_value = cust.get("cf_sales_person")

        # If exactly "Defaulter" or "Company customers", store in special lists
        if cf_value == "Defaulter":
            defaulters.append(cust)
            continue
        elif cf_value == "Company customers":
            company_customers.append(cust)
            continue

        # Otherwise, cf_sales_person could be a string or array
        # Normalize to a list so we can handle both in one pass
        items = cf_value if isinstance(cf_value, list) else [cf_value]

        # Check each item against each code to see if there's a match
        for item in items:
            for code in codes:
                # \b ensures we match code as a separate word or token
                if re.search(rf"\b{re.escape(code)}\b", str(item), re.IGNORECASE):
                    grouped_by_code[code].append(cust)
                    # If a customer can match multiple codes, remove this `break`
                    break

    # 5. Attach customers to each salesperson
    for sp in sales_people:
        code = sp.get("code")
        if code:
            # Their specific matches + universal defaulters + company customers
            sp["customers"] = (
                grouped_by_code.get(code, []) + defaulters + company_customers
            )
        else:
            sp["customers"] = defaulters + company_customers

    return {"users": sales_people}


@router.get("/customers")
def get_salespeople_customers():
    users_cursor = db.users.find({"role": "sales_person"})
    users = serialize_mongo_document(list(users_cursor))
    return {"users": users}


@router.post("")
async def create_salesperson(salesperson: dict):
    # Check if salesperson code or email already exists
    existing_person = next(
        (
            sp
            for sp in db.users.find({})
            if sp.get("email") == salesperson.get("email")
            or sp.get("code") == salesperson.get("code")
        ),
        None,
    )
    if existing_person:
        raise HTTPException(
            status_code=400,
            detail="Salesperson with this email or code already exists.",
        )

    # Add salesperson to the collection
    db.users.insert_one(salesperson)
    return "Sales Person Created"


@router.get("/{salesperson_id}")
def salesperson(salesperson_id: str):
    users_cursor = db.users.find_one({"_id": ObjectId(salesperson_id)})
    sales_person = serialize_mongo_document(dict(users_cursor))

    # Prepare the result
    sales_person_code = sales_person.get("code")

    if sales_person_code:
        escaped_sales_person = re.escape(sales_person_code)
        # Fetch customers assigned to the salesperson
        customers_cursor = db.customers.find(
            {
                "$or": [
                    {
                        "cf_sales_person": {
                            "$regex": f"^{escaped_sales_person}$",
                            "$options": "i",
                        }
                    },
                    {"cf_sales_person": "Defaulter"},
                    {"cf_sales_person": "Company customers"},
                ],
                "status": "active",
            }
        )
        sales_person["customers"] = serialize_mongo_document(list(customers_cursor))
    else:
        # Assign customers with "Defaulter" or "Company customers" to all salespeople
        customers_cursor = db.customers.find(
            {
                "$or": [
                    {"cf_sales_person": "Defaulter"},
                    {"cf_sales_person": "Company customers"},
                ],
                "status": "active",
            }
        )
        sales_person["customers"] = serialize_mongo_document(list(customers_cursor))

    return {"sales_person": sales_person}


@router.put("/{salesperson_id}")
def salespeople_id(salesperson_id: str, salesperson: dict):
    update_data = {k: v for k, v in salesperson.items() if k != "_id" and v is not None}

    if not update_data:
        raise HTTPException(
            status_code=400, detail="No valid fields provided for update"
        )

    # Perform the update
    result = db.users.update_one(
        {"_id": ObjectId(salesperson_id)},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sales Person not found")
    return {"message": "Sales Person Updated"}
