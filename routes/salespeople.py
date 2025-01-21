from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document, parse_data  # type: ignore
from bson import ObjectId

router = APIRouter()

client, db = connect_to_mongo()


client, db = connect_to_mongo()
users_collection = db["users"]


@router.get("")
def salespeople():
    users_cursor = db.users.find({"role": "sales_person"})
    sales_people = list(users_cursor)

    # Prepare the result
    # for sales_person in sales_people:
    #     sales_person_code = sales_person.get("code")

    #     if sales_person_code:
    #         # Fetch customers assigned to the salesperson
    #         customers_cursor = db.customers.find(
    #             {
    #                 "$or": [
    #                     {
    #                         "cf_sales_person": {
    #                             "$regex": f"\\b{sales_person_code}\\b",
    #                             "$options": "i",
    #                         }
    #                     },
    #                     {"cf_sales_person": "Defaulter"},
    #                     {"cf_sales_person": "Company customers"},
    #                 ],
    #                 "status": "active",
    #             }
    #         )
    #         sales_person["customers"] = serialize_mongo_document(list(customers_cursor))
    #     else:
    #         # Assign customers with "Defaulter" or "Company customers" to all salespeople
    #         customers_cursor = db.customers.find(
    #             {
    #                 "$or": [
    #                     {"cf_sales_person": "Defaulter"},
    #                     {"cf_sales_person": "Company customers"},
    #                 ],
    #                 "status": "active",
    #             }
    #         )
    #         sales_person["customers"] = serialize_mongo_document(list(customers_cursor))

    return {"users": serialize_mongo_document(sales_people)}


@router.get("/customers")
def get_salespeople_customers():
    users_cursor = db.users.find({"role": "sales_person"})
    users = serialize_mongo_document(list(users_cursor))
    return {"users": users}


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
