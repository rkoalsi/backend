from fastapi import APIRouter, HTTPException
from backend.config.root import connect_to_mongo, serialize_mongo_document  # type: ignore
from .helpers import get_access_token
from dotenv import load_dotenv
import datetime, json

load_dotenv()

router = APIRouter()

client, db = connect_to_mongo()


def handle_estimate(data: dict):
    estimate = data.get("estimate")
    estimate_id = estimate.get("estimate_id")
    exists = serialize_mongo_document(
        db.estimates.find_one({"estimate_id": estimate_id})
    )
    if not exists:
        db.estimates.insert_one(
            {
                **estimate,
                "created_at": datetime.datetime.now(),
            }
        )
    else:
        print("Estimate Exists", json.dumps((exists), indent=4))
        print("New Estimate Data", json.dumps(data, indent=4))


def handle_customer(data: dict):
    contact = data.get("contact")
    contact_id = contact.get("contact_id")

    # Check if the customer already exists in the database
    existing_customer = serialize_mongo_document(
        db.customers.find_one({"contact_id": contact_id})
    )

    def is_address_present(address, existing_addresses):
        # Check if the address is already present in the existing addresses
        return any(
            all(address.get(key) == existing_addr.get(key) for key in address.keys())
            for existing_addr in existing_addresses
        )

    if not existing_customer:
        # Insert the new customer with addresses
        addresses = []

        # Add billing_address to addresses if it exists
        if "billing_address" in contact and contact["billing_address"]:
            addresses.append(contact["billing_address"])

        # Add shipping_address to addresses if it exists and is not a duplicate of billing_address
        if "shipping_address" in contact and contact["shipping_address"]:
            if not is_address_present(contact["shipping_address"], addresses):
                addresses.append(contact["shipping_address"])

        # Remove billing_address and shipping_address from contact
        contact.pop("billing_address", None)
        contact.pop("shipping_address", None)

        db.customers.insert_one(
            {
                **contact,
                "addresses": addresses,
                "created_at": datetime.datetime.now(),
                "updated_at": datetime.datetime.now(),
            }
        )
        print("New customer inserted.")
    else:
        print("Customer exists. Checking for updates...")

        # Prepare the update document
        update_fields = {}

        # Update individual fields if they have changed
        for key, value in contact.items():
            if (
                key not in ["billing_address", "shipping_address", "addresses"]
                and existing_customer.get(key) != value
            ):
                update_fields[key] = value

        # Handle addresses
        existing_addresses = existing_customer.get("addresses", [])
        new_addresses = []

        # Add billing_address to new_addresses if it doesn't already exist
        if "billing_address" in contact and contact["billing_address"]:
            if not is_address_present(contact["billing_address"], existing_addresses):
                new_addresses.append(contact["billing_address"])

        # Add shipping_address to new_addresses if it doesn't already exist
        if "shipping_address" in contact and contact["shipping_address"]:
            if not is_address_present(contact["shipping_address"], existing_addresses):
                new_addresses.append(contact["shipping_address"])

        # Add new addresses to the update
        if new_addresses:
            update_fields["addresses"] = existing_addresses + new_addresses

        # Remove billing_address and shipping_address from contact
        update_fields.pop("billing_address", None)
        update_fields.pop("shipping_address", None)

        # Update the customer if there are changes
        if update_fields:
            update_fields["updated_at"] = datetime.datetime.now()
            db.customers.update_one(
                {"contact_id": contact_id},
                {
                    "$set": update_fields,
                    "$unset": {"billing_address": "", "shipping_address": ""},
                },
            )
            # Convert datetime to string for JSON serialization
            update_fields_serialized = {
                key: (
                    value.isoformat() if isinstance(value, datetime.datetime) else value
                )
                for key, value in update_fields.items()
            }
            print("Customer updated:", json.dumps(update_fields_serialized, indent=4))
        else:
            print("No updates required for the customer.")


@router.post("/estimate")
def estimate(data: dict):
    print(data)
    handle_estimate(data)
    return "Estimate Webhook Received Successfully"


@router.post("/customer")
def customer(data: dict):
    print(data)
    handle_customer(data)
    return "Customer Webhook Received Successfully"
