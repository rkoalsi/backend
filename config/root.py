import os, json
from bson import json_util
from bson.objectid import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


def connect_to_mongo():
    """Connects to the MongoDB database using the MONGO_URI environment variable.

    Returns:
        MongoClient: The MongoClient object for interacting with the database.

    Raises:
        ConnectionError: If there's an error connecting to the database.
    """

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME")
    if not mongo_uri:
        raise ConnectionError("MONGO_URI environment variable not set")

    client = MongoClient(mongo_uri)
    return client, client.get_database(db_name)


def disconnect_on_exit(client):
    """Disconnects from the MongoDB database.

    Args:
        client (MongoClient): The MongoClient object to disconnect.
    """

    client.close()


def parse_data(data):
    return json.loads(json_util.dumps(data))


def serialize_mongo_document(document):
    """
    Recursively convert MongoDB ObjectId fields to strings in a document.
    """
    if isinstance(document, list):
        return [serialize_mongo_document(item) for item in document]
    elif isinstance(document, dict):
        return {key: serialize_mongo_document(value) for key, value in document.items()}
    elif isinstance(document, ObjectId):
        return str(document)
    else:
        return document
