import os, json
from bson import json_util
from pymongo import MongoClient


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
