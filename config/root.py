import os
from bson.objectid import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Shared MongoDB client instance (singleton pattern)
_mongo_client = None
_mongo_db = None


def get_client():
    """Get the shared MongoDB client instance.

    Returns:
        MongoClient: The MongoDB client instance.

    Raises:
        ConnectionError: If there's an error connecting to the database.
    """
    global _mongo_client, _mongo_db

    if _mongo_client is None:
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("DB_NAME")
        if not mongo_uri:
            raise ConnectionError("MONGO_URI environment variable not set")

        # Optimized connection pool settings for better performance
        _mongo_client = MongoClient(
            mongo_uri,
            maxPoolSize=50,  # Maximum connections in the pool
            minPoolSize=10,  # Minimum connections to maintain
            maxIdleTimeMS=50000,  # Max idle time before closing connection
            socketTimeoutMS=20000,  # Socket timeout
            connectTimeoutMS=20000,  # Connection timeout
            serverSelectionTimeoutMS=5000  # Server selection timeout
        )
        _mongo_db = _mongo_client.get_database(db_name)

    return _mongo_client


def get_database():
    """Get the shared MongoDB database instance.

    Returns:
        Database: The MongoDB database instance.

    Raises:
        ConnectionError: If there's an error connecting to the database.
    """
    # Ensure client is initialized
    get_client()
    return _mongo_db


def connect_to_mongo():
    """Connects to the MongoDB database using the MONGO_URI environment variable.

    DEPRECATED: Use get_database() instead for shared client instance.

    Returns:
        MongoClient: The MongoClient object for interacting with the database.

    Raises:
        ConnectionError: If there's an error connecting to the database.
    """
    global _mongo_client
    db = get_database()
    return _mongo_client, db


def disconnect_on_exit(client=None):
    """Disconnects from the MongoDB database.

    Args:
        client (MongoClient): The MongoClient object to disconnect (optional, uses shared client if None).
    """
    global _mongo_client

    if client is not None:
        client.close()
    elif _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        _mongo_db = None


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
    elif isinstance(document, datetime):
        return document.isoformat()
    else:
        return document
