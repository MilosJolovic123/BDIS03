from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME

def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)

def get_db():
    client = get_mongo_client()
    return client[MONGO_DB_NAME]
