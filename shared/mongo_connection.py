from pymongo import MongoClient
import gridfs
from config import settings

class MongoDBConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            
            cls._instance.client = MongoClient(settings.MONGO_URI)
            cls._instance.db = cls._instance.client[settings.DB_NAME]
            cls._instance.fs = gridfs.GridFS(cls._instance.db)
            cls._instance.state_collection = cls._instance.db["pipeline_state"]
            
        return cls._instance

mongo_db = MongoDBConnection()
