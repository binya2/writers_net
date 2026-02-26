import os
from pymongo import MongoClient
import gridfs
from logger_config import logger


class MongoDB:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)

            MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
            logger.info("Connecting to MongoDB...")

            cls._instance.client = MongoClient(MONGO_URI)
            cls._instance.db = cls._instance.client["reshet_db"]
            cls._instance.fs = gridfs.GridFS(cls._instance.db)
            cls._instance.state_collection = cls._instance.db["pipeline_state"]

        return cls._instance


mongo_db = MongoDB()
