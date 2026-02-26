from elasticsearch import Elasticsearch
from Shared.config import settings
from Shared.logger_config import get_logger

logger = get_logger("shared-elastic")

class ElasticConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ElasticConnection, cls).__new__(cls)
            try:
                cls._instance.client = Elasticsearch([settings.ES_HOST])
                logger.info(f"Connected to Elasticsearch at {settings.ES_HOST}")
            except Exception as e:
                logger.error(f"Failed to connect to Elasticsearch: {e}")
                cls._instance.client = None
        return cls._instance

    def ensure_index(self, index_name: str, mapping: dict = None):
        try:
            if not self.client.indices.exists(index=index_name):
                if mapping:
                    self.client.indices.create(index=index_name, body=mapping)
                else:
                    self.client.indices.create(index=index_name)
                logger.info(f"Created Elasticsearch index: {index_name} with mapping.")
        except Exception as e:
            logger.error(f"Failed to check/create index {index_name}: {e}")

elastic_service = ElasticConnection()
