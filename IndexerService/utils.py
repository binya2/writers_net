from Shared.elastic_connection import elastic_service
from Shared.logger_config import get_logger
from Shared.config import settings

logger = get_logger("indexer-service")

WRITERS_MAPPING = {
    "mappings": {
        "properties": {
            "image_id": {"type": "keyword"},
            "original_filename": {"type": "keyword"},
            "status": {"type": "keyword"},
            "metadata": {
                "properties": {
                    "format": {"type": "keyword"},
                    "width": {"type": "integer"},
                    "height": {"type": "integer"},
                    "size_bytes": {"type": "long"}
                }
            },
            "results": {
                "properties": {
                    "analysis": {
                        "properties": {
                            "top_10_words": {
                                "type": "nested",
                                "properties": {
                                    "word": {"type": "keyword"},
                                    "count": {"type": "integer"}
                                }
                            },
                            "weapons_found": {"type": "keyword"},
                            "sentiment": {"type": "keyword"}
                        }
                    },
                    "clean_text": {"type": "text", "analyzer": "standard"}
                }
            }
        }
    }
}


def init_indexer():
    try:
        index_name = settings.ES_INDEX
        elastic_service.ensure_index(index_name, mapping=WRITERS_MAPPING)
        logger.info(f"Indexer initialized for index: {index_name}")
    except Exception as e:
        logger.error(f"Failed to initialize indexer: {e}")


def index_document(doc: dict):
    image_id = doc.get("image_id", "unknown")
    try:
        index_name = settings.ES_INDEX

        elastic_service.client.index(
            index=index_name,
            id=image_id,
            document=doc
        )
        logger.info(f"Document {image_id} indexed to Elasticsearch.")
    except Exception as e:
        logger.error(f"Failed to index document {image_id}: {e}")


def process_message(msg_value: dict):
    image_id = msg_value.get("image_id")
    if not image_id:
        logger.warning("Received message with missing 'image_id' in Indexer. Skipping.")
        return

    logger.info(f"Indexing process started for: {image_id}")

    try:
        indexed_doc = {
            "image_id": image_id,
            "original_filename": msg_value.get("filename", "unknown"),
            "status": "completed",
            "metadata": msg_value.get("metadata", {}),
            "results": {
                "analysis": msg_value.get("analytics", {}),
                "clean_text": msg_value.get("clean_text", "")
            }
        }
        
        index_document(indexed_doc)
    except Exception as e:
        logger.error(f"Error during indexing process for {image_id}: {e}")
