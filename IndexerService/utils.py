from Shared.mongo_connection import mongo_db
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


def fetch_and_clean_document(image_id: str) -> dict | None:
    document = mongo_db.state_collection.find_one({"image_id": image_id})
    if not document:
        return None

    results_data = document.get("results", {})
    clean_text = results_data.get("clean_text", "")
    raw_text = results_data.get("raw_text", "")

    final_text = clean_text if clean_text.strip() else raw_text

    indexed_doc = {
        "image_id": document.get("image_id"),
        "original_filename": document.get("original_filename"),
        "status": document.get("status"),
        "metadata": document.get("metadata", {}),
        "results": {
            "analysis": results_data.get("analysis", {}),
            "clean_text": final_text
        }
    }
    return indexed_doc


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

        mongo_db.state_collection.update_one(
            {"image_id": image_id},
            {"$set": {"status": "indexed"}}
        )
    except Exception as e:
        logger.error(f"Failed to index document {image_id}: {e}")


def process_message(msg_value: dict):
    image_id = msg_value.get("image_id")
    if not image_id:
        logger.warning("Received message with missing 'image_id' in Indexer. Skipping.")
        return

    logger.info(f"Indexing process started for: {image_id}")

    try:
        clean_doc = fetch_and_clean_document(image_id)
        if clean_doc:
            index_document(clean_doc)
        else:
            logger.error(f"Document {image_id} not found in MongoDB for indexing.")
            mongo_db.update_failed_status(image_id, "Document not found in MongoDB for indexing")
    except Exception as e:
        logger.error(f"Error indexing document {image_id}: {e}")
        mongo_db.update_failed_status(image_id, str(e))
