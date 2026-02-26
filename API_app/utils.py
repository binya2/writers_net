import os
import uuid
import json
import mimetypes
from io import BytesIO
from PIL import Image

from shared.mongo_connection import mongo_db
from shared.kafka_connection import kafka_service
from shared.logger_config import get_logger
from shared.config import settings

logger = get_logger("ingestion-service")


def extract_image_metadata(image_bytes: bytes) -> dict:
    """מחלץ מטא-דאטה ספציפי לתמונות"""
    try:
        img = Image.open(BytesIO(image_bytes))
        return {
            "type": "image",
            "format": img.format,
            "width": img.width,
            "height": img.height,
        }
    except Exception as e:
        logger.error(f"Failed to extract image metadata: {e}")
        return {"type": "unknown"}


def get_object_metadata(file_data: bytes, content_type: str) -> dict:
    """מחלץ מטא-דאטה לפי סוג האובייקט"""
    metadata = {
        "size_bytes": len(file_data),
        "content_type": content_type,
    }

    if content_type.startswith("image/"):
        metadata.update(extract_image_metadata(file_data))
    else:
        metadata["type"] = content_type.split('/')[0] if '/' in content_type else "other"

    return metadata


def save_object_to_store(file_data: bytes, filename: str, object_id: str, content_type: str):
    """שמירת גוף האובייקט ב-GridFS"""
    mongo_db.fs.put(
        file_data,
        filename=filename,
        object_id=object_id,
        content_type=content_type
    )
    logger.info(f"Object {object_id} ({content_type}) saved to store")


def create_initial_state(object_id: str, filename: str, metadata: dict):
    state_document = {
        "object_id": object_id,
        "original_filename": filename,
        "content_type": metadata.get("content_type"),
        "status": "ingested",
        "metadata": metadata,
        "results": {
            "raw_text": None,
            "clean_text": None,
            "analysis": {
                "top_10_words": [],
                "weapons_found": [],
                "sentiment": None,
                "language": None
            }
        }
    }

    mongo_db.state_collection.insert_one(state_document)
    logger.info(f"State initialized for object: {object_id}")


def dispatch_object_event(object_id: str, metadata: dict):
    event = {
        "object_id": object_id,
        "content_type": metadata.get("content_type"),
        "type": metadata.get("type"),
        "status": "ingested"
    }

    kafka_service.producer.produce(
        settings.KAFKA_TOPIC,
        key=object_id.encode('utf-8'),
        value=json.dumps(event).encode('utf-8')
    )
    kafka_service.producer.flush()


def process_and_dispatch(file_data: bytes, filename: str, content_type: str = None) -> str:
    if not content_type:
        content_type, _ = mimetypes.guess_type(filename)
        content_type = content_type or "application/octet-stream"

    object_id = str(uuid.uuid4())
    logger.info(f"Ingesting object: {filename} as {object_id}")

    metadata = get_object_metadata(file_data, content_type)

    save_object_to_store(file_data, filename, object_id, content_type)
    create_initial_state(object_id, filename, metadata)
    dispatch_object_event(object_id, metadata)

    return object_id


def scan_local_folder_task():
    if not os.path.exists(settings.INPUT_FOLDER):
        logger.error(f"Input folder {settings.INPUT_FOLDER} does not exist.")
        return

    for filename in os.listdir(settings.INPUT_FOLDER):
        file_path = os.path.join(settings.INPUT_FOLDER, filename)
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                file_data = f.read()
            process_and_dispatch(file_data, filename)
            os.remove(file_path)
