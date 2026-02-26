import os
import uuid
import json
from io import BytesIO
from PIL import Image

from shared.mongo_connection import mongo_db
from shared.kafka_connection import kafka_service
from shared.logger_config import get_logger
from shared.config import settings

logger = get_logger("ingestion-service")


def get_image_metadata(image_bytes: bytes) -> dict:
    try:
        img = Image.open(BytesIO(image_bytes))
        return {
            "format": img.format,
            "width": img.width,
            "height": img.height,
            "size_bytes": len(image_bytes)
        }
    except Exception as e:
        logger.error(f"Failed to extract image metadata: {e}")
        return {"error": "Invalid image file", "size_bytes": len(image_bytes)}


def save_to_gridfs(file_data: bytes, filename: str, image_id: str, metadata: dict):
    content_type = f"image/{metadata.get('format', 'unknown').lower()}"
    mongo_db.fs.put(
        file_data,
        filename=filename,
        image_id=image_id,
        content_type=content_type
    )
    logger.info(f"Image saved to GridFS: {filename}")


def create_initial_state(image_id: str, filename: str, metadata: dict):
    state_document = {
        "image_id": image_id,
        "original_filename": filename,
        "status": "ingested",
        "metadata": metadata,
        "raw_text": None,
        "clean_text": None,
        "analysis": {
            "top_10_words": [],
            "weapons_found": [],
            "sentiment": None
        },
        "history": [{
            "status": "ingested",
            "timestamp": "now"
        }]
    }
    mongo_db.state_collection.insert_one(state_document)
    logger.info(f"Initial state created for image_id: {image_id}")


def notify_kafka_ingested(image_id: str):
    kafka_message = {
        "image_id": image_id,
        "status": "ingested"
    }

    kafka_service.producer.produce(
        settings.KAFKA_TOPIC,
        key=image_id.encode('utf-8'),
        value=json.dumps(kafka_message).encode('utf-8')
    )
    kafka_service.producer.flush()
    logger.info(f"Kafka notification sent for image_id: {image_id}")


def process_and_dispatch(file_data: bytes, filename: str) -> str:
    image_id = str(uuid.uuid4())
    logger.info(f"Processing image: {filename} (ID: {image_id})")

    metadata = get_image_metadata(file_data)

    save_to_gridfs(file_data, filename, image_id, metadata)
    create_initial_state(image_id, filename, metadata)
    notify_kafka_ingested(image_id)

    return image_id


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
