import re
import json
from shared.mongo_connection import mongo_db
from shared.kafka_connection import kafka_service
from shared.logger_config import get_logger
from shared.config import settings

logger = get_logger("clean-service")


def fetch_raw_text(image_id: str) -> str | None:
    document = mongo_db.state_collection.find_one({"image_id": image_id})
    if not document:
        logger.error(f"Document {image_id} not found in MongoDB")
        return None
    return document.get("results", {}).get("raw_text", "")


def clean_ocr_text(raw_text: str) -> str:
    if not raw_text:
        return ""
    text = raw_text.replace('\n', ' ').replace('\r', ' ')
    text = re.sub(r'[^a-zA-Z0-9\s.,!?"\'():\-/;%&+=]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def update_db_cleaned(image_id: str, clean_text: str):
    mongo_db.state_collection.update_one(
        {"image_id": image_id},
        {"$set": {
            "results.clean_text": clean_text,
            "status": "clean_completed"
        }}
    )
    logger.info(f"MongoDB updated with clean text for: {image_id}")


def notify_clean_complete(image_id: str):
    next_event = {
        "image_id": image_id,
        "status": "clean_completed"
    }
    kafka_service.producer.poll(0)
    kafka_service.producer.produce(
        settings.PRODUCE_TOPIC,
        key=image_id.encode('utf-8'),
        value=json.dumps(next_event).encode('utf-8')
    )
    kafka_service.producer.flush()


def process_message(msg_value: dict):
    image_id = msg_value.get("image_id")
    if not image_id:
        return

    logger.info(f"Starting Clean process for: {image_id}")

    raw_text = fetch_raw_text(image_id)
    if raw_text is None:
        return

    cleaned = clean_ocr_text(raw_text)
    update_db_cleaned(image_id, cleaned)
    notify_clean_complete(image_id)
