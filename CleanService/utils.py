import re
import json
import os
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from Shared.config import settings

logger = get_logger("clean-service")

def load_weapons():
    try:
        file_path = settings.WEAPONS_FILE
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                return [line.strip().lower() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Failed to load weapons for cleaning: {e}")
    return []

WEAPONS_LIST = load_weapons()

def clean_ocr_text(raw_text: str) -> str:
    try:
        if not raw_text:
            return ""
        text = raw_text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'[^a-zA-Z0-9\s.,!?"\'():\-/;%&+=]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    except Exception as e:
        logger.error(f"Text cleaning failed: {e}")
        return raw_text

def notify_clean_complete(image_id: str, filename: str, metadata: dict, clean_text: str):
    next_event = {
        "image_id": image_id,
        "filename": filename,
        "metadata": metadata,
        "clean_text": clean_text,
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
    filename = msg_value.get("filename", "unknown")
    metadata = msg_value.get("metadata", {})
    raw_text = msg_value.get("raw_text", "")

    if not image_id:
        logger.warning("Received message with missing 'image_id' in CleanService. Skipping.")
        return

    logger.info(f"Starting Clean process for: {image_id}")

    try:
        cleaned = clean_ocr_text(raw_text)
        if not cleaned and raw_text:
            cleaned = raw_text
        
        notify_clean_complete(image_id, filename, metadata, cleaned)
    except Exception as e:
        logger.error(f"Error cleaning document {image_id}: {e}")
        notify_clean_complete(image_id, filename, metadata, raw_text)
