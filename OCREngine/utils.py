import json
from io import BytesIO
import pytesseract
from PIL import Image
from Shared.mongo_connection import mongo_db
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from Shared.config import settings

logger = get_logger("ocr-service")

def extract_text_from_memory(image_bytes: bytes) -> str:
    try:
        img = Image.open(BytesIO(image_bytes))
        text = pytesseract.image_to_string(img, lang='eng')
        return text.strip()
    except Exception as e:
        logger.error(f"OCR failed: {e}")
        return ""

def fetch_image_from_gridfs(image_id: str) -> bytes:
    grid_out = mongo_db.fs.find_one({"image_id": image_id})
    return grid_out.read() if grid_out else None

def notify_ocr_complete(image_id: str, filename: str, metadata: dict, raw_text: str):
    next_event = {
        "image_id": image_id, 
        "filename": filename,
        "metadata": metadata,
        "raw_text": raw_text,
        "status": "ocr_completed"
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
    
    if not image_id:
        logger.warning("Received message with missing 'image_id'. Skipping.")
        return

    try:
        image_bytes = fetch_image_from_gridfs(image_id)
        if image_bytes:
            raw_text = extract_text_from_memory(image_bytes)
            notify_ocr_complete(image_id, filename, metadata, raw_text)
        else:
            logger.error(f"Image {image_id} not found in GridFS.")
    except Exception as e:
        logger.error(f"Error processing message for image {image_id}: {e}")
