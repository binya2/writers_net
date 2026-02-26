import os
import uuid
from io import BytesIO
from PIL import Image

from logger_config import logger
from mongoDB_connection import mongo_db
from kafka_connection import kafka_service

INPUT_FOLDER = os.getenv("INPUT_FOLDER", "./input_images")


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
        logger.error(f"Failed to extract metadata: {e}")
        return {"error": "Invalid image file"}


def process_and_dispatch(file_data: bytes, filename: str):
    image_id = str(uuid.uuid4())
    logger.info(f"Processing new file: {filename} with ID: {image_id}")

    metadata = get_image_metadata(file_data)

    mongo_db.fs.put(
        file_data,
        filename=filename,
        image_id=image_id,
        content_type=f"image/{metadata.get('format', 'unknown').lower()}"
    )

    state_document = {
        "image_id": image_id,
        "original_filename": filename,
        "status": "ingested",
        "metadata": metadata,

        "raw_text": None,
        "clean_text": None,
        "analytics": {
            "top_10_words": [],
            "weapons_found": [],
            "sentiment": None
        }
    }
    mongo_db.state_collection.insert_one(state_document)

    kafka_message = {
        "image_id": image_id,
        "status": "ingested"
    }
    kafka_service.produce(key=image_id, value=kafka_message)

    logger.info(f"Successfully dispatched image_id: {image_id}")
    return image_id


def scan_local_folder_task():
    if not os.path.exists(INPUT_FOLDER):
        logger.error(f"Input folder {INPUT_FOLDER} does not exist.")
        return

    logger.info(f"Scanning folder: {INPUT_FOLDER}")
    for filename in os.listdir(INPUT_FOLDER):
        file_path = os.path.join(INPUT_FOLDER, filename)
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                file_data = f.read()
            process_and_dispatch(file_data, filename)
            os.remove(file_path)
