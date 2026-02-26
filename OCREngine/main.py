import json
from shared.kafka_connection import kafka_service
from OCREngine.utils import process_message
from shared.logger_config import get_logger
from shared.config import settings

logger = get_logger("ocr-service")

def start_consumer():
    kafka_service.consumer.subscribe([settings.CONSUME_TOPIC])
    logger.info(f"OCR Engine started. Listening to topic: {settings.CONSUME_TOPIC}")
    try:
        while True:
            msg = kafka_service.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                process_message(json.loads(msg.value().decode('utf-8')))
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    finally:
        kafka_service.consumer.close()

if __name__ == "__main__":
    start_consumer()
