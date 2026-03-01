import json
from Shared.kafka_connection import kafka_service
from OCREngine.utils import process_message
from Shared.logger_config import get_logger
from Shared.config import settings

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
                kafka_service.consumer.commit(msg)
            except Exception as e:
                logger.error(f"Critical error in OCR consumer loop: {e}")
    finally:
        kafka_service.consumer.close()

if __name__ == "__main__":
    start_consumer()
