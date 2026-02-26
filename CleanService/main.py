import json
from shared.config import settings
from shared.kafka_connection import kafka_service
from shared.logger_config import get_logger
from CleanService.utils import process_message

logger = get_logger("clean-service")


def start_consumer():
    kafka_service.consumer.subscribe([settings.CONSUME_TOPIC])
    logger.info(f"Clean Service started. Listening to topic: {settings.CONSUME_TOPIC}")

    try:
        while True:
            msg = kafka_service.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                msg_value = json.loads(msg.value().decode('utf-8'))
                process_message(msg_value)
            except Exception as e:
                logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down Clean service...")
    finally:
        kafka_service.consumer.close()


if __name__ == "__main__":
    start_consumer()
