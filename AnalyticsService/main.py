import json
from Shared.config import settings
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from AnalyticsService.utils import process_message, init_analytics

logger = get_logger("analytics-service")


def start_consumer():
    logger.info("Initializing Analytics Engine...")
    init_analytics()

    kafka_service.consumer.subscribe([settings.CONSUME_TOPIC])
    logger.info(f"Analytics Service started. Listening to topic: {settings.CONSUME_TOPIC}")

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
        logger.info("Shutting down Analytics service...")
    finally:
        kafka_service.consumer.close()


if __name__ == "__main__":
    start_consumer()
