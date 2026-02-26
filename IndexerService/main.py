import json
from Shared.config import settings
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from IndexerService.utils import process_message, init_indexer

logger = get_logger("indexer-service")


def start_consumer():
    init_indexer()

    kafka_service.consumer.subscribe([settings.CONSUME_TOPIC])
    logger.info(f"Indexer Service started. Listening to topic: {settings.CONSUME_TOPIC}")

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
        logger.info("Shutting down Indexer service...")
    finally:
        kafka_service.consumer.close()


if __name__ == "__main__":
    start_consumer()
