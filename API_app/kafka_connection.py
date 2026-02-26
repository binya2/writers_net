import os
import json
from confluent_kafka import Producer

from logger_config import logger


class KafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaProducer, cls).__new__(cls)
            
            KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
            logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
            
            cls._instance.producer = Producer({'bootstrap.servers': KAFKA_BROKER})
            cls._instance.topic = os.getenv("KAFKA_TOPIC", "FILE_UPLOADED")
            
        return cls._instance

    def produce(self, key, value):
        self.producer.produce(
            self.topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8')
        )
        self.producer.flush()


kafka_service = KafkaProducer()
