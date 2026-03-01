from confluent_kafka import Producer, Consumer
from Shared.config import settings

class KafkaConnection:
    _instance = None

    def __init__(self):
        self._producer = None
        self._consumer = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaConnection, cls).__new__(cls)
            cls._instance._producer = None
            cls._instance._consumer = None
        return cls._instance

    @property
    def producer(self):
        if self._producer is None:
            self._producer = Producer({'bootstrap.servers': settings.KAFKA_BROKER})
        return self._producer

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = Consumer({
                'bootstrap.servers': settings.KAFKA_BROKER,
                'group.id': settings.GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.partition.eof': False,
                'enable.auto.commit': False
            })
        return self._consumer

kafka_service = KafkaConnection()
