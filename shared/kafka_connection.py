from confluent_kafka import Producer, Consumer
from config import settings

class KafkaConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaConnection, cls).__new__(cls)

            cls._instance.producer = Producer({'bootstrap.servers': settings.KAFKA_BROKER})
            cls._instance.consumer = Consumer({
                'bootstrap.servers': settings.KAFKA_BROKER,
                'group.id': settings.GROUP_ID,
                'auto.offset.reset': 'earliest'
            })
            
            cls._instance.consumer.subscribe([settings.CONSUME_TOPIC])
            cls._instance.consume_topic = settings.CONSUME_TOPIC
            cls._instance.produce_topic = settings.PRODUCE_TOPIC
            
        return cls._instance

kafka_service = KafkaConnection()
