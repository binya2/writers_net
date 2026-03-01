from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # General
    LOG_LEVEL: str = "INFO"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # MongoDB
    MONGO_URI: str = "mongodb://localhost:27017/"
    DB_NAME: str = "reshet_db"

    # Kafka
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_TOPIC: str = "FILE_UPLOADED"
    CONSUME_TOPIC: str = "FILE_UPLOADED"
    PRODUCE_TOPIC: str = "RAW_TEXT_EXTRACTED"
    GROUP_ID: str = "default_group"

    # Ingestion App
    INPUT_FOLDER: str = "./input_images"

    # Elasticsearch
    ES_HOST: str = "http://localhost:9200"
    ES_INDEX: str = "writers_index"

    # Dedicated Dashboard
    DASHBOARD_HOST: str = "0.0.0.0"
    DASHBOARD_PORT: int = 8501

    WEAPONS_FILE: str = "Shared/weapons.txt"

settings = Settings()

