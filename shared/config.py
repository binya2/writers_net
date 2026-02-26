from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    LOG_LEVEL: str = "INFO"
    
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


settings = Settings()
