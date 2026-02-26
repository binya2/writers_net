import logging
import os

class CustomLogger:
    _instance = None

    def __new__(cls, service_name="default-service"):
        if cls._instance is None:
            cls._instance = super(CustomLogger, cls).__new__(cls)
            
            # Configuration
            log_level = os.getenv("LOG_LEVEL", "INFO").upper()
            
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            cls._instance.logger = logging.getLogger(service_name)
            
        return cls._instance.logger

logger = CustomLogger("ingestion-service")
