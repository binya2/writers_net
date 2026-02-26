import logging
from Shared.config import settings

def get_logger(service_name):
    logging.basicConfig(
        level=settings.LOG_LEVEL, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(service_name)
