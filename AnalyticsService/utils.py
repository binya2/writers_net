import json
import os
import re
from collections import Counter
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from Shared.config import settings

logger = get_logger("analytics-service")

analyzer = SentimentIntensityAnalyzer()

STOP_WORDS = {"the", "and", "is", "in", "to", "a", "of", "it", "on", "for", "with", "as", "by", "an", "this", "that", "are", "was", "were", "or", "at", "from", "be", "has", "have", "had", "but", "not", "what", "all", "we", "when", "your", "can", "said", "there", "use", "if", "will", "my", "one", "no", "he", "she", "they", "who", "which", "up", "about"}
WEAPON_PATTERNS = {}

def init_analytics(file_path: str = None):
    global WEAPON_PATTERNS
    file_path = file_path or settings.WEAPONS_FILE
    try:
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                weapons_list = [line.strip().lower() for line in f if line.strip()]
            WEAPON_PATTERNS = {w: re.compile(r'\b' + re.escape(w) + r'\b') for w in weapons_list}
            logger.info(f"Initialized {len(WEAPON_PATTERNS)} weapon patterns.")
    except Exception as e:
        logger.error(f"Failed to load weapons: {e}")

def get_top_10_words(text: str) -> list:
    if not text: return []
    words = text.lower().split()
    filtered = [w for w in words if w not in STOP_WORDS and len(w) > 1]
    return [{"word": item[0], "count": item[1]} for item in Counter(filtered).most_common(10)]

def analyze_sentiment(text: str) -> str:
    if not text: return "Neutral"
    try:
        score = analyzer.polarity_scores(text)['compound']
        if score >= 0.05: return "Positive"
        if score <= -0.05: return "Negative"
    except: pass
    return "Neutral"

def process_message(msg_value: dict):
    image_id = msg_value.get("image_id")
    if not image_id: return
    logger.info(f"Processing Analytics: {image_id}")
    try:
        clean_text = msg_value.get("clean_text", "")
        analytics = {
            "top_10_words": get_top_10_words(clean_text),
            "weapons_found": [w for w, p in WEAPON_PATTERNS.items() if p.search(clean_text.lower())],
            "sentiment": analyze_sentiment(clean_text)
        }
        
        next_event = {
            "image_id": image_id, "filename": msg_value.get("filename", "unknown"),
            "metadata": msg_value.get("metadata", {}), "clean_text": clean_text,
            "analytics": analytics, "status": "analytics_completed"
        }
        kafka_service.producer.produce(settings.PRODUCE_TOPIC, key=image_id.encode('utf-8'), value=json.dumps(next_event).encode('utf-8'))
        kafka_service.producer.flush()
    except Exception as e:
        logger.error(f"Error: {e}")
