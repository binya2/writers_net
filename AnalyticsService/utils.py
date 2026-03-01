import json
import os
import re
from collections import Counter

from Shared.mongo_connection import mongo_db
from Shared.kafka_connection import kafka_service
from Shared.logger_config import get_logger
from Shared.config import settings

from nltk.sentiment.vader import SentimentIntensityAnalyzer

logger = get_logger("analytics-service")

analyzer = SentimentIntensityAnalyzer()

STOP_WORDS = {
    "the", "and", "is", "in", "to", "a", "of", "it", "on", "for",
    "with", "as", "by", "an", "this", "that", "are", "was", "were", "or",
    "at", "from", "be", "has", "have", "had", "but", "not", "what", "all",
    "we", "when", "your", "can", "said", "there", "use", "if", "will", "my",
    "one", "no", "he", "she", "they", "who", "which", "will", "up", "about"
}

WEAPON_PATTERNS = {}


def init_analytics(file_path: str = None):
    global WEAPON_PATTERNS

    if file_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "weapons.txt")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            weapons_list = [line.strip().lower() for line in f if line.strip()]

        WEAPON_PATTERNS = {
            weapon: re.compile(r'\b' + re.escape(weapon) + r'\b')
            for weapon in weapons_list
        }
        logger.info(f"Successfully initialized {len(WEAPON_PATTERNS)} weapon patterns.")

    except FileNotFoundError:
        logger.error(f"Failed to initialize analytics: {file_path} not found!")


def fetch_clean_text(image_id: str) -> str | None:
    document = mongo_db.state_collection.find_one({"image_id": image_id})
    if not document:
        logger.error(f"Document {image_id} not found in MongoDB")
        return None
    return document.get("results", {}).get("clean_text", "")


def get_top_10_words(text: str) -> list:
    if not text:
        return []
    words = text.lower().split()
    filtered_words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
    word_counts = Counter(filtered_words)
    return [{"word": item[0], "count": item[1]} for item in word_counts.most_common(10)]


def find_weapons(text: str) -> list:
    if not text:
        return []
    text_lower = text.lower()
    found_weapons = []
    for weapon, pattern in WEAPON_PATTERNS.items():
        if pattern.search(text_lower):
            found_weapons.append(weapon)
    return found_weapons


def analyze_sentiment(text: str) -> str:
    if not text:
        return "Neutral"
    
    scores = analyzer.polarity_scores(text)
    compound = scores['compound']

    if compound >= 0.05:
        return "Positive"
    elif compound <= -0.05:
        return "Negative"
    else:
        return "Neutral"


def update_db_analytics(image_id: str, top_words: list, weapons: list, sentiment: str):
    mongo_db.state_collection.update_one(
        {"image_id": image_id},
        {"$set": {
            "results.analysis.top_10_words": top_words,
            "results.analysis.weapons_found": weapons,
            "results.analysis.sentiment": sentiment,
            "status": "analytics_completed"
        }}
    )
    logger.info(f"MongoDB updated with analytics for: {image_id}")


def notify_analytics_complete(image_id: str):
    next_event = {
        "image_id": image_id,
        "status": "analytics_completed"
    }
    kafka_service.producer.poll(0)
    kafka_service.producer.produce(
        settings.PRODUCE_TOPIC,
        key=image_id.encode('utf-8'),
        value=json.dumps(next_event).encode('utf-8')
    )
    kafka_service.producer.flush()


def process_message(msg_value: dict):
    image_id = msg_value.get("image_id")
    if not image_id:
        logger.warning("Received message with missing 'image_id' in Analytics. Skipping.")
        return

    logger.info(f"Starting Analytics process for: {image_id}")

    try:
        clean_text = fetch_clean_text(image_id)
        if clean_text is None:
            logger.error(f"Text for image {image_id} not found in MongoDB for analytics.")
            mongo_db.update_failed_status(image_id, "Clean text not found in MongoDB for analytics")
            return

        top_words = get_top_10_words(clean_text)
        weapons = find_weapons(clean_text)
        sentiment = analyze_sentiment(clean_text)

        update_db_analytics(image_id, top_words, weapons, sentiment)
        notify_analytics_complete(image_id)
    except Exception as e:
        logger.error(f"Error analyzing document {image_id}: {e}")
        mongo_db.update_failed_status(image_id, str(e))
