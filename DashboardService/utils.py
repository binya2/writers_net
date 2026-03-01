import os
from Shared.elastic_connection import elastic_service
from Shared.config import settings
from Shared.logger_config import get_logger

logger = get_logger("dashboard-utils")

def get_weapons_list():
    try:
        file_path = settings.WEAPONS_FILE
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                return sorted([line.strip().lower() for line in f if line.strip()])
    except Exception as e:
        logger.error(f"Failed to load weapons for dashboard: {e}")
    return []

def get_basic_metrics():
    try:
        res = elastic_service.client.count(index=settings.ES_INDEX)
        return {"total_documents": res["count"]}
    except Exception as e:
        logger.error(f"Failed to fetch metrics: {e}")
        return {"total_documents": 0}

def search_documents(query: str = None, sentiment: str = "All", weapon: str = "All"):
    try:
        must_clauses = []
        if query:
            must_clauses.append({"multi_match": {"query": query, "fields": ["results.clean_text", "original_filename"]}})
        
        if sentiment != "All":
            must_clauses.append({"match": {"results.analysis.sentiment": sentiment}})
            
        if weapon != "All":
            must_clauses.append({"match": {"results.analysis.weapons_found": weapon}})
        
        search_body = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else {"match_all": {}}
                }
            }
        }
        
        res = elastic_service.client.search(index=settings.ES_INDEX, body=search_body)
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return []

def get_latest_10_documents():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "size": 10,
                "sort": [{"image_id": "desc"}]
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Failed to get latest documents: {e}")
        return []

def search_israel_docs():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "size": 5,
                "query": {"match": {"results.clean_text": "israel"}}
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Search israel docs failed: {e}")
        return []

def search_multiple_weapons():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "size": 10,
                "query": {
                    "bool": {
                        "filter": {
                            "script": {
                                "script": "doc['results.analysis.weapons_found'].size() >= 3"
                            }
                        }
                    }
                }
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Search multiple weapons failed: {e}")
        return []

def search_positive_palestinian():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "size": 10,
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"results.clean_text": "palestinian"}},
                            {"match": {"results.analysis.sentiment": "Positive"}}
                        ]
                    }
                }
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Search positive palestinian failed: {e}")
        return []

def search_grenade_december():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"results.clean_text": "grenade"}},
                            {
                                "bool": {
                                    "should": [
                                        {"wildcard": {"results.clean_text": "*12-*"}},
                                        {"wildcard": {"results.clean_text": "*-12-*"}}
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Search grenade december failed: {e}")
        return []

def get_all_documents():
    try:
        res = elastic_service.client.search(
            index=settings.ES_INDEX,
            body={
                "size": 1000,
                "query": {"match_all": {}}
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Failed to get all documents: {e}")
        return []
