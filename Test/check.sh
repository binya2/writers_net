#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "================================"
echo "   Services Health Check"
echo "================================"

# MongoDB
echo -n "MongoDB:       "
if docker exec mongo mongosh --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q "1"; then
    echo -e "${GREEN}UP${NC}"
else
    echo -e "${RED}DOWN${NC}"
fi

# Elasticsearch
echo -n "Elasticsearch: "
if curl -s http://localhost:9200/_cluster/health 2>/dev/null | grep -qE '"status":"(green|yellow)"'; then
    echo -e "${GREEN}UP${NC}"
else
    echo -e "${RED}DOWN${NC}"
fi

# Kafka
echo -n "Kafka:         "
if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo -e "${GREEN}UP${NC}"
else
    echo -e "${RED}DOWN${NC}"
fi

# API Service
echo -n "API Service:   "
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/docs | grep -q "200"; then
    echo -e "${GREEN}UP${NC}"
else
    echo -e "${RED}DOWN${NC}"
fi

# Dashboard Service (Streamlit Health Check)
echo -n "Dashboard UI:  "
if curl -s http://localhost:8501/_stcore/health | grep -q "ok"; then
    echo -e "${GREEN}UP${NC}"
else
    echo -e "${RED}DOWN${NC}"
fi

echo "================================"