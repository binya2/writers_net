FROM python:3.10-slim

WORKDIR /app

COPY api_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api_app/ ./api_app/
COPY shared/ ./shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "api_app.main"]
