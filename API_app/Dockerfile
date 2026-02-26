FROM python:3.10-slim

WORKDIR /app

COPY API_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY API_app/ ./API_app/
COPY Shared/ ./Shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "API_app.main"]
