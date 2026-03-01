FROM python:3.10-slim

WORKDIR /app

COPY AnalyticsService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY AnalyticsService/ ./AnalyticsService/
COPY Shared/ ./Shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "AnalyticsService.main"]
