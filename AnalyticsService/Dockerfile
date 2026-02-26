FROM python:3.10-slim

WORKDIR /app

COPY AnalyticsService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m nltk.downloader vader_lexicon

COPY AnalyticsService/ ./AnalyticsService/
COPY shared/ ./shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "AnalyticsService.main"]
