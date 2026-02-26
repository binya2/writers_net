FROM python:3.10-slim

WORKDIR /app

COPY IndexerService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY IndexerService/ ./IndexerService/
COPY Shared/ ./Shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "IndexerService.main"]
