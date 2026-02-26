FROM python:3.10-slim

WORKDIR /app

COPY CleanService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY CleanService/ ./CleanService/
COPY Shared/ ./Shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "CleanService.main"]
