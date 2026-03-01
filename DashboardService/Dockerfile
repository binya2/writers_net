FROM python:3.10-slim

WORKDIR /app

COPY DashboardService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY DashboardService/ ./DashboardService/
COPY Shared/ ./Shared/

ENV PYTHONPATH=/app

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

CMD ["python", "-m", "DashboardService.main"]
