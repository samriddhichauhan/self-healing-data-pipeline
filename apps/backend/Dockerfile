FROM python:3.11-slim

WORKDIR /app

COPY apps/backend/requirements.txt ./apps/backend/requirements.txt
RUN pip install --no-cache-dir -r apps/backend/requirements.txt

COPY src ./src
COPY airflow/config ./airflow/config
COPY scripts/fault-injection ./scripts/fault-injection
COPY apps/backend ./apps/backend

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "apps.backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
