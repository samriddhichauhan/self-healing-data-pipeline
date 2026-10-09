FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY selfheal_api ./selfheal_api

EXPOSE 8001
CMD ["uvicorn", "selfheal_api.main:app", "--host", "0.0.0.0", "--port", "8001"]
