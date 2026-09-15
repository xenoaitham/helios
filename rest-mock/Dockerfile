# HELIOS rest-mock image (ADR-003): Pricing & Promotions mock API.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini ./
COPY restmock/ ./restmock/
COPY tests/ ./tests/
COPY smoke_client.py ./

EXPOSE 8000

CMD ["uvicorn", "restmock.app:app", "--host", "0.0.0.0", "--port", "8000"]
