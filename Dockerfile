FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml ./
COPY simulator/ ./simulator/
COPY producer/ ./producer/
COPY consumer/ ./consumer/

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 1000 heartbeat
USER heartbeat

CMD ["python", "-m", "consumer.consumer"]
