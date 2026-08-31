FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml ./
COPY schemas/ ./schemas/
COPY simulator/ ./simulator/
COPY producer/ ./producer/
COPY consumer/ ./consumer/
COPY notifier/ ./notifier/


# Dev tooling: ruff and pytest. Source is bind-mounted at run time.
FROM base AS dev

RUN pip install --no-cache-dir -e ".[dev]"

COPY tests/ ./tests/

CMD ["pytest"]


FROM base AS runtime

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 1000 heartbeat
USER heartbeat

CMD ["python", "-m", "consumer.consumer"]
