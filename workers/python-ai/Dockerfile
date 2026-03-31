FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY workers/python-ai/src /app/src
COPY config/skill_bundle.json /app/config/skill_bundle.json

EXPOSE 8090

ENTRYPOINT ["python", "-m", "python_ai_worker.main"]
