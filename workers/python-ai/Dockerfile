FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && python -m pip install --no-cache-dir "fastembed>=0.8,<0.9" "pyarrow>=21,<22" "psycopg[binary]>=3.2,<3.3" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY workers/python-ai/src /app/src
COPY config/skill_bundle.json /app/config/skill_bundle.json

EXPOSE 8090

ENTRYPOINT ["python", "-m", "python_ai_worker.main"]
