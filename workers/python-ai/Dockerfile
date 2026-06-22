FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && python -m pip install --no-cache-dir "fastembed>=0.8,<0.9" "pydantic>=2.8,<3.0" "pyarrow>=21,<22" "duckdb>=1.1,<2.0" "psycopg[binary]>=3.2,<3.3" "structlog>=24,<26" "kiwipiepy>=0.23,<0.24" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY workers/python-ai/src /app/src
# ADR-017 (2026-05-19) — internal task routing 카탈로그 (planner와 분리).
# δ-4 (2026-05-21)로 skill_bundle.json은 삭제됐다 — plan은 planner_v2가
# LLM으로 생성하므로 고정 카탈로그가 필요하지 않다.
COPY config/task_registry.json /app/config/task_registry.json
COPY config/prompts /app/config/prompts
# 5/11: clean 단계 inline noise scrub 패턴 — `_load_noise_patterns`가 tier 4
# config fallback으로 직접 read.
COPY config/noise_patterns /app/config/noise_patterns
# taxonomy-driven config Phase 1 (2026-05-27) — aspect/sentiment taxonomy
# single source. clause_label / planner schema가 점진적으로 여기서 derive.
COPY config/taxonomies /app/config/taxonomies
# ADR-030 — primary_area(대상 축) 공유 taxonomy. clause_label import 시점에 로드되므로
# 누락 시 worker가 import 실패로 crash. taxonomies와 동일하게 반드시 동봉.
COPY config/primary_area /app/config/primary_area

EXPOSE 8090

ENTRYPOINT ["python", "-m", "python_ai_worker.main"]
