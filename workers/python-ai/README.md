# Python AI Worker

이 디렉터리는 목표 아키텍처에서 Python이 맡을 AI worker 스캐폴드다.

## 책임

- planner
- dataset prepare
- sentiment label
- embedding 생성
- semantic search
- evidence generation
- 1차 support skill
  - `document_filter`
  - `keyword_frequency`
  - `time_bucket_count`
  - `meta_group_count`
  - `document_sample`
- `dataset_prepare`, `sentiment_label`, `document_filter`, `keyword_frequency`, `time_bucket_count`, `meta_group_count`, `document_sample`, `unstructured_issue_summary`, `issue_breakdown_summary`, `issue_trend_summary`, `issue_period_compare`, `issue_sentiment_summary`, `semantic_search`, `issue_evidence_summary`, `evidence_pack`, `embedding` HTTP task 실행

## 원칙

- workflow 상태를 직접 관리하지 않는다.
- API를 직접 소유하지 않는다.
- contract를 받아서 계산 결과만 반환한다.

## 현재 구현 범위

- `GET /health`
- `GET /capabilities`
- `POST /tasks/planner`
- `POST /tasks/dataset_prepare`
- `POST /tasks/sentiment_label`
- `POST /tasks/embedding`
- `POST /tasks/document_filter`
- `POST /tasks/keyword_frequency`
- `POST /tasks/time_bucket_count`
- `POST /tasks/meta_group_count`
- `POST /tasks/document_sample`
- `POST /tasks/semantic_search`
- `POST /tasks/issue_breakdown_summary`
- `POST /tasks/issue_trend_summary`
- `POST /tasks/issue_period_compare`
- `POST /tasks/issue_sentiment_summary`
- `POST /tasks/issue_evidence_summary`
- `POST /tasks/evidence_pack`
- `POST /tasks/unstructured_issue_summary`

## 실행

- 기본 bind: `127.0.0.1:8090`
- 환경 변수:
  - `PYTHON_AI_WORKER_HOST`
  - `PYTHON_AI_WORKER_PORT`
  - `PYTHON_AI_WORKER_ROLE`
  - `PYTHON_AI_WORKER_QUEUE`
  - `PYTHON_AI_LLM_PROVIDER`
  - `ANTHROPIC_API_KEY`
  - `ANTHROPIC_MODEL`
  - `ANTHROPIC_PREPARE_MODEL`
  - `ANTHROPIC_API_URL`
  - `ANTHROPIC_VERSION`
  - `ANTHROPIC_MAX_TOKENS`
  - `ANTHROPIC_TIMEOUT_SEC`
- 기본 LLM 설정:
  - provider: `anthropic`
  - planner/evidence model: `claude-sonnet-4-6`
  - prepare/sentiment model: `claude-3-5-haiku-latest`
- planner와 `issue_evidence_summary`는 Claude Sonnet 호출을 우선 시도하고, 실패하거나 키가 없으면 deterministic fallback으로 내려간다.
- `dataset_prepare`와 `sentiment_label`은 Claude Haiku 호출을 우선 시도하고, 실패하거나 키가 없으면 deterministic fallback으로 내려간다.
- `embedding`은 token-overlap 기반 sidecar file을 생성하고, `semantic_search`는 그 sidecar를 읽어 점수를 계산한다.
- `dataset_prepare`는 raw row를 `normalized_text` 기준 prepared JSONL artifact로 만들고, downstream unstructured skill이 이 artifact를 우선 사용하게 한다.
- `sentiment_label`은 prepared JSONL artifact에 `sentiment_label`, `sentiment_confidence`, `sentiment_reason` 컬럼을 추가한 sentiment JSONL artifact를 만든다.
- `issue_sentiment_summary`는 sentiment artifact를 읽어 라벨 분포와 대표 예시를 집계한다.
- rule-based planner는 비정형 요청에 대해 support skill을 먼저 배치한다.
  - 일반 요약: `document_filter -> keyword_frequency -> document_sample -> unstructured_issue_summary -> issue_evidence_summary`
  - 추세: `document_filter -> time_bucket_count -> document_sample -> issue_trend_summary -> issue_evidence_summary`
  - 분해: `document_filter -> meta_group_count -> document_sample -> issue_breakdown_summary -> issue_evidence_summary`
  - 비교: `document_filter -> document_sample -> issue_period_compare -> issue_evidence_summary`
  - 감성: `document_filter -> document_sample -> issue_sentiment_summary -> issue_evidence_summary`
- 메타데이터 확인:
  - `PYTHONPATH=src python -m python_ai_worker.main --describe`
