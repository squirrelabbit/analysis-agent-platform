# Python AI Worker

이 디렉터리는 현재 런타임에서 Python이 맡는 AI worker와 비정형 deterministic skill 구현체다.

## 책임

- planner
- dataset build task
  - `dataset_prepare`
  - `sentiment_label`
  - `embedding`
- unstructured support skill
  - `document_filter`
  - `deduplicate_documents`
  - `keyword_frequency`
  - `time_bucket_count`
  - `meta_group_count`
  - `document_sample`
  - `dictionary_tagging`
  - `embedding_cluster`
  - `cluster_label_candidates`
  - `semantic_search`
  - `evidence_pack`
- unstructured core skill
  - `unstructured_issue_summary`
  - `issue_breakdown_summary`
  - `issue_cluster_summary`
  - `issue_trend_summary`
  - `issue_period_compare`
  - `issue_sentiment_summary`
  - `issue_taxonomy_summary`
  - `issue_evidence_summary`

## 현재 코드 구조

- `src/python_ai_worker/main.py`
  - HTTP entrypoint
- `src/python_ai_worker/task_router.py`
  - task name -> handler routing
- `src/python_ai_worker/planner.py`
  - planner entrypoint와 rule-based planner
- `src/python_ai_worker/runtime/`
  - `constants.py`: 공통 상수
  - `payloads.py`: payload normalize와 기본 입력 merge
  - `common.py`: text/io/date/token helper
  - `artifacts.py`: prior artifact 선택과 집계 helper
  - `llm.py`: planner/evidence/prepare/sentiment LLM helper
- `src/python_ai_worker/skills/`
  - `dataset_build.py`: `dataset_prepare`, `sentiment_label`, `embedding`
  - `support.py`: filter/dedup/tagging/search/cluster support skill
  - `core.py`: issue summary/evidence/core 분석 skill
- `src/python_ai_worker/tasks.py`
  - 기존 import 호환을 위한 export 레이어만 유지한다.

## 원칙

- workflow 상태를 직접 관리하지 않는다.
- 운영 API를 직접 소유하지 않는다.
- contract를 받아 계산 결과만 반환한다.
- LLM 경로가 실패해도 deterministic fallback을 유지한다.
- planner 기본 입력, capability 노출, plan 허용 skill 목록은 공용 `skill bundle` 기준으로 맞춘다.

## 현재 구현 범위

- `GET /health`
- `GET /capabilities`
- `POST /tasks/planner`
- `POST /tasks/dataset_prepare`
- `POST /tasks/sentiment_label`
- `POST /tasks/embedding`
- `POST /tasks/document_filter`
- `POST /tasks/deduplicate_documents`
- `POST /tasks/keyword_frequency`
- `POST /tasks/time_bucket_count`
- `POST /tasks/meta_group_count`
- `POST /tasks/document_sample`
- `POST /tasks/dictionary_tagging`
- `POST /tasks/embedding_cluster`
- `POST /tasks/cluster_label_candidates`
- `POST /tasks/semantic_search`
- `POST /tasks/issue_breakdown_summary`
- `POST /tasks/issue_cluster_summary`
- `POST /tasks/issue_trend_summary`
- `POST /tasks/issue_period_compare`
- `POST /tasks/issue_sentiment_summary`
- `POST /tasks/issue_taxonomy_summary`
- `POST /tasks/issue_evidence_summary`
- `POST /tasks/evidence_pack`
- `POST /tasks/unstructured_issue_summary`

## skill bundle 연동

- runtime source는 저장소 루트의 `config/skill_bundle.json`이다.
- `GET /capabilities`, `GET /health`, `python -m python_ai_worker.main --describe`는 `skill_bundle_version`을 함께 노출한다.
- rule-based planner의 sequence와 기본 입력값도 bundle에서 읽는다.
- `dataset_prepare`, `sentiment_label`, `embedding`은 bundle에 포함되지만 plan skill이 아니라 dataset build task로 본다.

## 실행 메모

- 기본 bind: `127.0.0.1:8090`
- 환경 변수:
  - `PYTHON_AI_WORKER_HOST`
  - `PYTHON_AI_WORKER_PORT`
  - `PYTHON_AI_WORKER_ROLE`
  - `PYTHON_AI_WORKER_QUEUE`
  - `SKILL_BUNDLE_PATH`
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

## 구현 메모

- `planner`와 `issue_evidence_summary`는 Claude Sonnet을 우선 시도하고 실패 시 deterministic fallback으로 내려간다.
- `dataset_prepare`와 `sentiment_label`은 Claude Haiku를 우선 시도하고 실패 시 deterministic fallback으로 내려간다.
- `dataset_prepare`는 Anthropic prepare 경로가 켜져 있으면 기본 `prepare_batch_size=8` 기준 batch 정제를 사용한다.
- `issue_evidence_summary`는 `issue_trend_summary`, `issue_breakdown_summary`, `issue_period_compare`, `issue_cluster_summary`, `issue_taxonomy_summary`, `issue_sentiment_summary` 같은 prior artifact를 `analysis_context`로 반영한다.
- `embedding`은 token-overlap 기반 sidecar file을 만들고, `semantic_search`와 `embedding_cluster`는 이 sidecar를 사용한다.
- `deduplicate_documents`는 정규화 텍스트 동일성 + token-set Jaccard similarity를 사용한다.
- `dictionary_tagging`은 rule-based taxonomy tagging을 사용한다.
- `embedding_cluster`는 token vector cosine similarity 기반 greedy clustering을 사용한다.
- `cluster_label_candidates`는 cluster top term으로 label 후보를 만든다.
- helper 단위 테스트는 `workers/python-ai/tests/test_runtime_helpers.py`에서 payload/artifact/planner helper를 직접 검증한다.

## rule-based planner 패턴

- 일반 요약:
  - `document_filter -> keyword_frequency -> document_sample -> unstructured_issue_summary -> issue_evidence_summary`
- 추세:
  - `document_filter -> time_bucket_count -> document_sample -> issue_trend_summary -> issue_evidence_summary`
- 분해:
  - `document_filter -> meta_group_count -> document_sample -> issue_breakdown_summary -> issue_evidence_summary`
- 비교:
  - `document_filter -> document_sample -> issue_period_compare -> issue_evidence_summary`
- 감성:
  - `document_filter -> document_sample -> issue_sentiment_summary -> issue_evidence_summary`
- 군집:
  - `document_filter -> deduplicate_documents -> embedding_cluster -> cluster_label_candidates -> issue_cluster_summary -> issue_evidence_summary`
- taxonomy:
  - `document_filter -> dictionary_tagging -> issue_taxonomy_summary -> issue_evidence_summary`

## 메타데이터 확인

- `PYTHONPATH=workers/python-ai/src python -m python_ai_worker.main --describe`

## skill 단위 테스트

- 개별 skill 샘플 케이스 목록 보기
  - `PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_skill_case --list`
- registry와 샘플 케이스 정합성만 빠르게 확인
  - `PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_skill_case --validate`
- 개별 skill 직접 실행
  - `PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_skill_case --skill semantic_search --pretty`
  - `PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_skill_case --skill issue_cluster_summary --pretty --keep-tempdir`
- 기본값은 LLM 호출을 강제로 끄고 deterministic fallback 경로로 실행한다.
  - 실제 키가 있어도 `ANTHROPIC_API_KEY`를 비워서 local case를 안정적으로 재현한다.
  - LLM 포함 경로까지 보고 싶으면 `--allow-llm`을 사용한다.
- `python_ai_worker.devtools` 패키지는 `available_skill_cases`, `run_skill_case`, `validate_skill_cases`를 공개 API로 export한다.
- 자동 검증
  - `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_skill_cases.py'`
  - 이 테스트는 `task_router`에 등록된 모든 skill/task에 대해 샘플 케이스가 존재하는지와 실제 실행이 되는지를 확인한다.
