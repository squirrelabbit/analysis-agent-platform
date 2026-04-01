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
- `src/python_ai_worker/prompt_registry.py`
  - prepare/sentiment prompt version registry
- `src/python_ai_worker/openai_client.py`
  - OpenAI Embeddings API client
- `src/python_ai_worker/runtime/`
  - `constants.py`: 공통 상수
  - `payloads.py`: payload normalize와 기본 입력 merge
  - `common.py`: text/io/date/token helper
  - `artifacts.py`: prior artifact 선택과 집계 helper
  - `embeddings.py`: dense embedding helper와 fallback 판단
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
  - `ANTHROPIC_PREPARE_PROMPT_VERSION`
  - `ANTHROPIC_PREPARE_BATCH_PROMPT_VERSION`
  - `ANTHROPIC_SENTIMENT_PROMPT_VERSION`
  - `ANTHROPIC_API_URL`
  - `ANTHROPIC_VERSION`
  - `ANTHROPIC_MAX_TOKENS`
  - `ANTHROPIC_TIMEOUT_SEC`
  - `OPENAI_API_KEY`
  - `OPENAI_API_URL`
  - `OPENAI_EMBEDDING_MODEL`
  - `OPENAI_EMBEDDING_DIMENSIONS`
  - `OPENAI_EMBEDDING_BATCH_SIZE`
  - `OPENAI_TIMEOUT_SEC`
  - `LOCAL_EMBEDDING_MODEL`
- 기본 LLM 설정:
  - provider: `anthropic`
  - planner/evidence model: `claude-sonnet-4-6`
  - prepare/sentiment model: `claude-3-5-haiku-latest`

## 구현 메모

- `planner`와 `issue_evidence_summary`는 Claude Sonnet을 우선 시도하고 실패 시 deterministic fallback으로 내려간다.
- `dataset_prepare`와 `sentiment_label`은 기본 `ANTHROPIC_PREPARE_MODEL=claude-3-5-haiku-latest`를 사용하고 실패 시 deterministic fallback으로 내려간다.
- prepare/sentiment prompt는 `prompt_registry.py`에서 버전별로 관리하고, 기본 선택은 `ANTHROPIC_PREPARE_PROMPT_VERSION`, `ANTHROPIC_PREPARE_BATCH_PROMPT_VERSION`, `ANTHROPIC_SENTIMENT_PROMPT_VERSION`으로 바꿀 수 있다.
- `dataset_prepare`는 Anthropic prepare 경로가 켜져 있으면 기본 `prepare_batch_size=8` 기준 batch 정제를 사용한다.
- `dataset_prepare` 기본 출력은 `prepared.parquet`이며, 각 row에 `row_id`를 부여하고 `prepared_ref`, `prepare_format=parquet`, `row_id_column`을 함께 남긴다. 명시적으로 `.jsonl` output path를 주면 호환용 JSONL도 계속 생성할 수 있다.
- `sentiment_label` 기본 출력도 `sentiment.parquet`이며 `row_id`, `source_row_index`, `sentiment_ref`, `sentiment_format=parquet` metadata를 함께 남긴다.
- `issue_sentiment_summary`는 `prepared_dataset_name` 입력을 함께 받아 `sentiment.parquet`와 `prepared.parquet`를 join해 텍스트 샘플을 복원한다.
- `embedding` 기본값은 현재 `intfloat/multilingual-e5-small`이고, 입력 row를 text window로 잘라 `chunks.parquet`를 만든 뒤 `fastembed` local model 경로를 우선 시도한다.
- `embedding_model=text-embedding-*` override를 주면 OpenAI dense embedding 경로를 사용할 수 있다.
- 예를 들어 기본값 `embedding_model=intfloat/multilingual-e5-small`이면 local model download 뒤 `384차원` embedding을 만들 수 있다.
- `OPENAI_API_KEY`가 없거나 local/OpenAI dense 호출이 불가하면 `embedding`은 `token-overlap-v1` sidecar로 자동 fallback한다.
- dense가 성공해도 `embeddings.jsonl`에는 기존 `token_counts`, `norm`을 같이 남겨 fallback과 lexical guardrail 경로를 유지하고, 별도 `embeddings.index.parquet`를 index 적재용으로 만든다.
- control plane은 build가 끝난 뒤 `embeddings.index.parquet`를 우선 읽어 dense vector가 있으면 그대로, 없으면 64차원 hashed projection vector로 바꿔 `pgvector` table `embedding_index_chunks`에 적재한다. index source를 못 읽을 때만 `embeddings.jsonl`로 fallback한다.
- `semantic_search`는 현재 `pgvector`를 우선 조회하고, index metadata가 dense model이면 같은 model로 query embedding을 다시 만든다. 불가하면 `embeddings.jsonl`로 fallback한다. 검색 결과에는 `retrieval_backend`, `chunk_id`, `chunk_index`, `char_start`, `char_end`, `chunk_ref`를 함께 남긴다.
- `BuildEmbeddings` request는 `embedding_model` override를 받아 dataset version에 저장된 기본 model을 바꿔 실행할 수 있다.
- `issue_evidence_summary`와 `evidence_pack`은 `semantic_search` prior artifact가 있을 때 chunk citation을 evidence artifact까지 그대로 보존한다.
- `runtime/common.py`는 `.parquet` reader를 지원하므로 `sentiment_label`, `document_filter`, `time_bucket_count` 같은 row 기반 task가 prepared Parquet를 직접 읽을 수 있다.
- `issue_evidence_summary`는 `issue_trend_summary`, `issue_breakdown_summary`, `issue_period_compare`, `issue_cluster_summary`, `issue_taxonomy_summary`, `issue_sentiment_summary` 같은 prior artifact를 `analysis_context`로 반영한다.
- `embedding_cluster`는 현재 `pgvector` index와 `chunks.parquet`를 우선 읽고, dense vector가 있으면 lexical guardrail을 둔 `dense-hybrid` similarity를 우선 사용한다. generic overlap 회귀 fixture가 unit test에 추가됐고, `pgvector`를 읽을 수 없을 때만 `embeddings.jsonl` token fallback을 사용한다.
- 테스트에는 `dense-only`와 `dense-hybrid`를 같은 generic overlap fixture에서 비교하는 helper 케이스가 있고, 현재 기준으로 `dense-only`는 1개 군집으로 붕괴되고 `dense-hybrid`는 `3개 군집`을 유지한다.
- 테스트에는 local embedding fixture를 직접 주입해 `semantic_search` top ranking과 `embedding_cluster` membership이 기대 토픽 그룹을 유지하는지 확인하는 회귀 케이스도 포함한다.
- embedding sidecar record는 `row_id`, `chunk_id`, `chunk_index`, `char_start`, `char_end`를 함께 저장하고, 별도 `chunks.parquet`에는 `chunk_text`와 chunk metadata를 남긴다.
- `deduplicate_documents`는 정규화 텍스트 동일성 + token-set Jaccard similarity를 사용한다.
- `dictionary_tagging`은 rule-based taxonomy tagging을 사용한다.
- `embedding_cluster`는 dense vector가 있으면 dense cosine similarity에 token-overlap guardrail을 곱한 `dense-hybrid` greedy clustering을 사용하고, dense가 없으면 token vector cosine similarity로 fallback한다.
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
