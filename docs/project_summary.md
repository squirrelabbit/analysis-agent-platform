# 프로젝트 요약

## 1. 한 줄 정의

- 확인 필요: 저장소 루트에서 `project_context.yaml`은 확인되지 않았다.
- 이 프로젝트는 질문을 재실행 가능한 `Skill Plan`으로 고정하고, 실행 결과를 `result / rerun / diff` 단위로 남기는 분석 실행 플랫폼이다.

## 2. 핵심 흐름

- 프로젝트와 dataset, dataset version을 등록한다.
- 원본 dataset을 upload한 뒤 필요하면 `prepare`, `sentiment`, `embedding` 산출물을 만든다.
- 분석 요청을 제출하면 planner가 최소 skill plan을 만들고, Temporal workflow가 실행과 `waiting / resume`를 오케스트레이션한다.
- 실행 결과는 artifact와 execution metadata로 남고, 같은 execution context 기준으로 `rerun / diff` 할 수 있다.

## 3. 현재 런타임 경계

- `Go control plane`
  - 프로젝트, dataset, analysis request, execution API
- `Temporal runtime`
  - execution lifecycle와 `waiting / resume`
- `DuckDB`
  - 현재 연결된 structured skill은 `structured_kpi_summary` 1종
- `Python AI worker`
  - planner, task router, runtime helper, dataset build task, 비정형 support/core skill
- `Rust worker`
  - 확인 필요: 저장소에는 스캐폴드가 있으나 현재 실행 hot path에는 연결되지 않았다.

## 4. 현재 상태

- dataset build task `dataset_prepare`, `sentiment_label`, `embedding`이 연결돼 있다.
- 비정형 support/core skill은 taxonomy, dedup, clustering 계열까지 현재 실행 경로에 포함된다.
- `dataset_prepare`는 Anthropic prepare가 켜지면 batch 정제를 사용하고, `issue_evidence_summary`는 prior artifact를 `analysis_context`로 재사용한다.
- planner/evidence/prepare/sentiment/embedding artifact는 현재 `usage` metadata를 남기고, 가격 env가 설정되면 `estimated_cost_usd`를 함께 계산한다.
- dataset build artifact는 현재 `prepare/sentiment/chunk=Parquet`, `embedding=JSONL sidecar + index source parquet` 구성이며 `row_id`, `prepared_ref`, `sentiment_ref`, `embedding_ref`, `embedding_index_source_ref` 같은 metadata를 함께 남긴다.
- dataset version metadata에는 현재 `prepare_usage`, `sentiment_usage`, `embedding_usage`가 저장되고, execution result contract에는 실행 artifact 기준 `usage_summary`가 집계된다.
- `sentiment.parquet`는 현재 `row_id`, `source_row_index`, 감성 컬럼 중심 sidecar이고, `issue_sentiment_summary`는 prepared dataset ref를 받아 텍스트를 조인한다.
- `embedding`은 현재 `chunks.parquet`를 먼저 만들고, 기본 `embedding_model=intfloat/multilingual-e5-small` 기준으로 FastEmbed local model dense vector를 생성한다. 결과는 `embeddings.jsonl` fallback sidecar와 `embeddings.index.parquet` index source로 함께 남긴다. 필요하면 OpenAI model override를 줄 수 있고, dense 호출이 불가하면 `token-overlap-v1`로 fallback한다.
- control plane은 embedding build 직후 `embeddings.index.parquet`를 우선 읽어 dense vector가 있으면 그대로, 없으면 64차원 hashed projection vector로 바꿔 `pgvector` 테이블 `embedding_index_chunks`에 적재한다. index source가 없을 때만 `embeddings.jsonl` fallback을 사용한다.
- `semantic_search`는 현재 `pgvector`를 우선 조회하고, index metadata가 dense model이면 같은 model로 query vector를 다시 만든다. 분석 plan과 worker input도 `embedding_index_ref + chunk_ref`를 우선 사용하고, `embedding_uri`는 명시적 fallback일 때만 읽는다. 반환 artifact에는 `retrieval_backend`, `chunk_id`, `chunk_index`, `char_start`, `char_end`, `chunk_ref`를 함께 남긴다.
- `embedding_cluster`는 현재 `pgvector` index와 `chunks.parquet`를 우선 읽고, dense vector가 있으면 lexical guardrail을 둔 `dense-hybrid` similarity를 사용한다. generic overlap fixture가 unit test에 추가됐고, `pgvector`를 읽을 수 없을 때만 `embeddings.jsonl` token-overlap fallback으로 내려간다.
- `issue_evidence_summary`와 `evidence_pack`은 `semantic_search`가 있을 때 chunk citation을 그대로 보존한다. evidence LLM 입력이 커지면 `analysis_context`와 selected document text를 budget 기준으로 compaction하고 artifact에 `prompt_compaction` metadata를 남긴다.
- `dataset_prepare`, `sentiment_label`은 기본 Haiku model을 사용하고 prompt version을 registry로 관리한다.
- plan skill 메타데이터는 공용 `skill bundle`인 `config/skill_bundle.json`으로 중앙화됐다.
- Python worker 내부는 `task_router`, `planner`, `runtime`, `skills/support`, `skills/core` 중심으로 분리됐다.
- 로컬 임베딩 품질 비교용으로 고정 fixture 기반 `evaluate_embedding_model` CLI와 unit test 자산이 추가됐다.
- 상세 skill 목록과 계약은 `docs/skill/skill_registry.md`를 기준으로 본다.
- skill별 분석 기법은 `docs/skill/analysis_techniques.md`에 정리돼 있다.
- Python worker runtime은 현재 `.parquet` reader를 지원하고, `sentiment_label`과 `issue_sentiment_summary`는 Parquet를 직접 읽는다.
- 개발용 compose stack은 현재 `pgvector` 이미지와 `vector` extension을 켜고 `embedding_index_chunks` table까지 만든다.
- 비정형 dataset build는 현재 `prepare/sentiment/chunk Parquet` 단계와 sentiment join, chunk citation 경로까지 반영됐고, vector index 전환안은 `docs/architecture/unstructured_storage_transition.md`에 분리해 정리했다.
- GitHub Actions CI는 Python worker 테스트와 Go 테스트/빌드를 현재 구조 기준으로 실행한다.

## 5. 문서 구분

- `docs/project_summary.md`
  - 현재 제품 정의와 실행 흐름의 짧은 스냅샷
- `docs/devlog/`
  - 매일의 고민, 챌린지, 실험 메모, 다음 액션 기록
- `docs/chat-notes/`
  - 확정된 결정 로그와 Codex 대화 보관본

## 6. 확인 필요

- `pgvector` extension과 `embedding_index_chunks` table은 dev stack에서 확인했고, `semantic_search` smoke에서 `retrieval_backend=pgvector`를 확인했다.
- 이번 turn의 `smoke_semantic.sh`, `smoke_cluster.sh`는 `intfloat/multilingual-e5-small` local model 기준으로 다시 실행했고 `embedding_index_backend=pgvector`, `embedding_index_source_format=parquet`, `embedding_vector_dim=384`, `retrieval_backend=pgvector`, `embedding_source_backend=pgvector`, `cluster_similarity_backend=dense-hybrid`를 확인했다.
- Python unit test에는 generic overlap fixture를 추가해 `dense-hybrid`가 `3개 군집`으로 분리되는 회귀 케이스를 고정했다.
- 별도 컨테이너 검증에서도 `intfloat/multilingual-e5-small` local model download와 `fastembed`, `384차원` dense embedding 생성까지 확인했다.
- 확인 필요: OpenAI key를 넣은 dense embedding end-to-end smoke는 이번 turn에 재현하지 않았다.
- `pgvector` 이미지로 바꾼 뒤 기존 volume을 재사용하면 Postgres가 collation version mismatch warning을 출력했다. 개발용 초기화 절차와 `--check-only` 확인 경로는 [reset_postgres_dev.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/reset_postgres_dev.sh), [dev_postgres_reset.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/dev_postgres_reset.md)에 정리했다.
- 확인 필요: `estimated_cost_usd`는 가격 env가 설정된 경우에만 계산되며, 기본 개발 stack에서는 비어 있을 수 있다.
- Rust worker를 실제 hot path로 넘길 성능 기준과 시점은 별도 측정이 필요하다.
