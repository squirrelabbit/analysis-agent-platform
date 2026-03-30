# 분석 실행 플랫폼

이 저장소는 Python MVP를 기준으로 시작했지만, 현재 목표 구조는 **Go control plane + Temporal workflow + DuckDB + Postgres + Python AI workers + Rust skill workers** 조합입니다.

제품의 핵심은 그대로 유지합니다.
- 질문을 `Skill Plan`으로 바꾼다.
- 등록된 Skill만 실행한다.
- 같은 실행 조건으로 `rerun/diff` 할 수 있게 남긴다.

다만 구현 방식은 바뀝니다.
- API, 인증, 실행 제어는 `Go`
- durable workflow와 `waiting/retry/resume`는 `Temporal`
- 구조화 데이터 계산은 `DuckDB`
- 메타데이터와 실행 이력은 `Postgres`
- LLM, 임베딩, 의미 검색은 `Python worker`
- CPU 집약 Skill은 `Rust worker`

## 현재 진행 상태

- 현재는 단순 스캐폴드 단계가 아니라, **통합 개발 stack에서 E2E 실행이 되는 MVP 단계**다.
- `Go control-plane + Temporal worker + Postgres + DuckDB + Python AI worker` 조합이 개발용 compose에서 함께 동작한다.
- `Claude Sonnet` 기반 planner/evidence generation 경로와 fallback 경로가 Python AI worker에 반영돼 있다.
- `src/`는 기존 Python MVP 런타임이며, 새 구조 이관이 끝날 때까지 비교 기준으로 남겨둔다.
- `workers/rust-skills/`는 아직 실사용 hot path가 연결되지 않은 준비 단계다.

즉, 저장소는 **새 구조 MVP가 이미 동작하고 있고, 운영 기능과 추가 Skill을 확장하는 단계**로 보면 된다.

## 구현 완료 범위

- 분석 실행 기본 흐름
  - `request -> plan -> execute -> result -> rerun/diff`
- dataset/version 흐름
  - dataset 등록
  - dataset 파일 upload
  - dataset version 등록/조회
  - dataset prepare build
  - sentiment build
  - embedding build
  - `waiting -> resume`
- 실행 엔진
  - `Temporal` workflow 기반 execution lifecycle
  - `Postgres` metadata store
  - `DuckDB` structured execution
  - `Python AI worker` 기반 unstructured execution
- planner / AI 경로
  - 기본 stub planner
  - `PLANNER_BACKEND=python-ai` 경로
  - `Claude Sonnet` 기반 structured JSON 응답 처리
- 저장소 경로
  - raw upload는 `UPLOAD_ROOT`
  - prepare/sentiment/embedding 산출물은 `ARTIFACT_ROOT`
- 검증 자산
  - Go unit test / build
  - Python unit test
  - compose 기반 smoke script

## 구현된 Skill 현황

Core skill:
- `structured_kpi_summary`
- `unstructured_issue_summary`
- `issue_breakdown_summary`
- `issue_trend_summary`
- `issue_period_compare`
- `issue_sentiment_summary`
- `issue_evidence_summary`

Support skill:
- `dataset_prepare`
- `sentiment_label`
- `document_filter`
- `keyword_frequency`
- `time_bucket_count`
- `meta_group_count`
- `document_sample`
- `embedding`
- `semantic_search`
- `evidence_pack`

현재 방향:
- 코어 스킬 우선 구현은 유지한다.
- 1차 support skill 분리는 완료했고, planner가 support step을 명시적으로 포함한다.
- 다음 support 확장 후보는 `deduplicate_documents`, `cluster_label_candidates`, `dictionary_tagging`이다.

## 목표 흐름

1. 사용자가 분석 요청을 생성한다.
2. Go control plane이 요청과 dataset version을 고정한다.
3. Temporal workflow가 plan 생성, validation, execution, retry를 오케스트레이션한다.
4. Structured step은 DuckDB 기반 Skill로 처리한다.
5. LLM/임베딩/의미 검색 step은 Python AI worker가 처리한다.
6. 고비용 텍스트/클러스터링 step은 Rust worker가 처리한다.
7. 결과, 로그, 실행 메타데이터는 Postgres와 artifact storage에 남긴다.
8. 같은 execution context로 rerun/diff 한다.

현재 이 흐름은 개발용 compose 기준으로 다음 시나리오까지 검증돼 있다.
- 기본 비정형 요약
  - `document_filter -> keyword_frequency -> document_sample -> unstructured_issue_summary -> issue_evidence_summary`
- `issue_sentiment_summary -> issue_evidence_summary`
- `semantic_search -> issue_evidence_summary`
- `issue_trend_summary -> issue_evidence_summary`
- `issue_period_compare -> issue_evidence_summary`
- `issue_breakdown_summary -> issue_evidence_summary`

## 개발 실행

- 통합 개발 stack 파일: [compose.dev.yml](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/compose.dev.yml)
- 포함 서비스:
  - `postgres`
  - `temporal`
  - `control-plane`
  - `temporal-worker`
  - `python-ai-worker`
- 실행:
  - `docker compose -f compose.dev.yml up -d --build`
- 기본 API:
  - `http://127.0.0.1:18080`
- Python AI worker:
  - `http://127.0.0.1:18090`
- 샘플 smoke:
  - `apps/control-plane/dev/smoke.sh`
  - `apps/control-plane/dev/smoke_semantic.sh`
  - `apps/control-plane/dev/smoke_sentiment.sh`
  - `apps/control-plane/dev/smoke_trend.sh`
  - `apps/control-plane/dev/smoke_compare.sh`
  - `apps/control-plane/dev/smoke_breakdown.sh`
  - 기본 dataset 경로는 실행 환경을 보고 자동 선택한다.
  - 컨테이너 안에서는 `/workspace/data/...`
  - 호스트에서 직접 실행하면 repo의 `data/...`
- dataset upload:
  - `POST /projects/{project_id}/datasets/{dataset_id}/uploads`
  - `multipart/form-data`
  - 필수 필드: `file`
  - 선택 필드: `data_type`, `metadata`, `prepare_required`, `sentiment_required`, `embedding_required`
- 로컬 저장 기본값:
  - `DATA_ROOT=<repo>/data`
  - `UPLOAD_ROOT=<repo>/data/uploads`
  - `ARTIFACT_ROOT=<repo>/data/artifacts`
- 기본 비정형 샘플 데이터:
  - [data/issues.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues.csv)
  - [data/issues_sentiment.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_sentiment.csv)
  - [data/issues_trend.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_trend.csv)
  - [data/issues_compare.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_compare.csv)

## 검증 상태

- `cd apps/control-plane && go test ./... && go build ./...`
- `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
- `docker compose -f compose.dev.yml up -d --build`
- smoke script:
  - `smoke.sh`
  - `smoke_semantic.sh`
  - `smoke_sentiment.sh`
  - `smoke_trend.sh`
  - `smoke_compare.sh`
  - `smoke_breakdown.sh`
  - 모든 smoke script가 host/container 경로를 자동 선택한다.

확인 필요:
- `issue_evidence_summary`는 현재도 동작하지만, breakdown/compare/trend의 이전 step 산출물을 더 강하게 반영하도록 prompt/context를 고도화할 여지가 있다.
- `dataset_prepare`는 현재 row 단위 Haiku 호출 또는 fallback 정규화를 사용한다. 대용량 dataset 기준 batch 전략 최적화는 이후 단계다.

## 디렉터리

- `apps/control-plane/`
  Go 기반 API, execution control plane, dataset/version API, Temporal client, worker runtime
- `workers/python-ai/`
  planner, embeddings, semantic search, evidence generation, unstructured core skill
- `workers/rust-skills/`
  CPU 집약 Skill 커널과 고성능 worker 준비 영역
- `src/`
  확인 필요: 완전 제거 전까지 참고용으로 남겨두는 Python MVP 런타임
- `docs/`
  새 목표 구조 기준 문서

## TODO

Must:
- auth / approval / 권한관리 추가
- artifact 외부 저장소 분리
- `issue_evidence_summary` 품질 강화

Should:
- structured skill 확장
  - `period_compare_summary`
  - `dimension_breakdown_summary`
  - `topn_rank_summary`
- unstructured core skill 확장
  - `issue_cluster_summary`
  - `issue_taxonomy_summary`
- support skill 2차 확장
  - `deduplicate_documents`
  - `cluster_label_candidates`
  - `dictionary_tagging`
- 범용 waiting / signal 패턴 일반화
- observability
  - metrics
  - tracing
  - retry / timeout 정책

Later:
- Rust skill worker를 실제 hot path에 투입
- legacy `src/` 제거
- 운영 배포 파이프라인 정리

## 우선 문서

- [docs/project_summary.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/project_summary.md)
- [docs/architecture/target_stack.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/target_stack.md)
- [docs/architecture/language_roles.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/language_roles.md)
- [docs/architecture/migration_plan.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/migration_plan.md)
- [docs/skill/skill_registry.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/skill/skill_registry.md)
