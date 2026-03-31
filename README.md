# 분석 실행 플랫폼

구현의 중심은 **Go control plane + Temporal workflow + DuckDB + Postgres + Python AI worker** 조합입니다. 
`workers/rust-skills/`는 hot path 최적화 후보를 위한 스캐폴드로만 남아 있습니다.

제품의 핵심은 다음과 같습니다.
- 질문을 `Skill Plan`으로 바꾼다.
- 등록된 Skill만 실행한다.
- 같은 실행 조건으로 `rerun/diff` 할 수 있게 남긴다.
- plan skill 메타데이터는 공용 `skill bundle`로 관리한다.

현재 구현 경계는 다음과 같습니다.
- API, 인증, 실행 제어는 `Go`
- durable workflow와 `waiting/retry/resume`는 `Temporal`
- 구조화 데이터 계산은 `DuckDB`
- 메타데이터와 실행 이력은 `Postgres`
- LLM, 임베딩, 의미 검색은 `Python worker`
- 확인 필요: CPU 집약 Skill의 Rust 이관은 아직 runtime에 연결되지 않았고, 성능 측정 뒤 별도 결정 대상이다.

## 현재 진행 상태

- 현재는 단순 스캐폴드 단계가 아니라, **실행 경로와 테스트가 붙은 MVP 단계**다.
- `Go control-plane + Temporal worker + Postgres + DuckDB + Python AI worker` 조합으로 unit test와 build가 현재 구조 기준으로 통과한다.
- `Claude Sonnet` 기반 planner/evidence generation 경로와 fallback 경로가 Python AI worker에 반영돼 있다.
- `issue_evidence_summary`는 trend/breakdown/compare/cluster/taxonomy/sentiment 계열 prior artifact를 `analysis_context`로 끌어와 근거 설명에 반영한다.
- `dataset_prepare`는 Anthropic prepare 경로가 켜지면 기본 `prepare_batch_size=8` 기준 batch 정제를 수행한다.
- `dataset_prepare`, `sentiment_label`, `embedding` JSONL artifact는 현재 `row_id/ref/format` 메타데이터를 함께 남겨 이후 Parquet 전환의 기반을 잡아 두었다.
- `dataset_prepare`와 `sentiment_label`은 기본 Haiku model을 쓰고, prompt version은 registry와 환경 변수로 선택할 수 있다.
- 비정형 deterministic skill은 Python worker 안에서 `deduplicate_documents`, `dictionary_tagging`, `embedding_cluster`, `cluster_label_candidates`, `issue_cluster_summary`, `issue_taxonomy_summary`까지 확장돼 있다.
- Python AI worker는 현재 `task_router + planner + runtime helper + support/core skill module` 구조로 분리돼 있다.
- Python skill-case devtool은 `python_ai_worker.devtools` 패키지와 `run_skill_case --validate` CLI로 정식 검증 경로를 가진다.
- 비정형 dataset build artifact는 현재 JSONL 중심이고, Parquet + vector index 전환 설계는 `docs/architecture/unstructured_storage_transition.md`에 따로 정리했다.
- 레거시 Python `src/` 디렉터리는 현재 저장소에 없다.
- `workers/rust-skills/`는 아직 실사용 hot path가 연결되지 않은 선택 최적화 경로다.

즉, 저장소는 **새 구조 MVP가 동작 중이고, 운영 기능과 structured 확장을 남겨둔 단계**로 보면 된다.

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
- skill contract 경로
  - 공용 skill bundle: `config/skill_bundle.json`
  - Go control plane과 Python AI worker가 같은 bundle을 읽는다.
  - `/skills`와 planner 기본 입력, worker capability 노출은 이 bundle 기준으로 맞춘다.
- 저장소 경로
  - raw upload는 `UPLOAD_ROOT`
  - prepare/sentiment/embedding 산출물은 `ARTIFACT_ROOT`
  - 현재 기본 포맷은 JSONL이고, 장기 전환안은 `docs/architecture/unstructured_storage_transition.md`를 기준으로 본다.
- 검증 자산
  - Go unit test / build
  - Python unit test
  - Python skill-case devtool validate
  - compose 기반 smoke script

## 구현된 Skill 현황

Core skill:
- `structured_kpi_summary`
- `unstructured_issue_summary`
- `issue_breakdown_summary`
- `issue_cluster_summary`
- `issue_trend_summary`
- `issue_period_compare`
- `issue_sentiment_summary`
- `issue_taxonomy_summary`
- `issue_evidence_summary`

Support skill:
- `dataset_prepare`
- `sentiment_label`
- `document_filter`
- `deduplicate_documents`
- `keyword_frequency`
- `time_bucket_count`
- `meta_group_count`
- `document_sample`
- `dictionary_tagging`
- `embedding`
- `embedding_cluster`
- `cluster_label_candidates`
- `semantic_search`
- `evidence_pack`

현재 방향:
- 코어 스킬 우선 구현은 유지한다.
- 1차 support skill 분리는 완료했고, planner가 support step을 명시적으로 포함한다.
- cluster/taxonomy 계열 비정형 분석도 support skill 조합으로 실행할 수 있다.
- 현재 skill 추가/수정의 메타데이터 변경은 `config/skill_bundle.json` 중심으로 반영한다.

## 현재 실행 흐름

1. 사용자가 분석 요청을 생성한다.
2. Go control plane이 요청과 dataset version을 고정한다.
3. Temporal workflow가 plan 생성, validation, execution, retry를 오케스트레이션한다.
4. Structured step은 DuckDB 기반 Skill로 처리한다.
5. LLM/임베딩/의미 검색 step은 Python AI worker가 처리한다.
6. 고비용 텍스트/클러스터링 step은 현재 Python worker가 처리하고, 필요 시 Rust worker 후보로 분리할 수 있다.
7. 결과, 로그, 실행 메타데이터는 Postgres와 artifact storage에 남긴다.
8. 같은 execution context로 rerun/diff 한다.

저장소에는 개발용 compose 기준으로 아래 smoke 시나리오가 정리돼 있다.
- 기본 비정형 요약
  - `document_filter -> keyword_frequency -> document_sample -> unstructured_issue_summary -> issue_evidence_summary`
- `issue_sentiment_summary -> issue_evidence_summary`
- `semantic_search -> issue_evidence_summary`
- `issue_trend_summary -> issue_evidence_summary`
- `issue_period_compare -> issue_evidence_summary`
- `issue_breakdown_summary -> issue_evidence_summary`
- `deduplicate_documents -> embedding_cluster -> cluster_label_candidates -> issue_cluster_summary -> issue_evidence_summary`
- `dictionary_tagging -> issue_taxonomy_summary -> issue_evidence_summary`

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
- API 문서:
  - Swagger UI: `http://127.0.0.1:18080/swagger`
  - OpenAPI YAML: `http://127.0.0.1:18080/openapi.yaml`
- Python AI worker:
  - `http://127.0.0.1:18090`
  - `GET /capabilities`와 `--describe`는 `skill_bundle_version`을 함께 노출한다.
- 샘플 smoke:
  - `apps/control-plane/dev/smoke.sh`
  - `apps/control-plane/dev/smoke_semantic.sh`
  - `apps/control-plane/dev/smoke_sentiment.sh`
  - `apps/control-plane/dev/smoke_trend.sh`
  - `apps/control-plane/dev/smoke_compare.sh`
  - `apps/control-plane/dev/smoke_breakdown.sh`
  - `apps/control-plane/dev/smoke_cluster.sh`
  - `apps/control-plane/dev/smoke_taxonomy.sh`
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
- skill bundle 기본 경로:
  - `config/skill_bundle.json`
  - 필요 시 `SKILL_BUNDLE_PATH`로 override할 수 있다.
- 기본 비정형 샘플 데이터:
  - [data/issues.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues.csv)
  - [data/issues_sentiment.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_sentiment.csv)
  - [data/issues_trend.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_trend.csv)
  - [data/issues_compare.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_compare.csv)
- smoke 전용 샘플 데이터:
  - [apps/control-plane/dev/testdata/issues_cluster.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/testdata/issues_cluster.csv)
  - [apps/control-plane/dev/testdata/issues_taxonomy.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/testdata/issues_taxonomy.csv)

## 검증 상태

- `cd apps/control-plane && go test ./... && go build ./...`
- `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
- `PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.run_skill_case --validate`
- `docker compose -f compose.dev.yml up -d --build`
- smoke script:
  - `smoke.sh`
  - `smoke_semantic.sh`
  - `smoke_sentiment.sh`
  - `smoke_trend.sh`
  - `smoke_compare.sh`
  - `smoke_breakdown.sh`
  - `smoke_cluster.sh`
  - `smoke_taxonomy.sh`
  - smoke script는 source file을 `/uploads`로 올린 뒤 dataset version을 만들어 host/container 경로 차이를 줄인다.

확인 필요:
- 이번 turn에서는 Python worker 재빌드 후 smoke 8종을 모두 다시 실행했다.
- smoke 재실행 기준으로 `dataset_prepare`는 `dataset-prepare-anthropic-batch-v1`, `prepare batch size: 8` 메타데이터가 기록됐다.
- smoke 재실행 기준으로 `issue_evidence_summary`는 compare/breakdown/trend/cluster/taxonomy/sentiment 시나리오에서 `analysis_context`를 포함했다.

## 디렉터리

- `apps/control-plane/`
  Go 기반 API, execution control plane, dataset/version API, Temporal client, worker runtime
- `workers/python-ai/`
  planner, embeddings, semantic search, evidence generation, unstructured deterministic skill
- `workers/rust-skills/`
  CPU 집약 Skill 커널과 고성능 worker 준비 영역
- `docs/`
  새 목표 구조 기준 문서

## TODO

Must:
- auth / approval / 권한관리 추가
- artifact 외부 저장소 분리

Should:
- structured skill 확장
  - `period_compare_summary`
  - `dimension_breakdown_summary`
  - `topn_rank_summary`
  - `dataset_profile`
  - `aggregate`
  - `rank`
  - `timeseries_peak`
  - `compare_period`
- observability
  - metrics
  - tracing
  - retry / timeout 정책

운영 메모:
- Rust skill worker 전환은 실제 hot path 측정 뒤 결정한다.
- 운영 배포 파이프라인은 현재 CI 기준으로만 정리되어 있다.

## 우선 문서

- [docs/project_summary.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/project_summary.md)
- [docs/devlog/README.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/devlog/README.md)
- [docs/architecture/target_stack.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/target_stack.md)
- [docs/architecture/language_roles.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/language_roles.md)
- [docs/architecture/migration_plan.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/architecture/migration_plan.md)
- [docs/skill/skill_registry.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/skill/skill_registry.md)
- [docs/skill/analysis_techniques.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/skill/analysis_techniques.md)
