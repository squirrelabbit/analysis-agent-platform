# Control Plane

이 디렉터리는 새 목표 아키텍처에서 Go가 담당할 control plane 스캐폴드다.

## 책임

- API
- 인증
- request validation
- Temporal workflow 시작/조회
- execution metadata 조회
- Temporal worker에서 execution 상태 반영
- DuckDB 기반 structured skill 실행
- Python AI worker 연동 기반 unstructured skill 실행

## 현재 구현 범위

- `GET /health`
- `GET /openapi.yaml`
- `GET /swagger`
- `GET /skills`
- `POST /projects`
- `GET /projects/{project_id}`
- `POST /projects/{project_id}/datasets`
- `GET /projects/{project_id}/datasets/{dataset_id}`
- `POST /projects/{project_id}/datasets/{dataset_id}/uploads`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions`
- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/prepare`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/sentiment`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/embeddings`
- `POST /projects/{project_id}/analysis_requests`
- `GET /projects/{project_id}/analysis_requests/{request_id}`
- `GET /projects/{project_id}/plans/{plan_id}`
- `POST /projects/{project_id}/plans/{plan_id}/execute`
- `GET /projects/{project_id}/executions/{execution_id}`
- `GET /projects/{project_id}/executions/{execution_id}/result`
- `POST /projects/{project_id}/executions/{execution_id}/resume`
- `POST /projects/{project_id}/executions/{execution_id}/rerun`
- `GET /projects/{project_id}/executions/diff?from=...&to=...`
- `cmd/temporal-worker`로 execution lifecycle activity, DuckDB structured skill, Python AI unstructured skill worker 실행
- 개발용 통합 stack: [compose.dev.yml](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/compose.dev.yml)
- Swagger UI: `http://127.0.0.1:18080/swagger`
- OpenAPI YAML: `http://127.0.0.1:18080/openapi.yaml`
- 개발용 smoke script: [smoke.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke.sh)
- 개발용 semantic search smoke script: [smoke_semantic.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_semantic.sh)
- 개발용 sentiment smoke script: [smoke_sentiment.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_sentiment.sh)
- 개발용 issue trend smoke script: [smoke_trend.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_trend.sh)
- 개발용 issue period compare smoke script: [smoke_compare.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_compare.sh)
- 개발용 issue breakdown smoke script: [smoke_breakdown.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_breakdown.sh)

## 범위 밖

- LLM planner 직접 실행
- CPU 집약 Skill 계산

## 상태

- 현재는 골격만 추가된 상태다.
- 기본 설정은 `STORE_BACKEND=memory`, `WORKFLOW_ENGINE=noop` 이다.
- 기본 planner backend는 `stub`이고, `PLANNER_BACKEND=python-ai`로 전환하면 plan 생성을 Python AI worker에 위임한다.
- `STORE_BACKEND=postgres`와 `WORKFLOW_ENGINE=temporal` 전환 경계는 코드에 반영되어 있다.
- `WORKFLOW_ENGINE=temporal`이면 Go control plane이 Temporal SDK로 workflow start를 시도한다.
- `cmd/temporal-worker`는 `analysis.execution.v1` workflow와 execution lifecycle activity, DuckDB structured skill, Python AI unstructured skill activity를 묶어서 실행한다.
- 관련 설정은 `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE`, `DUCKDB_PATH`, `PYTHON_AI_WORKER_URL`, `PLANNER_BACKEND`, `DATA_ROOT`, `UPLOAD_ROOT`, `ARTIFACT_ROOT`, `OPENAPI_PATH`다.
- 현재 DuckDB structured skill은 `structured_kpi_summary` 1종만 연결되어 있다.
- `dataset_name`은 DuckDB table 이름 또는 `.csv` / `.parquet` 파일 경로로 해석한다.
- dataset/version API, dataset upload API, embedding build API가 연결되어 있다.
- dataset/version API에는 `prepare -> embeddings(optional) -> analysis` 준비 흐름이 연결되어 있다.
- upload된 raw 파일은 `UPLOAD_ROOT`에 저장되고, prepare/sentiment/embedding 산출물은 `ARTIFACT_ROOT` 아래 version 디렉터리에 저장된다.
- 현재 Python AI worker는 `dataset_prepare`, `sentiment_label`, `unstructured_issue_summary`, `issue_breakdown_summary`, `issue_trend_summary`, `issue_period_compare`, `issue_sentiment_summary`, `semantic_search`, `issue_evidence_summary`, `evidence_pack`, `embedding`을 execution path 또는 dataset 준비 경로에 연결하고, `dataset_name`은 `.csv` / `.jsonl` / `.txt` 파일 경로로 해석한다.
- `issue_trend_summary`도 execution path에 연결되어 있고, `time_column`과 `bucket(day|week|month)` 입력으로 시계열 이슈 추세를 계산한다.
- `issue_period_compare`는 같은 입력에 `window_size`를 더 받아 현재 기간과 직전 기간의 이슈량/용어 차이를 계산한다.
- `dataset_prepare`는 raw text를 `normalized_text` 기준 prepared JSONL artifact로 만들고, 이후 unstructured step은 prepared artifact를 우선 사용한다.
- `sentiment` dataset build API는 prepared artifact를 입력으로 받아 `sentiment_label`이 포함된 JSONL artifact를 만든다.
- `issue_sentiment_summary`는 sentiment artifact가 준비되지 않으면 workflow가 `waiting`으로 전이되고, sentiment build 후 `resume`으로 재개할 수 있다.
- 비정형 execution step는 dataset version prepare가 준비되지 않으면 workflow가 `waiting`으로 전이되고, prepare 후 `resume`으로 재개할 수 있다.
- `semantic_search` step는 prepare 이후에도 embedding이 준비되지 않으면 workflow가 다시 `waiting`으로 전이되고, embedding build 후 `resume`으로 재개할 수 있다.
- 개발용 smoke는 컨테이너 내부에서는 `/workspace/data/...`, 호스트 직접 실행에서는 repo의 `data/...` 경로를 기본으로 사용한다.
- `smoke_semantic.sh`는 `semantic_search -> issue_evidence_summary` 경로와 `selection_source=semantic_search`를 검증한다.
- `smoke_sentiment.sh`는 `issue_sentiment_summary -> issue_evidence_summary` 경로와 sentiment 분포 산출물을 검증한다.
- `smoke_trend.sh`는 `issue_trend_summary -> issue_evidence_summary` 경로와 trend series 산출물을 검증한다.
- `smoke_compare.sh`는 `issue_period_compare -> issue_evidence_summary` 경로와 기간 비교 delta 산출물을 검증한다.
- `smoke_breakdown.sh`는 `issue_breakdown_summary -> issue_evidence_summary` 경로와 breakdown 산출물을 검증한다.
- planner가 생성하는 기본 비정형 plan은 `document_filter`, `keyword_frequency`, `time_bucket_count`, `meta_group_count`, `document_sample` 같은 1차 support skill을 앞단에 명시적으로 배치한다.
- 메모리 백엔드 기준 `request -> plan -> execute -> result -> rerun -> diff` 스모크 테스트가 `go test ./...`로 통과한다.
- 실사용 기준 worker는 공유 저장소가 필요하므로 `STORE_BACKEND=postgres` 구성이 맞다. `memory`는 단일 프로세스 테스트 전용에 가깝다.
- 확인 필요: auth policy, approval flow, semantic search 품질 고도화는 이후 단계에서 구현한다.
