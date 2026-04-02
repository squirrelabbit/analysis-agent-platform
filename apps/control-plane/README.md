# Control Plane

이 디렉터리는 현재 런타임에서 Go가 담당하는 control plane 구현체다.

## 책임

- API
- request validation
- dataset/version 관리
- analysis request / plan / execution 제어
- Temporal workflow 시작과 상태 반영
- DuckDB structured skill 실행
- Python AI worker 연동 기반 unstructured skill 실행

## 현재 구현 API

- `GET /health`
- `GET /openapi.yaml`
- `GET /swagger`
- `GET /skills`
- `POST /projects`
- `GET /projects/{project_id}`
- `POST /projects/{project_id}/scenarios`
- `POST /projects/{project_id}/scenarios/import`
- `GET /projects/{project_id}/scenarios`
- `GET /projects/{project_id}/scenarios/{scenario_id}`
- `POST /projects/{project_id}/scenarios/{scenario_id}/plans`
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
- `GET /projects/{project_id}/executions`
- `GET /projects/{project_id}/executions/{execution_id}`
- `GET /projects/{project_id}/executions/{execution_id}/result`
- `POST /projects/{project_id}/executions/{execution_id}/resume`
- `POST /projects/{project_id}/executions/{execution_id}/rerun`
- `GET /projects/{project_id}/executions/diff?from=...&to=...`
- `POST /projects/{project_id}/report_drafts`
- `GET /projects/{project_id}/report_drafts/{draft_id}`

## 현재 연결된 skill 경로

- 현재 plan skill 메타데이터의 runtime source는 `config/skill_bundle.json`이다.
- `GET /skills`, 기본 plan 조합, 입력 기본값, dataset readiness 판정, Python task path routing은 이 bundle 기준으로 동작한다.
- structured plan skill
  - `structured_kpi_summary`
- unstructured plan skill
  - `document_filter`
  - `deduplicate_documents`
  - `keyword_frequency`
  - `time_bucket_count`
  - `meta_group_count`
  - `document_sample`
  - `dictionary_tagging`
  - `embedding_cluster`
  - `cluster_label_candidates`
  - `unstructured_issue_summary`
  - `issue_breakdown_summary`
  - `issue_cluster_summary`
  - `issue_trend_summary`
  - `issue_period_compare`
  - `issue_sentiment_summary`
  - `issue_taxonomy_summary`
  - `semantic_search`
  - `issue_evidence_summary`
  - `evidence_pack`

## build task와 plan skill 구분

- `dataset_prepare`, `sentiment_label`, `embedding`은 worker task로 구현돼 있지만 현재 `/skills` plan skill 목록에는 넣지 않는다.
- 이 세 가지는 analysis plan이 아니라 dataset version 준비 API에서 직접 호출한다.
- bundle에서는 `kind=dataset_build`, `plan_enabled=false`로 구분한다.

## 실행 메모

- 기본 planner backend는 `stub`이다.
- `PLANNER_BACKEND=python-ai`로 두면 plan 생성을 Python AI worker에 위임한다.
- `WORKFLOW_ENGINE=temporal`이면 Go control plane이 Temporal workflow를 시작한다.
- `cmd/temporal-worker`는 execution lifecycle activity와 DuckDB/Python worker runtime을 함께 등록한다.
- execution 목록 API는 `result_v1_snapshot` 기준 preview를 만들고, 보고서 초안 API는 선택한 execution snapshot을 `report-draft-v1`로 저장한다.
- 시나리오 API는 현재 sheet 형태의 분석 시나리오를 `planning_mode=strict` 기준으로 project 단위에 저장하고, 저장된 step을 runtime skill plan으로 바꿔 `analysis_request + plan`을 생성할 수 있다.
- 시나리오 표가 row 형태로 준비돼 있으면 `POST /projects/{project_id}/scenarios/import`로 여러 시나리오를 한 번에 등록할 수 있다. 같은 `scenario_id`는 하나의 시나리오로 묶고, header 값이 충돌하면 에러를 돌린다.
- 현재 자동 plan 생성은 `strict`만 지원한다. 즉 저장된 step을 그대로 실행 plan으로 바꾸고, `runtime_skill_name`이 지정된 step 또는 control plane에 등록된 `function_name -> skill_name` 매핑만 허용한다.
- 직접 매핑되지 않는 step은 `runtime_skill_name`을 명시해야 하고, `guided`나 guardrail 기반 planner 확장은 backlog다.
- 관련 설정:
  - `TEMPORAL_ADDRESS`
  - `TEMPORAL_NAMESPACE`
  - `TEMPORAL_TASK_QUEUE`
  - `DUCKDB_PATH`
  - `PYTHON_AI_WORKER_URL`
  - `PLANNER_BACKEND`
  - `SKILL_BUNDLE_PATH`
  - `DATA_ROOT`
  - `UPLOAD_ROOT`
  - `ARTIFACT_ROOT`
  - `OPENAPI_PATH`

## readiness / waiting 규칙

- 비정형 execution step은 bundle의 `requires_prepare` 기준으로 prepared dataset이 없으면 `waiting`으로 전이된다.
- bundle의 `requires_sentiment=true` skill은 sentiment artifact가 없으면 `waiting`으로 전이된다.
- bundle의 `requires_embedding=true` skill은 embedding artifact가 없으면 `waiting`으로 전이된다.
- 준비가 끝나면 `resume`으로 workflow를 다시 enqueue할 수 있다.

## 검증 메모

- `go test ./...`
- `go build ./...`
- 개발용 smoke script
  - [smoke.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke.sh)
  - [smoke_semantic.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_semantic.sh)
  - [smoke_sentiment.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_sentiment.sh)
  - [smoke_trend.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_trend.sh)
  - [smoke_compare.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_compare.sh)
  - [smoke_breakdown.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_breakdown.sh)
  - [smoke_cluster.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_cluster.sh)
  - [smoke_taxonomy.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_taxonomy.sh)
  - smoke script는 입력 파일을 `/uploads` API로 올린 뒤 dataset version을 생성하므로 host/container 경로 차이에 덜 민감하다.

## 확인 필요

- auth policy와 approval flow는 아직 운영 기능 범위로 남아 있다.
