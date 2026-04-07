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
- `POST /projects/{project_id}/scenarios/{scenario_id}/execute`
- `POST /projects/{project_id}/datasets`
- `GET /projects/{project_id}/datasets/{dataset_id}`
- `POST /projects/{project_id}/datasets/{dataset_id}/uploads`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions`
- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/prepare`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/prepare_jobs`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/sentiment`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/sentiment_jobs`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/embeddings`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/embedding_jobs`
- `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/cluster_jobs`
- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/build_jobs`
- `GET /projects/{project_id}/dataset_build_jobs/{job_id}`
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
- `POST /projects/{project_id}/scenarios/{scenario_id}/execute`는 현재 strict 시나리오에서 `analysis_request + plan`을 만든 뒤 곧바로 execution enqueue까지 묶어서 처리한다.
- dataset version이 unstructured 계열이면 현재 version 생성 직후 `prepare` async job을 자동 enqueue하고, `POST /plans/{plan_id}/execute` 또는 `POST /scenarios/{scenario_id}/execute`는 필요한 step이 요구하는 `sentiment`, `embedding`, `cluster` dependency를 먼저 계산해 자동 build를 시도한다.
- 직접 매핑되지 않는 step은 `runtime_skill_name`을 명시해야 하고, `guided`나 guardrail 기반 planner 확장은 backlog다.
- 관련 설정:
  - `TEMPORAL_ADDRESS`
  - `TEMPORAL_NAMESPACE`
  - `TEMPORAL_TASK_QUEUE`
  - `TEMPORAL_BUILD_TASK_QUEUE`
  - `TEMPORAL_ANALYSIS_MAX_CONCURRENT_ACTIVITIES`
  - `TEMPORAL_BUILD_MAX_CONCURRENT_ACTIVITIES`
  - `DATASET_BUILD_PREPARE_MAX_CONCURRENT`
  - `DATASET_BUILD_SENTIMENT_MAX_CONCURRENT`
  - `DATASET_BUILD_EMBEDDING_MAX_CONCURRENT`
  - `DATASET_BUILD_CLUSTER_MAX_CONCURRENT`
  - `DUCKDB_PATH`
  - `DATASET_PROFILES_PATH`
  - `PYTHON_AI_WORKER_URL`
  - `PLANNER_BACKEND`
  - `SKILL_BUNDLE_PATH`
  - `DATA_ROOT`
  - `UPLOAD_ROOT`
  - `ARTIFACT_ROOT`
  - `OPENAPI_PATH`

## readiness / waiting 규칙

- 현재 기본 정책은 `prepare=eager`, `sentiment/embedding=lazy`다.
- 기본 profile registry는 현재 [dataset_profiles.json](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/dataset_profiles.json) 에 있고, `DATASET_PROFILES_PATH`로 바꿀 수 있다.
- dataset version 생성 시 `profile`을 안 주면 registry의 data type 기본 profile을 resolve해 저장한다.
- `GET /dataset_profiles/validate`는 현재 registry 기본값, prompt template front matter, worker rule catalog를 함께 읽어 prompt/rule 오타와 drift를 점검한다.
- version 생성 시 worker URL이 설정돼 있으면 `prepare`를 먼저 자동 시도한다.
- `prepare_jobs`, `sentiment_jobs`, `embedding_jobs`, `cluster_jobs`는 현재 Temporal workflow로 실행되고, `GET /dataset_build_jobs/{job_id}` 또는 version 단위 `GET /build_jobs`로 상태를 확인할 수 있다.
- build workflow는 현재 `TEMPORAL_BUILD_TASK_QUEUE`를 따로 사용하고, 값을 비우면 `<TEMPORAL_TASK_QUEUE>-build`를 기본값으로 쓴다.
- build job에는 현재 `workflow_id`, `workflow_run_id`, `attempt`, `last_error_type`, `resumed_execution_count`가 저장된다.
- build job 응답에는 현재 `diagnostics.retry_count`, `diagnostics.last_error_message`, `diagnostics.workflow_id`, `diagnostics.workflow_run_id`, `diagnostics.resumed_execution_count`가 추가로 내려간다.
- activity timeout/retry 기본값은 현재 `prepare=20분/4회`, `sentiment=45분/4회`, `embedding=60분/3회`, `cluster=60분/3회`, `backoff=10초 x2 최대 5분`이다.
- worker HTTP timeout은 현재 `prepare=10분`, `sentiment=30분`, `embedding=45분`으로 분리돼 있다.
- worker 동시성 기본값은 현재 `analysis activity=8`, `build activity=4`, `prepare slot=3`, `sentiment slot=2`, `embedding slot=1`, `cluster slot=1`이다.
- control plane 기동 시 현재 startup reconciliation을 한 번 수행한다. 남아 있던 `queued/running` build job은 다시 dispatch하고, `queued/running` execution은 다시 enqueue하며, `waiting` execution은 dependency를 다시 계산해 resume 가능 여부를 재평가한다.
- execution 시작 전에는 plan step을 보고 `requires_prepare`, `requires_sentiment`, `requires_embedding`를 계산한 뒤 필요한 build를 먼저 자동 시도한다.
- 그래도 준비되지 못한 경우에만 workflow가 `waiting`으로 전이된다.
- build job이 완료되면 같은 dataset version을 기다리던 execution은 dependency를 다시 계산한 뒤 자동으로 `resume`된다.
- 수동 `resume`은 자동 orchestration으로 해결하지 못한 외부 의존성 예외를 처리할 때만 사용하면 된다.
- execution/list/result 응답에는 현재 `diagnostics.event_count`, `diagnostics.latest_event_type`, `diagnostics.latest_event_message`, `diagnostics.failure_reason`, `diagnostics.waiting`가 포함된다.
- 운영 장애 대응 절차는 [recovery_guide.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/recovery_guide.md)를 참고한다.
- 확인 필요: dataset build workflow history의 장기 보존 기간은 아직 Temporal 서버 기본값을 따르고 있고, 운영 환경별 실제 동시성 상한은 머신 자원 기준으로 추가 튜닝이 필요하다.

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
  - [smoke_auto_resume_sentiment.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_auto_resume_sentiment.sh)
  - [smoke_auto_resume_embedding.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_auto_resume_embedding.sh)
  - [smoke_final_answer.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_final_answer.sh)
  - smoke script는 입력 파일을 `/uploads` API로 올린 뒤 dataset version을 생성하므로 host/container 경로 차이에 덜 민감하다.

## 확인 필요

- auth policy와 approval flow는 아직 운영 기능 범위로 남아 있다.
