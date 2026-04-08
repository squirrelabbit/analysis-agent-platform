# Control Plane

이 디렉터리는 현재 런타임에서 Go가 맡는 control plane 구현체다.

## 책임

- 프로젝트, dataset, dataset version API
- analysis request, plan, execution API
- scenario 등록과 strict plan 생성
- dataset build job orchestration
- Temporal workflow 시작과 상태 반영
- execution result snapshot, `final_answer`, report draft 저장

## 코드 구조

| 위치 | 역할 |
| --- | --- |
| `cmd/server` | HTTP 서버 entrypoint |
| `cmd/temporal-worker` | analysis/build workflow worker entrypoint |
| `internal/http` | 라우팅, OpenAPI/Swagger 노출 |
| `internal/service` | dataset, execution, scenario, report draft orchestration |
| `internal/workflows` | Temporal analysis/build runtime |
| `internal/store` | memory/postgres 저장소 |
| `internal/skills` | Python worker, DuckDB, planner 연동 |
| `internal/executionresult` | `result_v1`, `final_answer`, report draft presenter |
| `internal/domain` | request, execution, dataset metadata 모델 |

## 현재 핵심 흐름

1. dataset version 생성 또는 upload
2. unstructured version이면 `prepare` build job enqueue
3. execution 시작 전 필요한 `sentiment / embedding / cluster` dependency 계산
4. build job이 끝나면 waiting execution 자동 재평가
5. execution 완료 후 `result_v1 snapshot` 저장
6. `final_answer` 생성 후 execution/result API에 함께 노출

## 주요 설정

- workflow
  - `TEMPORAL_ADDRESS`
  - `TEMPORAL_NAMESPACE`
  - `TEMPORAL_TASK_QUEUE`
  - `TEMPORAL_BUILD_TASK_QUEUE`
- concurrency
  - `TEMPORAL_ANALYSIS_MAX_CONCURRENT_ACTIVITIES`
  - `TEMPORAL_BUILD_MAX_CONCURRENT_ACTIVITIES`
  - `DATASET_BUILD_PREPARE_MAX_CONCURRENT`
  - `DATASET_BUILD_SENTIMENT_MAX_CONCURRENT`
  - `DATASET_BUILD_EMBEDDING_MAX_CONCURRENT`
  - `DATASET_BUILD_CLUSTER_MAX_CONCURRENT`
- runtime path
  - `DATASET_PROFILES_PATH`
  - `SKILL_BUNDLE_PATH`
  - `PYTHON_AI_WORKER_URL`
  - `DUCKDB_PATH`
  - `DATA_ROOT`
  - `UPLOAD_ROOT`
  - `ARTIFACT_ROOT`

상세 API와 payload는 [../../docs/api/openapi.yaml](../../docs/api/openapi.yaml)을 기준으로 본다.

## 참고 문서

- 로컬 운영 입구: [../../manual.md](../../manual.md)
- 로컬 runbook: [../../docs/operations/local_runbook.md](../../docs/operations/local_runbook.md)
- 테스트와 smoke: [../../docs/testing/smoke_and_checks.md](../../docs/testing/smoke_and_checks.md)
- 수동 API 예시: [../../docs/testing/manual_api_walkthrough.md](../../docs/testing/manual_api_walkthrough.md)
- 장애 대응: [../../docs/recovery_guide.md](../../docs/recovery_guide.md)
