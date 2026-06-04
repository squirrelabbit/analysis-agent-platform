# Control Plane

이 디렉터리는 현재 런타임에서 Go가 맡는 control plane 구현체다. 전체 아키텍처와
빠른 시작은 저장소 루트 [../../README.md](../../README.md)를 본다.

> δ-1~δ-4 (2026-05-21) / ADR-018 β2 정리로 옛 1.x 흐름(execution / scenario /
> report_draft / prepare·sentiment·embedding·cluster build)은 모두 제거됐다.
> 현재 분석은 **planner + executor + composer** (Python) 흐름이고, Go는 API와
> orchestration, 응답 projection을 맡는다.

## 책임

- 프로젝트 / dataset / dataset version / project prompt API
- dataset build job orchestration (Temporal) — `clean / doc_genuineness / clause_label`
- analyze endpoint — active version resolve + artifact_paths 주입 후 Python worker
  `/tasks/analyze` 호출 (LLM 호출은 Python에 위임). 응답의 plan / display projection.
- analysis thread / message / run 저장 (분석 채팅 이력)
- 답변 불가(reject) 중 `unsupported_skill` 케이스를 `planner_rejection_events`에 적재
- 재기동 시 startup reconciliation으로 in-flight build/run 마감

## 코드 구조

| 위치 | 역할 |
| --- | --- |
| `cmd/server` | HTTP 서버 entrypoint |
| `cmd/temporal-worker` | dataset build workflow worker entrypoint (분석은 미등록 — sync HTTP) |
| `internal/http` | 라우팅, OpenAPI/Swagger 노출, CORS, obs middleware |
| `internal/service` | dataset / dataset_build / analyze orchestration + 응답 projection |
| `internal/workflows` | Temporal dataset_build runtime (activity 구현) |
| `internal/store` | Postgres / memory 저장소 (projects, datasets, dataset_versions, build_jobs, analysis_*) |
| `internal/skills` | Python worker HTTP client (`PythonBuildClient`) |
| `internal/registry` | `task_registry.json` loader (`TaskPathFor`) |
| `internal/domain` | dataset, dataset_version 등 도메인 모델 |
| `internal/displaytime` | 다운로드 파일명 등 KST 표시 시간 helper |
| `internal/obs` | Request ID 전파, structured logging |
| `internal/config` | env 기반 설정 로딩 (env 이름의 source of truth) |
| `internal/serviceerror` / `internal/workererror` | 에러 분류 (4xx 매핑, Temporal NonRetryable) |

## 현재 핵심 흐름

1. dataset version 생성 또는 upload
2. upload 직후 `clean` build job 자동 enqueue (Temporal)
3. `clean → doc_genuineness → clause_label` 순으로 dataset build 진행
4. build 완료 시 `waiting` 상태 analysis_run 자동 재평가
5. analyze 요청 → active version resolve + artifact_paths 주입 → Python `/tasks/analyze`
   호출 (planner → executor → composer) → plan/display projection 후 `run.result_json` 저장

## 주요 설정

env 이름과 기본값의 **source of truth는 [internal/config/config.go](internal/config/config.go)**다.
자주 쓰는 것:

- Temporal: `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_TASK_QUEUE` / `TEMPORAL_BUILD_TASK_QUEUE`
- worker 연동: `PYTHON_AI_WORKER_URL` / `PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC` (analyze 호출 timeout, default 120s)
- 저장소: `DATABASE_URL` / `DUCKDB_PATH`
- 경로: `DATA_ROOT` / `UPLOAD_ROOT` / `ARTIFACT_ROOT` / `DATASET_PROFILES_PATH` / `TASK_REGISTRY_PATH`(optional override)
- LLM 비용 상한: `ANTHROPIC_EXECUTION_TOKEN_CEILING`

상세 API와 payload는 [../../docs/api/openapi.yaml](../../docs/api/openapi.yaml)을 기준으로 본다.

## 참고 문서

- 로컬 API 예시: [../../docs/api/local.http](../../docs/api/local.http)
- 테스트 명령과 smoke script: [../../README.md](../../README.md#검증)
- 언어별 책임 경계: [../../docs/architecture/language_roles.md](../../docs/architecture/language_roles.md)
