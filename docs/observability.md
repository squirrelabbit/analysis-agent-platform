# Observability

> 상태: 2026-04-27 기준 Phase 2 완료
>
> 범위: 현재 구현된 구조화 로깅, Request ID 전파, 주요 이벤트 surface 정리

## 목적

이 문서는 LLM 기반 분석 실행 플랫폼의 현재 observability 구현을 요약한다.

- HTTP 요청 1건이 Go control plane → Temporal workflow/activity → Python AI worker까지 어떻게 추적되는지
- 어떤 이벤트가 현재 구조화 로그로 남는지
- 아직 미구현인 메트릭/트레이싱 범위가 무엇인지

장기 설계나 아이디어가 아니라 **현재 코드 기준으로 확인된 범위만** 기록한다.

## 현재 구현 범위

### Go control plane

- `apps/control-plane/internal/obs/`
  - `slog` 기반 JSON logger 초기화
  - HTTP Request ID 미들웨어
  - Temporal workflow/activity 공통 로깅 래퍼
- `apps/control-plane/internal/http/server.go`
  - `http.request.started`
  - `http.request.completed`
- `apps/control-plane/internal/workflows/analysis_runtime.go`
  - workflow 시작/종료
  - activity 시작/완료/실패
  - skill routing decision
- `apps/control-plane/internal/workflows/dataset_build_runtime.go`
  - dataset build workflow 시작/종료/실패
  - activity 시작/완료/실패
- `apps/control-plane/internal/skills/python_ai_client.go`
  - Python worker HTTP 호출 시 `X-Request-ID` 전파
- `apps/control-plane/internal/service/dataset_build_prepare.go`
- `apps/control-plane/internal/service/dataset_build_sentiment.go`
  - 기존 `log.Printf` fallback을 structured warning으로 전환

### Python AI worker

- `workers/python-ai/src/python_ai_worker/obs/`
  - `structlog` 기반 JSON logger
  - request context bind/clear
  - skill decorator
- `workers/python-ai/src/python_ai_worker/main.py`
  - `http.request.started`
  - `http.request.completed`
  - boot 시작/완료 로그
  - 응답 헤더에 `X-Request-ID` echo
- `workers/python-ai/src/python_ai_worker/task_router.py`
  - `task.dispatch.started`
  - `task.dispatch.completed`
  - `task.dispatch.failed`
  - deprecated alias 호출 시 `deprecated_skill_alias_called`
- `workers/python-ai/src/python_ai_worker/planner.py`
  - planner 시작/완료/폴백 로그
- `workers/python-ai/src/python_ai_worker/runtime/llm.py`
  - `llm.call.started`
  - `llm.call.completed`
  - `llm.call.failed`
- `workers/python-ai/src/python_ai_worker/skills/*.py`
  - public skill entrypoint decorator 적용
- `workers/python-ai/src/python_ai_worker/skills/dataset_build.py`
  - dataset build 관련 warning/skill execution을 structlog 경로로 통일

## Request ID 정책

### HTTP 진입

- Go control plane은 들어온 `X-Request-ID`를 우선 사용한다.
- 없으면 control plane이 새 ID를 생성한다.
- Python AI worker는 Go에서 전달한 `X-Request-ID`를 그대로 bind한다.

### Analysis workflow

- analysis request에서 생성된 `request_id`가 Temporal workflow/activity와 Python skill 실행까지 유지된다.
- 실제 smoke 실행에서 같은 `request_id`가 Temporal worker 로그와 Python worker 로그 양쪽에 남는 것을 확인했다.

### Dataset build workflow

- caller가 `request_id`를 제공하면 그대로 dataset build workflow input으로 전파한다.
- caller request가 없는 build job은 synthetic request ID를 생성한다.
  - 형식: `dataset-build-request-<job_id>`
- 이 경우에도 `job_id`가 build 정체성의 권위 있는 키이고, `request_id`는 상관 추적용이다.

## 현재 로그 이벤트

현재 코드 기준으로 주요 이벤트는 아래와 같다.

| 레이어 | 이벤트 |
| --- | --- |
| Go HTTP | `http.request.started`, `http.request.completed` |
| Temporal workflow/activity | `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.activity.started`, `workflow.activity.completed`, `workflow.activity.failed` |
| Skill routing | `skill.routing.decision` |
| Python HTTP | `http.request.started`, `http.request.completed` |
| Python task router | `task.dispatch.started`, `task.dispatch.completed`, `task.dispatch.failed`, `deprecated_skill_alias_called` |
| Python skill decorator | `skill.executed.started`, `skill.executed.completed`, `skill.executed.failed` |
| Planner | `planner.started`, `planner.completed`, `planner.fallback` |
| LLM | `llm.call.started`, `llm.call.completed`, `llm.call.failed` |
| Boot | `service.boot.started`, `service.boot.completed` |

## 검증

2026-04-27 Phase 2 마감 시점에 아래를 확인했다.

- `cd workers/python-ai && uv run pytest -q`
- `cd apps/control-plane && go test ./...`
- `cd apps/control-plane && go vet ./...`
- `cd apps/control-plane && ./dev/smoke.sh`

smoke 실행에서는 다음을 확인했다.

- canonical skill 이름(`term_frequency`)이 실제 plan/result surface에 반영됨
- 하나의 analysis execution request ID가 Temporal worker와 Python worker 로그 양쪽에 남음
- deprecated alias 호출 시 Python worker에서 warning 로그 1줄이 찍힘

## 현재 미구현

- 분산 트레이싱(OpenTelemetry, Jaeger, Tempo)
- Prometheus 메트릭 노출
- 로그 집계 backend(Loki/ELK 등) 연동
- trace/span 기반 cross-process 시각화

즉, 현재 단계는 **구조화 로깅 + Request ID 상관 추적**까지가 구현 범위다.
