# 마이그레이션 계획

## 목적

- 현재 Python MVP에서 목표 스택으로 무리 없이 이관하는 단계별 기준을 정의한다.
- 한 번에 전체 재작성하지 않고, 비교 가능한 상태로 옮기는 것을 원칙으로 한다.

## Phase 0. 동결 기준 잡기

- 레거시 Python `src/`를 기준 구현으로 본다.
- 현재 API, plan, execution contract를 스냅샷으로 남긴다.
- 삭제 전에 필요한 문서만 요약 문서로 압축한다.

## Phase 1. Go control plane 스캐폴드

- `apps/control-plane/`에 API 골격을 만든다.
- health, project, analysis request, execution 조회 API 경계를 먼저 확정한다.
- registry와 contract 읽기 계층을 Go에서 분리한다.

## Phase 2. Workflow를 Temporal로 이관

- `queued`, `running`, `waiting`, `succeeded`, `failed`를 Temporal workflow 상태로 옮긴다.
- `retry_waiting`, rerun, diff를 workflow 기준으로 재정의한다.
- 기존 수동 polling worker는 점진적으로 제거한다.

## Phase 3. Structured 실행을 DuckDB로 이관

- `aggregate`, `rank`, `compare_period` 같은 structured support Skill부터 옮긴다.
- 기존 Python CSV loop는 비교 기준으로만 남긴다.
- dataset contract를 DuckDB friendly 형태로 고정한다.

## Phase 4. Python AI worker 분리

- planner, embeddings, semantic search, evidence generation을 `workers/python-ai/`로 옮긴다.
- control plane에서 직접 LLM을 호출하지 않는다.
- 비정형 `waiting`은 embedding readiness 기반 workflow로 통합한다.

## Phase 5. Rust hot skill worker 추가

- clustering, dedup, keyword cooccurrence, 대규모 토큰화 같은 병목 Skill을 분리한다.
- 모든 Skill을 Rust로 옮기지 않는다.
- 성능이 증명된 hot path만 옮긴다.

## Phase 6. 레거시 정리

- 새 구조에서 최소 E2E 시나리오가 안정화되면 `src/` 제거 계획을 세운다.
- Docker, CI, local dev script를 새 디렉터리 기준으로 정리한다.
- 레거시 Python 전용 문서와 스크립트를 단계적으로 제거한다.

## 우선순위

1. control plane 경계 고정
2. workflow 이관
3. structured Skill 이관
4. AI worker 분리
5. Rust 최적화

## 완료 기준

- 같은 request와 dataset version에서 새 구조가 동일한 수준의 plan/execution 결과를 낸다.
- `waiting/retry/resume`가 worker 스크립트가 아니라 workflow로 관리된다.
- structured / unstructured 대표 시나리오 1개 이상이 새 구조에서 동작한다.
- 레거시 Python 문서를 더 이상 기준 문서로 보지 않아도 된다.
