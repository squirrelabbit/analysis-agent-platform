# 마이그레이션 계획

## 목적

- 현재 Python MVP에서 목표 스택으로 무리 없이 이관하는 단계별 기준을 정의한다.
- 한 번에 전체 재작성하지 않고, 비교 가능한 상태로 옮기는 것을 원칙으로 한다.

## Phase 0. 동결 기준 잡기

- 확인 필요: 레거시 Python `src/` 디렉터리는 현재 저장소에 없으므로, 기존 Python MVP는 문서와 남은 artifact 기준으로만 추적할 수 있다.
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
- 현재 구현에는 dedup, taxonomy tagging, clustering 기반 비정형 deterministic skill도 Python worker에 들어가 있다.
- 현재 JSONL artifact 경로를 곧바로 고정 계약으로 보지 않고, `docs/architecture/unstructured_storage_transition.md` 기준으로 `prepared/sentiment/chunk/vector index` 분리를 준비한다.

## Phase 4-1. 비정형 저장 포맷 전환

- `dataset_prepare` 출력은 `prepared.parquet` 중심으로 옮긴다.
- `sentiment_label`은 row 복제 JSONL 대신 `row_id` 기준 sidecar를 우선한다.
- dense retrieval을 위해 `chunk_id`와 vector index를 도입한다.
- control plane과 worker는 파일 경로 문자열보다 `ref + format` 해석 계층을 점진적으로 도입한다.
- 확인 필요: Parquet writer 의존성은 `pyarrow`와 DuckDB Python 경로 중 어느 쪽을 채택할지 합의가 필요하다.

## Phase 5. Rust hot skill worker 추가

- clustering, dedup, keyword cooccurrence, 대규모 토큰화 같은 병목 Skill을 분리한다.
- 모든 Skill을 Rust로 옮기지 않는다.
- 성능이 증명된 hot path만 옮긴다.
- 확인 필요: 현재 저장소의 Rust worker는 아직 runtime hot path에 연결되지 않았다.

## Phase 6. 레거시 정리

- `src/`는 현재 저장소 기준으로 제거된 상태다.
- Docker, CI, local dev script를 새 디렉터리 기준으로 계속 정리한다.
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
