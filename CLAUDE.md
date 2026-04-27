# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 저장소 운영 규칙 (AGENTS.md 요약)

- 코드, 문서, 커밋 메시지는 한국어로 작성한다.
- 커밋 접두사는 `feat:`, `fix:`, `refactor:`, `doc:`만 사용한다. `feat(scope):` 형식은 쓰지 않는다.
- 확인되지 않은 내용은 `확인 필요:`로 표시한다. 추정으로 API 계약, 환경변수, 설정값을 만들지 않는다.
- 요청된 범위 밖의 리팩토링, rename, import 정리, formatting-only 변경은 하지 않는다.
- API 계약이 바뀌면 `docs/api/openapi.yaml`을 반드시 함께 갱신한다.
- Skill을 추가·제거하면 `config/skill_bundle.json`, worker handler, 테스트, `docs/skill/*`를 함께 점검한다.

---

## 아키텍처 개요

LLM 기반 분석 에이전트 플랫폼. 사용자의 자연어 분석 요청을 **Skill Plan**으로 변환해 Temporal workflow로 실행한다.

### 런타임 구성

| 컴포넌트 | 역할 |
| --- | --- |
| **Go control plane** (`apps/control-plane`) | HTTP API, plan normalize, dataset build orchestration, Temporal client |
| **Python AI worker** (`workers/python-ai`) | planner, dataset build, 비정형 skill 실행, final answer 생성 |
| **Temporal** | analysis workflow, dataset build workflow 실행 엔진 |
| **Postgres** (pgvector) | project, dataset, execution, artifact 메타데이터 |
| **DuckDB** | `structured_kpi_summary` — 정형 데이터 집계 엔진 |
| **Rust worker** (`workers/rust-skills`) | 확인 필요: 현재 hot path에 연결되지 않은 스캐폴드 |

### 언어별 책임 경계

- **Go**: API, request validation, Temporal workflow 시작·결과 조회, skill bundle 메타데이터 노출, plan normalize
- **Python**: planner, embedding, semantic search, 비정형 skill, LLM 호출, final answer
- **DuckDB**: structured 집계 (Go에서 직접 호출)
- **Rust**: 향후 고성능 텍스트 처리 후보 (현재 미사용)

### Skill 실행 흐름

```
HTTP 요청 → Go control plane
  → plan normalize & readiness 판정
  → Temporal AnalysisWorkflow
    → 각 skill step: Go가 Python AI worker에 HTTP POST
    → DuckDB skill: Go가 직접 실행
  → result_v1 저장 → final_answer 생성
```

**Skill Registry의 단일 진실 소스**: `config/skill_bundle.json`
- planner, workflow, control plane, worker 모두 이 bundle만 참조한다.
- `engine: python-ai` → Python worker로 라우팅
- `engine: duckdb` → Go에서 DuckDB 직접 실행
- `kind: dataset_build, plan_enabled: false` → dataset 준비 전용 (plan에 포함되지 않음)

### Dataset Build 단계

`source → clean → prepare_sample → prepare → sentiment / embedding → cluster`

- `clean`은 업로드 직후 자동 실행
- `sentiment`와 `embedding`은 `prepare` 이후 병렬 실행 가능
- `cluster`는 `embedding` 완료 후 실행
- build 완료 시 `waiting` 상태의 execution이 자동 재평가됨

### 주요 패키지 위치

**Go (`apps/control-plane/internal/`)**:
- `http/` — 라우팅, Swagger 노출
- `service/` — dataset, execution, scenario, report draft orchestration
- `workflows/` — Temporal analysis/build activity 구현
- `store/` — Postgres/memory 저장소
- `skills/` — Python worker HTTP client, DuckDB runner, planner 연동
- `domain/` — request, execution, dataset 모델
- `executionresult/` — result_v1, final_answer, report draft presenter

**Python (`workers/python-ai/src/python_ai_worker/`)**:
- `task_router.py` — task name → handler 매핑 (모든 skill 진입점)
- `planner.py` — rule-based planner
- `skills/preprocess.py`, `aggregate.py`, `retrieve.py`, `summarize.py` — plan skill 공개 entrypoint
- `skills/dataset_build.py` — prepare/sentiment/embedding/cluster build
- `runtime/` — artifact, embedding, LLM, rule_config helper
- `config/prompts/` — prompt Markdown 템플릿 (버전별 파일명으로 관리)
- `config/skill_policies/` — skill policy JSON

---

## 검증 명령

```bash
# Go 테스트
(cd apps/control-plane && go test ./...)

# Go 빌드
(cd apps/control-plane && go build ./...)

# Python 테스트
PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'

# Python skill contract 검증
PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.run_skill_case --validate

# OpenAPI YAML 구문 검증 (ruby 필요)
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); puts "ok"'
```

API, workflow, dataset build에 영향을 주는 변경이면 관련 smoke script도 확인한다 (`apps/control-plane/dev/smoke*.sh`).

---

## 로컬 실행

```bash
docker compose -f compose.dev.yml up -d --build
```

- control plane: `http://127.0.0.1:18080`
- python-ai worker: `http://127.0.0.1:18090`
- Swagger UI: `http://127.0.0.1:18080/swagger`
- Temporal UI: `http://127.0.0.1:8233` (temporal dev server 기본값)

로컬 API 호출 예시: `docs/api/local.http`

### 주요 환경 변수 (control plane / temporal-worker)

| 변수 | 예시값 |
| --- | --- |
| `TEMPORAL_ADDRESS` | `temporal:7233` |
| `PYTHON_AI_WORKER_URL` | `http://python-ai-worker:8090` |
| `DATABASE_URL` | `postgresql://platform:platform@postgres:5432/analysis_support` |
| `DATA_ROOT` / `UPLOAD_ROOT` / `ARTIFACT_ROOT` | `/workspace/data/...` |
| `SKILL_BUNDLE_PATH` | `config/skill_bundle.json` |

### Python worker 개발용

```bash
# skill 기술 목록 확인
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.main --describe

# embedding 모델 평가
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.evaluate_embedding_model \
  --model intfloat/multilingual-e5-small --format markdown
```

---

## 주요 참조 문서

- `docs/api/openapi.yaml` — 전체 HTTP API 계약 (기준 문서)
- `docs/api/openapi.frontend.yaml` — 프론트 필수 API 계약
- `docs/skill/skill_registry.md` — runtime skill 계약 요약
- `docs/skill/skill_implementation_status.md` — skill별 구현 방식과 안정도
- `docs/architecture/language_roles.md` — 언어별 책임 경계 상세

---

## 현재 트랙 메모

- T4 Skill Surface Consolidation:
  - Phase 1~4 완료
  - Phase 5에서 consumer/caller surface canonical 전환과 alias 종료 시점 문서화 진행 중
- T4 후속 우선순위:
  1. T1 observability Phase 2 dirty 정리 및 재개
  2. Go `buildDefaultPlan()` 정리 sub-track
  3. T3 eval set / architecture evolution
