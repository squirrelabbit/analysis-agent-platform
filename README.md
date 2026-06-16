# 분석 지원 플랫폼

LLM을 planner로 사용해 사용자의 자연어 분석 질문을 **plan_v2**(LLM이 생성하는 분석 계획)로 바꾸고, 이를 **DuckDB로 결정론적으로 실행**해 답을 만드는 분석 에이전트 플랫폼이다.

데이터는 프로젝트와 dataset version 기준으로 관리한다. LLM이 만든 plan은 등록된 8개 표준 skill과 검증 가능한 실행 흐름으로 고정되므로, 분석 담당자가 매번 파일·프롬프트·실행 조건을 수동으로 맞추지 않아도 같은 입력과 plan 기준으로 결과를 추적·재사용할 수 있다.

> δ-1~δ-4 (2026-05-21) 정리로 옛 1.x 흐름(SkillPlan / Temporal 분석 실행 / executions / scenarios / report_drafts)은 모두 제거됐다. 현재는 **planner + executor + analyze** 흐름이다. wire contract `plan_version: "v2"`는 유지된다.

## 무엇을 제공하나

- 프로젝트별 dataset / dataset version / prompt를 관리한다.
- 업로드한 원본 데이터를 `clean → doc_genuineness → clause_label` 단계로 build한다 (ADR-018 β2).
- planner(LLM)가 사용자 질문을 8개 표준 skill 조합의 plan_v2로 만든다.
- executor가 plan_v2 step을 DuckDB로 순차 실행하고, composer가 사용자-facing 답변(본문 + 표/차트 힌트)을 구성한다.
- 분석 채팅(thread/message/run)으로 질문·이어질문 이력을 남기고, 화면 복원 시 plan/표를 다시 보여준다.
- 데이터셋으로 답할 수 없는 질문은 사유(reason)와 함께 거절하고, skill 부족 케이스는 backlog로 적재한다.
- 프론트(`apps/web`)와 OpenAPI / frontend OpenAPI / 로컬 `.http` 호출 파일을 제공한다.

## 핵심 개념

| 개념 | 설명 |
| --- | --- |
| Project | 분석 작업의 최상위 단위. dataset / prompt / 분석 thread를 묶는다. |
| Dataset | SNS 후기처럼 같은 성격의 데이터 묶음. |
| Dataset Version | 실제 분석 대상 업로드 데이터. 분석·build 기록은 version 기준. |
| Active Version | 일반 질의에서 기본으로 사용할 dataset version. |
| Build Stage | `source` → `clean` → `doc_genuineness` → `clause_label` 데이터 준비 단계. |
| plan_v2 | planner(LLM)가 질문을 8개 표준 skill step 목록으로 고정한 계획. |
| Analysis Thread / Run | 분석 채팅 단위. 질문·답변·plan·결과를 thread 안 run으로 기록. |
| Composer display | 답변의 화면 표현(표 + `recommended_view`/`chart_spec` 차트 힌트). |

## 분석 흐름 (stateless analyze + 채팅)

```
POST /projects/{pid}/datasets/{did}/analyze                   ← active version, 새 thread 생성
POST /projects/{pid}/datasets/{did}/analysis_threads/{tid}/messages  ← 이어질문 (body: {"content": ...})
GET  /projects/{pid}/datasets/{did}/analysis_threads/{tid}    ← thread 이력 (messages[].plan/display 복원)

  Go control plane
    → active version resolve + artifact_paths 주입
    → Python worker /tasks/analyze 호출
      → planner (Anthropic) → plan_v2 JSON
      → executor (DuckDB) → step 순차 실행
      → composer → assistant_content + display(+recommended_view/chart_spec)
    → run.result_json에 저장 + 응답 반환
```

- 분석 자체는 stateless sync HTTP다 (Temporal 미사용). 채팅 이력만 Postgres(analysis_threads/messages/runs)에 남는다.
- thread 재조회 시 assistant message에 plan / display projection이 attach되어 화면이 이력을 복원한다.

### 답변 불가 (reject) 처리

planner가 plan으로 풀 수 없는 질문은 `answerable: false` + reason으로 거절한다 (raw row 테이블을 만들지 않음).

| reason | 의미 |
| --- | --- |
| `out_of_dataset_scope` | 데이터셋과 무관한 외부/일반 질문 (예: 오늘 날씨). |
| `unsupported_skill` | 데이터셋 관련이나 현재 skill로 불가 (예: 텍스트 클러스터링). `capability_gap` 동반 → `planner_rejection_events`에 적재(skill upgrade backlog). |
| `missing_data_or_artifact` | 지원 분석이나 필요한 컬럼/아티팩트 부재. |

## plan_v2 / Skill

- planner는 8개 표준 skill(`join / filter / aggregate / compare / calculate / sort / present / summarize`)과 3개 RESERVED input table(`docs / clauses / genuineness`)을 조합해 plan_v2를 만든다.
- skill catalog는 `workers/python-ai/src/python_ai_worker/planner/schema.py:SKILL_CATALOG`로 잠금. validator가 schema / skill 범위 / SQL identifier 안전성을 검사하고 self-correct retry 1회 한다.
- 새 표준 skill을 추가하려면 `planner/schema.py:SKILL_CATALOG` + `executor/skills/` 핸들러 + validator 규칙 + 테스트를 함께 갱신한다.

## Dataset Build 단계

`source → clean → doc_genuineness → clause_label` (ADR-018 β2 결정으로 3단계).

- `clean`: 업로드 직후 자동 실행 (데이터 표준화 + deterministic 정제).
- `doc_genuineness`: `clean` 이후 LLOA 호출로 doc-level 3-tier 진성 분류.
- `clause_label`: `doc_genuineness` 이후 LLOA 호출로 절 분리 + sentiment + aspect 라벨링.
- dataset build workflow만 Temporal로 실행한다.

(prepare / sentiment / embedding / cluster / segment 단계는 ADR-018 β2로 제거됐다.)

## 런타임 구성

| 구성 요소 | 역할 |
| --- | --- |
| Go control plane (`apps/control-plane`) | HTTP API, dataset build orchestration(Temporal), analyze endpoint, 응답 projection |
| Python AI worker (`workers/python-ai`) | planner(LLM) / executor(DuckDB) / composer / dataset build(LLOA·Anthropic) |
| Temporal | **dataset build workflow만** 실행 (분석은 stateless) |
| Postgres (pgvector) | project / dataset / dataset_version / artifact + analysis_threads/messages/runs + planner_rejection_events |
| DuckDB | executor의 plan_v2 step 실행 |
| Vite/React (`apps/web`) | 프론트 SPA (compose `web` 서비스, nginx 정적 serve) |

## 빠른 시작

```bash
docker compose -f compose.dev.yml up -d --build
```

기본 주소:

- web(프론트): `http://127.0.0.1:5173`
- control plane: `http://127.0.0.1:18080`
- python-ai worker: `http://127.0.0.1:18090`
- Swagger UI: `http://127.0.0.1:18080/swagger`

분석/빌드에 LLM이 필요하면 `.env`에 `ANTHROPIC_API_KEY`, `LLOA_API_KEY`를 설정한다 (`.env.example` 참고). 로컬 API 호출 예시는 [docs/api/local.http](docs/api/local.http).

## 배포 (staging / 내부 서버)

내부 서버 `/srv/analysis-agent-platform`에서 운영한다. dev compose는 loopback
바인딩 그대로 두고, `compose.staging.yml` overlay를 얹어 web(`:5173`)과
control-plane(`:18080`)을 `0.0.0.0`으로 노출한다.

```bash
# 1) 최초 1회 — clone (사내 GitLab self-signed 인증서 → SSL 검증 비활성)
cd /srv
GIT_SSL_NO_VERIFY=true git clone https://gitlab.wisenut.kr/data/analysis-agent-platform.git
cd /srv/analysis-agent-platform

# 2) .env 작성 (시크릿 — .env.example 복사 후 키 채우기). 최초 1회.
cp .env.example .env && $EDITOR .env

# 3) 최신 코드로 갱신
GIT_SSL_NO_VERIFY=true git pull origin main

# 4) staging overlay로 기동/재배포
docker compose -f compose.dev.yml -f compose.staging.yml up -d --build
```

이후 재배포는 3)~4)만 반복한다.

> **DB cleanup (필요 시 1회, 수동)** — control-plane은 부팅 시 legacy 테이블을
> 자동 DROP하지 않는다 (silverone 2026-06-04, Codex review #4). ADR-018 β2 이전
> 스키마에서 올라온 서버라면 staging 테이블 정리를 operator가 한 번 직접 실행한다.
> compose up/부팅에 묶지 않는다. ⚠️ 운영 데이터 삭제가 발생할 수 있으므로 사전
> 확인 SELECT 후 실행:
>
> ```bash
> docker compose -f compose.dev.yml exec -T postgres \
>   psql -U platform -d analysis_support -f - < scripts/migrations/0001_drop_legacy_cluster_tables.sql
> ```

노출 주소(서버 IP 기준):

- web(앱): `http://<서버IP>:5173`
- control plane(API 직접): `http://<서버IP>:18080`
- python-ai worker / postgres / temporal 은 내부 네트워크 전용(미노출)

> control-plane은 무인증이다. 외부 노출이므로 **신뢰된 사내망 + 방화벽(5173 /
> 18080 inbound 제한)** 전제에서만 운영한다.

## 검증

```bash
# Go
(cd apps/control-plane && go build ./... && go test ./...)

# Python (requires-python >= 3.11)
PYTHONPATH=workers/python-ai/src python3.11 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'

# OpenAPI YAML parse
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); YAML.load_file("docs/api/openapi.frontend.yaml"); puts "ok"'
```

위 검증을 한 번에 돌리는 통합 러너: `./scripts/ci.sh --no-smoke` (Go test + Python test + OpenAPI parse + boot-time destructive SQL guard). CI(GitHub Actions `.github/workflows/ci.yml`)도 동일 3 job을 돈다.

주요 smoke script (compose dev + 외부 API 필요):

- `scripts/smoke_analyze_service.sh`: Python `execute_analyze_plan` direct 호출 (compose 없이 deterministic 경로).
- `scripts/smoke_analyze_endpoint.sh`: python-ai worker `/tasks/plan` + `/tasks/analyze` 4 case.
- `scripts/smoke_analyze_e2e.sh`: Go control plane `POST /analyze` active version end-to-end.
- `scripts/smoke_preprocess_pipeline.sh`: dataset build (clean → doc_genuineness → clause_label) 단계 smoke.
- `scripts/smoke_doc_genuineness_verify.sh`: doc_genuineness 교차모델 검증(verify) end-to-end smoke.

## 주요 문서

- [docs/api/openapi.yaml](docs/api/openapi.yaml): 전체 HTTP API 계약
- [docs/api/openapi.frontend.yaml](docs/api/openapi.frontend.yaml): 프론트 필수 API 계약
- [docs/api/local.http](docs/api/local.http): 로컬 API 호출 예시
- [docs/skill/skill_registry.md](docs/skill/skill_registry.md): runtime skill 계약
- [docs/skill/skill_implementation_status.md](docs/skill/skill_implementation_status.md): skill별 구현 방식과 안정도
- [docs/architecture/language_roles.md](docs/architecture/language_roles.md): 언어별 책임 경계
- `CLAUDE.md`: 저장소 운영 규칙 + 트랙 메모 (작업 기준 문서)

## 현재 범위

- 현재 단계는 SNS 후기 분석에 맞춘 build pipeline(clean/doc_genuineness/clause_label) + plan_v2 분석 채팅 MVP다.
- 분석은 stateless sync HTTP다 — 장기 실행 plan + 결과 polling(Temporal async)은 후속 ADR 트랙.
- startup reconciliation으로 재기동 후 남아 있던 dataset build job을 다시 평가한다. 런타임 보장 범위는 `GET /runtime_status`로 조회한다.
- 확인 필요: Temporal workflow history 장기 보존은 dev server 기본값을 따른다.
