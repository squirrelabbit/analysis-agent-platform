# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 저장소 운영 규칙

- 코드, 문서, 커밋 메시지는 한국어로 작성한다.
- 커밋 접두사는 `feat:`, `fix:`, `refactor:`, `doc:`만 사용한다. `feat(scope):` 형식은 쓰지 않는다.
- 확인되지 않은 내용은 `확인 필요:`로 표시한다. 추정으로 API 계약, 환경변수, 설정값을 만들지 않는다.
- 요청된 범위 밖의 리팩토링, rename, import 정리, formatting-only 변경은 하지 않는다.
- 미사용으로 보이는 코드/파일도 호출 경로 확인 전 임의 삭제하지 않는다.
- API 계약이 바뀌면 `docs/api/openapi.yaml`을 반드시 함께 갱신한다. 프론트 영향이 있으면 `docs/api/openapi.frontend.yaml`도 함께.
- δ-4 (2026-05-21)로 plan은 planner(LLM)가 plan_v2로 직접 생성하므로 plan skill 카탈로그(`config/skill_bundle.json`)는 삭제됐다. plan_v2 8 skill catalog는 `workers/python-ai/src/python_ai_worker/planner/schema.py`의 `SKILL_CATALOG`로 잠금되어 있다. dataset_build task를 추가·제거하면 `config/task_registry.json`, worker handler, 테스트, `docs/skill/*`를 함께 점검한다.
- 검증 실패를 성공처럼 요약하지 않는다. 일부만 확인했으면 일부라고 명시.

---

## 문서 작성 위치 (Obsidian vault)

검토·계획·진단·결정 같은 *프로젝트 문서*는 모두 다음 vault에 적는다. repo의 `docs/`는 *제품 계약/기술 레퍼런스*용으로만 쓴다 (openapi/skill_registry/observability 등).

**기준 위치**: `/Users/silverone/Documents/Obsidian Vault/01-Projects/분석지원시스템/`

| 폴더 | 용도 |
|---|---|
| `검토-raw/` | 사용자 가치 비교, 진단 사이클, ad-hoc 검토 결과 |
| `계획/` | 작업 계획, 일정·분업, 단계 확장 계획 |
| `ADR/` | Architecture Decision Records (`ADR-NNN_<topic>.md`) |
| `디자인/` | UI/UX, 기능 디자인 |
| `프로젝트 허브.md` / `종합요약.md` / `개발기록부_LLM분석플랫폼TF.md` | 인덱스/진행 기록 (vault root) |

문서 cross-link는 obsidian wiki-link `[[파일명]]` 형식 (확장자 생략).

---

## 아키텍처 개요

LLM 기반 분석 에이전트 플랫폼. 사용자의 자연어 분석 요청을 **plan_v2** (LLM 생성, DuckDB 결정론적 실행)로 변환해 처리한다. 옛 1.x 흐름(SkillPlan → AnalysisWorkflow → execution)은 δ-1~δ-4 (2026-05-21)로 모두 제거됐다.

### 런타임 구성

| 컴포넌트 | 역할 |
| --- | --- |
| **Go control plane** (`apps/control-plane`) | HTTP API, dataset build orchestration, analyze endpoint, Temporal client (dataset build only) |
| **Python AI worker** (`workers/python-ai`) | planner (LLM), executor (DuckDB), dataset build |
| **Temporal** | dataset build workflow만 실행. 분석(analyze)은 stateless |
| **Postgres** (pgvector) | project, dataset, dataset_version, artifact 메타데이터 |
| **DuckDB** | executor의 plan_v2 step 실행 엔진 |

### 언어별 책임 경계

- **Go**: API, request validation, dataset build Temporal 호출, analyze endpoint (LLM 호출은 Python에 위임)
- **Python**: planner (LLM plan 생성), executor (DuckDB), dataset build (LLOA / Anthropic)
- **DuckDB**: plan_v2 step 실행 (Python 안에서)

### Analyze 실행 흐름 (analyze, stateless)

```
POST /projects/{pid}/datasets/{did}/analyze                   ← active version
POST /projects/{pid}/datasets/{did}/versions/{vid}/analyze    ← explicit version

  Go control plane
    → active version resolve + artifact_paths 주입
    → Python worker /tasks/analyze 호출
      → planner (Anthropic) → plan_v2 JSON
      → executor (DuckDB) → step 순차 실행
    → response body로 plan + 결과 반환 (DB 저장 없음)
```

**plan_v2 / Task Registry**

- plan_v2 — planner가 8 skill (join / filter / aggregate / compare / calculate / sort / present / summarize)과 3 RESERVED input table (docs / clauses / genuineness)로 plan을 생성한다. catalog는 `workers/python-ai/src/python_ai_worker/planner/schema.py:SKILL_CATALOG`로 잠금. δ-4로 `config/skill_bundle.json`은 삭제됐다.
- `config/task_registry.json` — *control plane이 실행하는 내부 task* (dataset_build).
  - Go: `internal/registry.TaskPathFor("<name>")`로 task_path lookup (hardcoded 금지)
  - Python: `task_registry.task_definition("<name>")` 사용
  - ADR-018 (β2)로 hot path 호출 task는 `dataset_clean` / `dataset_doc_genuineness` / `dataset_clause_label` 3종.

### Dataset Build 단계

`source → clean → doc_genuineness → clause_label`

- `clean`은 업로드 직후 자동 실행
- `doc_genuineness`는 `clean` 이후 LLOA 한 호출씩 doc-level 3-tier 진성 분류
- `clause_label`은 `doc_genuineness` 이후 LLOA 한 호출씩 절 분리 + sentiment + aspect 라벨링
- build 완료 시 `waiting` 상태의 execution이 자동 재평가됨

(ADR-018 β2 결정으로 prepare/sentiment/embedding/segment/embedding_cluster/keyword_index/document_cluster_profile/cluster_build 단계는 사용 안 함.)

**교차검증(verify) + chunking** (ADR-026 / ADR-028 / ADR-029, 2026-06):
- `doc_genuineness` / `clause_label` 둘 다 `verify:true` 옵션으로 **교차모델 검증** 경로
  지원. doc_genuineness=2모델 분류+불일치 judge(ADR-026). clause_label=문장 앵커 단위
  2모델 라벨+reconcile(agree/union/sentiment_auto)+충돌만 judge(ADR-028, 절이 아니라 문장).
- **긴 문서 chunking (ADR-029)**: 문서(row/doc_id)는 안 쪼개고 LLM 호출 단위만 문장 chunk로
  분할. doc_genuineness는 `cleaned_text > max_input_chars(20000)`면 **기본 ON 자동 chunk
  aggregate**(진성 hit 우선, genuine 구간을 `genuine_spans` 기록. `chunking=false`로만 옛
  truncate). clause_label verify는 genuine_spans 구간만 처리 + 자체 chunking. 두 skill은
  공통 `dataset_build/_chunking.py` splitter를 써서 sentence_index가 정합한다.
- worker: `dataset_build/doc_genuineness_verify.py` / `clause_label_verify.py` / `_chunking.py`.

### 주요 패키지 위치

**Go (`apps/control-plane/internal/`)**:
- `http/` — 라우팅, Swagger 노출
- `service/` — dataset, dataset_build, analyze orchestration
- `workflows/` — Temporal dataset_build activity 구현
- `store/` — Postgres/memory 저장소 (projects, datasets, dataset_versions, dataset_build_jobs)
- `skills/` — Python worker HTTP client (`PythonBuildClient`)
- `domain/` — dataset, dataset_version 모델
- `registry/` — `task_registry.json` loader (`TaskPathFor`)

**Python (`workers/python-ai/src/python_ai_worker/`)**:
- `task_router.py` — task name → handler 매핑 (dataset_build + analyze 진입점)
- `planner/` — LLM plan 생성 (`schema.py` / `validator.py` / `prompt.py` / `llm.py`)
- `executor/` — DuckDB plan_v2 step 실행 (`context.py` / `runner.py` / `service.py` / `skills/`)
- `dataset_build/` — clean / doc_genuineness / clause_label build
- `runtime/` — Anthropic / LLOA client wrapper, retry, obs (`llm.py` / `llm_guards.py`)
- `clients/` — Anthropic, LLOA HTTP client
- `config/prompts/` — prompt Markdown 템플릿 (`planner-v2-anthropic-v1.md`, `dataset-doc-genuineness-v1.md`, `dataset-clause-label-v3.md`)

---

## 검증 명령

```bash
# Go 테스트
(cd apps/control-plane && go test ./...)

# Go 빌드
(cd apps/control-plane && go build ./...)

# Python 테스트 (requires-python >= 3.11 — macOS 기본 python3가 3.9면 datetime.UTC 등에서 실패)
PYTHONPATH=workers/python-ai/src python3.11 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'

# OpenAPI YAML 구문 검증 (ruby 필요)
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); puts "ok"'
```

API, workflow, dataset build에 영향을 주는 변경이면 관련 smoke script도 확인한다 (`scripts/smoke_analyze_*.sh`, `scripts/smoke_preprocess_pipeline.sh`, `scripts/smoke_doc_genuineness_verify.sh`). ADR-018로 삭제된 단계용 `apps/control-plane/dev/smoke*.sh`는 제거됨.

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
| `TASK_REGISTRY_PATH` | `config/task_registry.json` (optional override) |

### Python worker 개발용

```bash
# task capability 목록 확인
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.main --describe

# δ-1 이후 devtools/ 폴더는 삭제됐다. eval/smoke는 scripts/smoke_analyze_*.sh 사용.
```

---

## 주요 참조 문서

- `docs/api/openapi.yaml` — 전체 HTTP API 계약 (기준 문서)
- `docs/api/openapi.frontend.yaml` — 프론트 필수 API 계약
- `docs/observability.md` — 현재 observability 구현 범위와 Request ID 정책
- `docs/skill/skill_registry.md` — runtime skill 계약 요약
- `docs/skill/skill_implementation_status.md` — skill별 구현 방식과 안정도
- `docs/architecture/language_roles.md` — 언어별 책임 경계 상세

---

## 현재 트랙 메모

### δ-1 ~ δ-4 (2026-05-21) 대규모 정리 결과

옛 1.x 분석 흐름(SkillPlan / planner v1 / executions / report_drafts / 13 hardcoded plan skill / `skill_bundle.json`)을 모두 제거하고 **planner + executor + analyze_v2 endpoint** stateless 흐름으로 대체.

| 항목 | 상태 |
|---|---|
| 옛 plan layer (skills/, planner/, scenarios) | 삭제 |
| 옛 store (executions / skill_plans / analysis_requests / report_drafts / scenarios) | DROP TABLE |
| `config/skill_bundle.json` + schema.md | 삭제. plan_v2 카탈로그는 `planner/schema.py:SKILL_CATALOG`로 잠금 |
| Python `registries/skill_bundle.py` / `policy.py` / `runtime/payloads.py` | 삭제 |
| Python `runtime/llm.py` | 1250→135줄로 축소 (v2가 쓰는 `_anthropic_client` / `_create_json_response_logged` / `_retry_policy_from_config`만) |
| Python `registries/prompt.py` | 420→160줄로 축소. 옛 render 함수(`render_execution_final_answer_prompt` 등) 제거 |
| openapi.yaml | 3244→1736줄. 옛 paths 5종 + schema 40여종 제거 |
| Go `internal/registry/bundle.go` | 삭제. `/skills` HTTP route + `SKILL_BUNDLE_PATH` env 제거 |
| 검증 | Go all pass, Python v2 168 tests OK, smoke direct-plan + user-question 모두 PASS |

rename PR 완료: code 심볼은 `planner` / `executor`로 정리, HTTP task path는 canonical `/tasks/analyze` + `/tasks/analyze_v2` deprecated alias 유지. wire contract `plan_version: "v2"`도 유지.

### 완료 항목 (2026-06-01 기준)

5/21 이후 closure된 작업. 다시 next priority로 올리지 않는다.

- **rename PR** — code 심볼 `planner` / `executor` 정리, canonical `/tasks/analyze` + `/tasks/analyze_v2` alias 유지. `plan_version: "v2"` 유지.
- **ADR-022 taxonomy-driven config** — Phase 1~4 완료.
- **composer PR-A / PR-C** — 완료. (PR-B는 아래 우선순위에 남음)
- **analysis thread 모델 + display contract** — 완료. plan step별 화면용 display(label, expression) 합성 포함.
- **Q4 plan 안정성 closure** — 완료.
- **clause_label 속도 개선 + 9-aspect taxonomy** — 완료.
- **CI 1차 구성** — **GitHub Actions `.github/workflows/ci.yml`** (go-tests / python-tests(3.11) / web-build / release-guard). smoke 제외. (※ 과거 메모의 "GitLab CI / `.gitlab-ci.yml`"는 오기 — 실제는 GitHub Actions)

### 다음 우선순위 (2026-06-01 갱신)

1. **dataset_versions β2 후속**
   - PR3: service read path cleanup
   - PR4: Python runtime fallback + OpenAPI deprecated field cleanup
   - frontend stage enum 협의
2. **composer PR-B / 답변 품질 개선** — LLM composer는 optional. 도입 전 평가 필요.
3. **demo fixture rebuild** — 7/30 데모 1주 전 operator rebuild.
4. **validator contract refactor** — R1~R5 pilot 완료. 후속은 ADR-021 결정 후 진행.
5. **CI 후속** — smoke / manual staging job은 2차 후보 (docker compose + LLOA/Anthropic 외부 API 의존).

### 옛 트랙 메모 (δ-4 이전, history 보존)

아래는 δ-1~δ-4 이전 트랙 history. 현재 코드와 대응 안 되는 항목이 많지만 의사결정 사슬 추적용으로 둔다.

- T1 Observability:
  - Phase 1~2 완료
  - Go control plane / Temporal / Python worker까지 Request ID 전파와 structured logging 연결 완료
  - `apps/control-plane/dev/smoke.sh` 실제 실행으로 canonical skill surface와 cross-language request correlation 확인
  - **단계 결정 (2026-04-29)**: 7/30 릴리즈는 1단계(logs only)로 도달. POC 이후 트리거 충족 시 2단계(Loki+Grafana, 0.5주)로 확장. 3단계(Prometheus/OpenTelemetry)는 8월 이후 ADR 트랙. 트리거·작업 내용은 vault `관측성_단계_확장계획.md`에 정리. 1단계 동안 새 skill·분기는 반드시 `obs.get(__name__)` 기반 structured event를 남겨야 한다 (관측성 위생).
- T4 Skill Surface Consolidation:
  - Phase 1~5 완료
  - ADR-009 closed
  - ADR-010 closed: keyword_frequency·evidence_pack alias 코드 제거 + DB 마이그레이션 완료 (2026-04-29). artifact value 내부의 잔존 skill_name 필드 2건도 `scripts/migrate_adr010_artifact_skill_names.py`로 정리 완료. 옛 alias 이름이 plan으로 들어오면 `PythonAIClient.Run`에서 fail-loud (Codex adversarial review finding 1 fix).
- ADR-011 Hierarchical Planner:
  - closed (2026-04-29, 7/7 완료)
  - skill_bundle layer 필드 + LAYER_PRECEDENCE, planner_layer_hints, planner_meta(active layer 선택), planner_compose(hard dependency 자동 활성화), 12건 fixture pool 회귀 baseline 도입 완료
  - Codex round-2 후속: `planner_compose._sort_by_precedence`가 skill_name을 식별자로 써서 중복 step(같은 skill 다른 input)을 silent drop하던 결함을 node-id 기반 그래프로 교체.
  - **ADR-012 closed (2026-04-29, Phase 1~5 완료)**: summarize layer가 `issue_summary(view=overview|breakdown|cluster|trend|period_compare|sentiment|taxonomy)` + `issue_evidence_summary` 두 skill로 통폐합. 옛 7개 issue_*_summary 핸들러는 task_router/skill_bundle/skill_cases에서 모두 제거(plain 함수로만 `_summarize_impl.py`에 남아 dispatcher backend 역할). DB는 `scripts/migrate_adr012_consolidated_summary.py`로 6 records(skill_plans 4 + executions 2) 정리, 0건 잔존. ADR-010 closure(alias map empty)도 함께 보존. 후속 부채: structured_kpi_summary 이중 책임(ADR-013 후보), eval set은 T3 Phase 1·2로 일부 진행 중
  - **ADR-012 Phase 5 후속 (2026-04-30, commit `f01bf01f`)**: Go production code가 view를 못 알아서 issue_summary(view=sentiment) 같은 step이 sentiment dataset 라우팅·readiness check·input enrichment에서 누락되던 부채 해소. `resolvedDatasetNameForSkill` / `enrichInputsForSkill` / `planRequiresSentiment` / `requiresSentimentReady` / `refreshIssueSummaryViewInputs` view-aware 처리. Go test 10건 fixture(legacy `issue_*_summary` → `issue_summary(view=...)`) 회복. read-side는 `IssueSummaryViewSkillName` helper로 unwrap (`apps/control-plane/internal/skills/aliases.go`) — ADR-009/010 closure(alias map empty) 보존. CONTRIBUTING.md의 "pre-existing 회귀 10건" 노트 제거.
- Evidence summary 회복:
  - Codex round-3 finding (2026-04-29): 4/24 refactor commit `1243c6e5`이 `issue_evidence_summary`의 deterministic fallback 경로를 제거하고 LLM presenter 부재/실패 시 hard-fail로 바꾼 회귀를 발견. ANTHROPIC_API_KEY 없거나 일시 outage 시 default 비정형 plan이 통째로 실패.
  - 수정: `_run_evidence_summary`에 graceful fallback 분기 복원. fallback 경로는 `quality_tier=heuristic`, `llm_output_parsed_strictly=False`, notes에 `fallback: <reason>` 표시로 degraded mode를 운영자에게 노출. test도 hard-fail 잠금 테스트에서 fallback artifact 검증 테스트로 교체.
- T3 quality eval set:
  - Phase 1 closed (2026-04-29) — `docs/eval/quality_v1/` 신설
  - dataset: `festival_sample_50.csv` (강릉 문화재 야행 SNS 50 rows, 익명화). 기존 `data/festival.csv` (2121 rows, gitignored)에서 stratified sampling
  - cases: `issue_evidence_summary.yaml` 5건 (positive + leak-prevention + negative strict_fail)
  - runner: `python -m python_ai_worker.devtools.run_eval_case [--case <id>] [--report <path>] [--allow-llm]`. default mock LLM, `--allow-llm`로 실 Anthropic 호출
  - regression v1과 분리된 트랙 — regression은 plan 잠금, quality는 skill 출력 의미 검증
  - 후속 Phase: embedding_cluster·cluster_label_candidates·execution_final_answer case + CI 통합 + ADR-012 통폐합 후 재정렬
- LLM guards (J' = retry + ceiling + prompt cache):
  - closed (2026-04-29). Codex adversarial review finding 2(2026-04-29)에 따라 ceiling 위치를 재설계 — Python worker 내부에서 enforce하던 contextvar ledger는 per-HTTP-request scope라 multi-step execution에서 step별로 reset됐다. control-plane(`PythonAIClient.Run`)에서 cumulative usage를 누적·enforce하도록 이동, Python `_CostLedger`/`LLMCostCeilingExceeded`/`cost_ledger_for_execution`/`current_ledger`는 제거. `ANTHROPIC_EXECUTION_TOKEN_CEILING`은 Go config(`AnthropicExecutionTokenCeiling`)에서 읽고, Python config의 동명 필드는 제거.
  - retry는 Python worker `runtime/llm_guards.py`에 그대로 — `with_retry` (429/5xx/connection error backoff). retry 관련 4 env knob: `ANTHROPIC_RETRY_MAX_ATTEMPTS` (3), `ANTHROPIC_RETRY_BASE_DELAY_SEC` (0.5), `ANTHROPIC_RETRY_MAX_DELAY_SEC` (8.0), `ANTHROPIC_PROMPT_CACHE_ENABLED` (true).
  - `AnthropicClient`에 `system` + `cache_control` 지원, `_create_json_response_logged`이 cache_creation/cache_read 토큰을 로그에 기록.
  - prompt template에 `{{__CACHE_BREAK__}}` 마커 도입 — execution-final-answer-v1에 적용. 다른 prompt 통합은 ADR-012/E 작업에서 함께 진행
- ROI 트리오 + silent fallback 후속 (2026-04-30, 5 commits):
  - `42a474af` — `execution.summary` log line (workflow runtime 3 path: waiting/failed/completed). 5분 incident 진단용 단일 라인. event/request_id/execution_id/project_id/status/runner_engine/processed_steps/artifact_count/total_tokens/fallback_notes_count/duration_ms 11개 필드.
  - `95430bf0` — `CONTRIBUTING.md` PR 직전 체크리스트. Skill 추가/변경 9곳 동기화 + 5 silent regression invariant + silent fallback obs warning 패턴 + 검증 명령 + 첫 PR pairing 안내.
  - `1fd508b6` — silent fallback 4건 가시화: `embeddings.py` TextEmbedding init 실패 + supported_models query 실패 + custom model registration 실패 + `artifacts.py` prior artifact JSON 파싱 실패. 각각 obs warning(error_category, error_message, artifact_key 등 구조화 필드).
  - `1ed41ab6` — silent fallback 후속: `runtime/common.py` kiwi tokenize 실패 + kss split 실패 시 obs warning. Codex 라운드 3B 마무리 (skill_policy_registry.py:236은 type guard라 silent fallback 아님).
  - `f01bf01f` — ADR-012 Phase 5 후속 (위 ADR-012 항목 참조).
- Auth/RBAC 결정 (2026-04-30): **(a) 7/30 내부 신뢰 사용자만** (사내망 + 같은 팀 분석가). Auth 작업 미루고 8월 이후 ADR. network restriction이 7/30 전 격리 책임. Codex 라운드 3A 보안 audit이 외부 시범 차단 권고했지만 silverone가 시나리오 (a)로 명시 결정.
- skill.usage instrumentation (2026-04-30): Go control-plane이 매 step 실행마다 `event=skill.usage` 1줄 emit (`apps/control-plane/internal/skills/skill_usage.go` + `python_ai_client.go` + `duckdb_runner.go`). 스키마: skill_name, view, engine, project_id, execution_id, step_id, status(completed|failed|failed_unsupported|failed_ceiling|skipped_*|skipped_unsupported), duration_ms, total_tokens, error_category. 집계 스크립트 `scripts/skill_usage_summary.sh` (awk 기반 — count/ok/fail/skip/avg_ms/tokens/last_seen 표). 운영 가이드 `CONTRIBUTING.md` §5. 8월 prune 결정용 데이터 누적 시작점 (Codex 라운드 2A). 14 emit 위치(python-ai 12 + duckdb 2 + skipped_unsupported 1)로 모든 path 커버.
- Anthropic structured output 호환성 (2026-04-30, commit `d1b3c343`): festival 5 query 검증 중 모든 LLM-backed skill이 100% 실패한 silent regression 진단 + fix. 두 단계:
  - Fix 1 — `_strict_object_schema` helper (`anthropic_client.py`): 모든 nested object의 `additionalProperties:false` 자동 강제. Anthropic structured-output strict mode가 missing/`true`면 HTTP 400 거부. 14 잠금 테스트 (`test_anthropic_client.py` StrictObjectSchemaTests + CreateJsonResponseSchemaInjectionTests).
  - Fix 2 — `planner_schema.inputs`을 `type: string` (JSON 문자열)로 변경 + `_decode_planner_step_inputs` hard-fail parser. Anthropic grammar compiler가 14 properties × nested 조합을 'Schema is too complex' (HTTP 400) 또는 60s 후 503 'Grammar compilation is temporarily unavailable'로 거부했음. 10 잠금 테스트 (`test_planner_inputs_string_contract.py`).
  - 부수: `PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC` (default 120s) + `ANTHROPIC_TIMEOUT_SEC` (default 90s) env 노출. `scripts/festival_5_query.sh` 자동화 CLI.
  - 후속 ADR-014 후보: "schema-driven LLM 분석 플랫폼"이 provider grammar 한도에 종속됨. 8월에 provider-portable parse/validate 계층 분리 검토.
- 사용자 가치 검증 (2026-04-30): festival × ChatGPT 5 query 비교. 결과: **ChatGPT 4/5 우위, Q2 무승부**. 본 플랫폼은 Q1/Q3/Q4/Q5에서 heuristic fallback template ("질문 X 기준 관련 문서를 추렸고, 주요 용어는 ..."). 가치 제안 재포지셔닝 결정: "더 좋은 답변" ❌ → "반복·통제·감사 가능한 분석 절차" ✅. POC 슬라이드 = **Q2 (부정 반응)** + 운영자 가치 단독 슬라이드 1장. 상세: vault `사용자_가치_비교_festival_2026-04.md`.
- 다음 우선순위 (4/30 사용자 가치 검증 완료 후 갱신):
  1. **execution_final_answer prompt 보강** — Q1/Q3/Q4/Q5 fallback template 탈피. 답변 본문 합성이 약한 게 본 플랫폼 가장 큰 약점 (가치 제안 재포지셔닝 후에도). 0.3~0.5주.
  2. GitLab CI 구성 (5월 1주 — Go test + Python test 최소)
  3. **issue_summary skill을 LLM-backed로** — 현 deterministic shell만 만드는 상태라 plan에 있어도 답변 본문 기여 0. T3 quality eval Phase 2 case 작성 후 prompt 작성. 0.5주.
  4. ~~semantic_search query rewrite step~~ — ADR-018 β2로 retrieve layer 전체 삭제됨 (5/19). 항목 폐기.
  5. T3 quality eval Phase 2 — embedding_cluster·cluster_label_candidates·execution_final_answer case 추가. **embedding_cluster·cluster_label_candidates도 ADR-018 β2로 삭제 — execution_final_answer case만 의미 있음.**
  6. (선택) skill_bundle ↔ handler ↔ test cross-reference 자동 생성
  7. (선택) silverone-이교범 첫 1주 pairing protocol 문서화

- ADR-018 (2026-05-19~20) — β2 dataset_build 축소:
  - dataset_build hot path: 4 task → 3 task (`dataset_clean` → `dataset_doc_genuineness` → `dataset_clause_label`). `dataset_document_cluster_profile` 추가 삭제.
  - retrieve layer 전체 삭제 (`embedding_cluster` / `cluster_label_candidates` / `semantic_search`)
  - summarize의 cluster/sentiment view 제거 (issue_summary 5 view 유지: overview / breakdown / trend / period_compare / taxonomy)
  - prepare / sentiment / embedding / segment / embedding_cluster / keyword_index / cluster_build 7 옛 dataset_build task 코드 + endpoint 완전 삭제
  - Go ~30 service / workflow / route 파일 삭제. Python ~10 module + retrieve/_impl 삭제. compat shim 정리. worker 폴더 구조 정리 (`clients/` / `planner/` / `registries/`)
  - openapi.yaml: 17 path + 16 schema 삭제. `_migration_targets.py` 완전 삭제 (canonical_skill_name inline + LEGACY 4 test 제거)
  - Python 252 OK / Go all pass. vault `ADR-018_dataset_build_simplification.md` accepted

- ADR-018 §5/20 후속 — clause_label 속도 최적화 (44× 단축):
  - **결정 4**: (a) prompt 단순화 + `/no_think`, (b) `ThreadPoolExecutor(max_workers=8)` default, (c) `reasoning_effort=low` default (`config.py` + `compose.dev.yml` LLOA_REASONING_EFFORT default low), (d) `include_genuineness` **default ON** — non_review skip (LLOA 호출 ~60% 절감 + 분석 가치 0). caller가 `include_genuineness=[]` 명시 시 모든 doc 처리 opt-out.
  - **측정** (festival 50 doc): 5/19 baseline ~11분 → 5/20 final **49초** (44× 단축). clause_label only 10분 → 15초 (40×).
  - **Trade-off**: `reasoning_effort=low`로 **28% 절 누락** 관측 (5/19 372 clauses → 5/20 268 clauses). PoC 26%와 일치. silverone가 5/20에 속도 우선시. 후속 — 더 큰 sample 정확도 평가 + Q1~Q5 답변 품질 영향 측정.
  - vault `검토-raw/festival_smoke_5_20_clause_label_속도최적화.md` 진단 기록 + ADR-018 §"5/20 추가 결정" 정식화.
