# Python AI Worker

이 디렉터리는 현재 런타임에서 Python이 맡는 AI worker 구현체다. 자연어 분석
요청을 plan_v2로 만들고(planner), DuckDB로 실행하고(executor), 사용자-facing
답변을 구성하며(composer), dataset build 전처리를 수행한다. 전체 아키텍처는
저장소 루트 [../../README.md](../../README.md)를 본다.

> δ-1~δ-4 (2026-05-21) / ADR-018 β2 정리로 옛 흐름(rule-based planner /
> preprocess·retrieve·summarize skill / prepare·sentiment·embedding·cluster
> build / semantic search / final_answer presenter / devtools)은 모두 제거됐다.
> 현재는 **planner(LLM) + executor(DuckDB) + composer** 3-layer다.

## 책임

- **planner** — Anthropic LLM이 사용자 질문을 8개 표준 skill plan_v2로 생성 + validator self-correct
- **executor** — plan_v2 step을 DuckDB로 순차 실행
- **composer** — step 결과로 답변 본문 + display(표/차트 힌트) 구성
- **dataset build** — `clean`(deterministic) / `doc_genuineness`(LLOA) / `clause_label`(LLOA)
- prompt template / taxonomy / rule config 로딩

## HTTP task

`main.py`가 `ThreadingHTTPServer`로 `/tasks/<name>` POST를 받아 `task_router`로 dispatch한다.

| task | 설명 |
| --- | --- |
| `analyze` | plan 또는 user_question + artifact_paths → plan_v2 실행 결과 (canonical) |
| `plan` | user_question → plan_v2 생성 (debug entrypoint) |
| `dataset_clean` | 업로드 행 deterministic 정제 |
| `dataset_doc_genuineness` | LLOA doc-level 3-tier 진성 분류 |
| `dataset_clause_label` | LLOA 절 분리 + sentiment + aspect 라벨링 |

옛 `analyze_v2` / `plan_v2` task 이름은 backward-compatible alias로 dispatch된다.
GET `/healthz`(생존), `/capabilities`(task 목록)도 제공한다.

## 코드 구조

| 위치 | 역할 |
| --- | --- |
| `src/python_ai_worker/main.py` | HTTP entrypoint (`ThreadingHTTPServer`) |
| `src/python_ai_worker/task_router.py` | task name → handler routing (alias 포함) |
| `src/python_ai_worker/planner/` | LLM plan 생성 — `schema.py`(SKILL_CATALOG) / `validator.py` / `prompt.py` / `llm.py` / `skill_specs.py` / `step_display.py` / `recipes.py` |
| `src/python_ai_worker/executor/` | DuckDB plan_v2 실행 — `context.py` / `runner.py` / `service.py` / `skills/` |
| `src/python_ai_worker/executor/skills/` | atomic skill 구현 (`join / filter / aggregate / compare / calculate / sort / present`) |
| `src/python_ai_worker/composer/` | 답변 본문 + display projection 구성 |
| `src/python_ai_worker/dataset_build/` | `clean.py` / `doc_genuineness.py` / `clause_label.py` (+ `_common.py`) |
| `src/python_ai_worker/clients/` | 외부 LLM HTTP 클라이언트 — `anthropic.py` / `lloa.py` / `openai.py` |
| `src/python_ai_worker/registries/` | `prompt.py`(prompt 템플릿) / `task_registry.py`(task 정의) |
| `src/python_ai_worker/runtime/` | LLM wrapper, retry, obs helper |
| `src/python_ai_worker/taxonomies.py` | aspect taxonomy 로딩 |
| `src/python_ai_worker/prompt_options.py` | task-folder prompt version/default resolver |
| `src/python_ai_worker/sql_identifiers.py` | plan SQL identifier 안전성 검사 |
| `tests` | runtime / planner / executor / dataset_build regression test |

## plan_v2 / skill

- 8개 표준 skill: `join / filter / aggregate / compare / calculate / sort / present / summarize`.
  3개 RESERVED input table: `docs / clauses / genuineness`.
- skill catalog는 `planner/schema.py:SKILL_CATALOG`로 잠금. 새 표준 skill은
  `schema.py` + `executor/skills/` 핸들러 + `validator.py` 규칙 + 테스트를 함께 갱신한다.
- recipe(`planner/recipes.py`)는 자주 쓰는 패턴(예 `distribution`)을 실행 전
  결정론적으로 atomic step으로 lower한다.

## prompt / taxonomy / config 연결

- planner prompt는 저장소 루트 [../../config/prompts](../../config/prompts)의 Markdown으로 관리한다 (version 이름 = 파일 stem).
- dataset build prompt(doc_genuineness / clause_label)는 task-folder(`config/prompts/<task>/`)에서 resolve하며 `/prompt_options`로 선택지를 노출한다.
- rule config는 기본 상수 위에 `PYTHON_AI_RULE_CONFIG_PATH`, `PYTHON_AI_RULE_CONFIG_JSON`, request payload override를 순서대로 덮는다.
- LLM key: `ANTHROPIC_API_KEY`(planner/composer), `WISENUT_LLOA_MAX_V1_2_1_API_KEY`(dataset build).

## 자주 쓰는 명령

```bash
# 테스트 (requires-python >= 3.11)
PYTHONPATH=workers/python-ai/src python3.11 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'

# worker capability 목록
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.main --describe
```
