# Contributing — PR 직전 체크리스트

> 이 문서는 **PR 보내기 직전** 5분 안에 훑어보는 체크리스트다.
> 저장소 공통 규칙·트랙 메모는 [`CLAUDE.md`](./CLAUDE.md), 의사결정 기록은 Obsidian vault `01-Projects/분석지원시스템/검토-raw/`에 둔다.

δ-1~δ-4 (2026-05-21)로 옛 plan layer(SkillPlan / executions / report_drafts / 13 hardcoded skill)는 모두 삭제됐다. 현재 분석은 LLM-driven **planner_v2** + 결정론적 **executor_v2**의 2-계층 구조다. 본 체크리스트도 그 기준으로 정리돼 있다.

---

## 1. Task / plan_v2 skill 추가·변경

### 1-A. plan_v2 skill 추가/변경 (LLM-callable 분석 skill)

plan_v2 skill 카탈로그는 **코드로 잠금**돼 있다 (`config/skill_bundle.json` 같은 외부 JSON 없음). 새 표준 skill을 추가하려면 다음 5곳을 함께 갱신한다.

| # | 위치 | 검증 명령 |
|---|---|---|
| 1 | `workers/python-ai/src/python_ai_worker/planner_v2/schema.py:SKILL_CATALOG` entry | `grep -n "^SKILL_CATALOG" workers/python-ai/src/python_ai_worker/planner_v2/schema.py` |
| 2 | `workers/python-ai/src/python_ai_worker/planner_v2/validator.py` skill-specific 검사 규칙 | `grep -n "_<NAME>_REQUIRED_KEYS\|def _validate_<name>" workers/python-ai/src/python_ai_worker/planner_v2/validator.py` |
| 3 | `workers/python-ai/src/python_ai_worker/executor_v2/skills/<name>.py` 빌더 | `ls workers/python-ai/src/python_ai_worker/executor_v2/skills/` |
| 4 | `workers/python-ai/src/python_ai_worker/executor_v2/runner.py:SKILL_BUILDERS` dispatch | `grep -n "SKILL_BUILDERS" workers/python-ai/src/python_ai_worker/executor_v2/runner.py` |
| 5 | `workers/python-ai/tests/test_planner_v2_*.py` + `test_executor_v2_*.py` 잠금 테스트 | `PYTHONPATH=workers/python-ai/src python3.11 -m unittest discover -s workers/python-ai/tests -p 'test_planner_v2_*.py' -p 'test_executor_v2_*.py'` |

원칙: planner_v2 prompt가 새 skill을 출력할 수 있도록 prompt 본문도 함께 갱신 (`config/prompts/planner-v2-anthropic-v1.md`). 새 skill이 prompt에 안 보이면 LLM은 사용하지 않는다.

### 1-B. dataset_build internal task 추가/변경

dataset_build·관리·평가성 *내부 실행 task*는 `config/task_registry.json`에 등록한다 (ADR-017). 동기화 6곳:

| # | 위치 | 검증 명령 |
|---|---|---|
| 1 | `config/task_registry.json` entry | `python3 -c "import json; r=json.load(open('config/task_registry.json')); [print(t['task_name']) for t in r['tasks']]"` |
| 2 | `workers/python-ai/src/python_ai_worker/dataset_build/<name>.py` handler + `run_dataset_<name>` 함수 | `grep -rn "def run_dataset_" workers/python-ai/src/python_ai_worker/dataset_build/` |
| 3 | `workers/python-ai/src/python_ai_worker/task_router.py` map | `grep -n "<name>" workers/python-ai/src/python_ai_worker/task_router.py` |
| 4 | Go service `apps/control-plane/internal/service/dataset_build_<name>.go` | `ls apps/control-plane/internal/service/dataset_build_*.go` |
| 5 | Go service 호출은 `registry.TaskPathFor("<name>")` lookup (hardcoded 금지) | `grep -rn '"/tasks/' apps/control-plane/internal/` (결과 0건이어야 함) |
| 6 | HTTP route + handler (`apps/control-plane/internal/http/server.go`) | `grep -n "<name>\|<Name>" apps/control-plane/internal/http/server.go` |

원칙: *plan_v2 안에서 LLM이 직접 호출할 skill인가?* → Yes면 1-A (코드 SKILL_CATALOG), No면 1-B (`task_registry.json`).

---

## 2. 검증해야 할 invariant

δ-1~δ-4 대규모 정리 이후 살아남은 invariant + 새 v2 layer가 만든 invariant.

### 2-1. plan_v2 validator는 skill 카탈로그 밖을 silent drop하지 않는다

- 위치: `workers/python-ai/src/python_ai_worker/planner_v2/validator.py`
- 잠금 테스트: `test_planner_v2_validator.py`
- **금지**: SKILL_CATALOG에 없는 skill_name을 plan에 넣으면 `PlannerValidationError`로 fail-loud. silently 통과시키지 마라.

### 2-2. DuckDB SQL identifier 안전성

- 위치: `executor_v2/context.py` + `executor_v2/skills/*.py` (column / alias 검사)
- 잠금: identifier가 `^[a-zA-Z_][a-zA-Z0-9_]*$` regex 통과해야 함. LLM이 임의 표현식을 넣어도 SQL injection 안 됨.
- **금지**: 새 skill 빌더에서 `f"SELECT {column}"` 식으로 검증 없이 string interpolation 하지 마라.

### 2-3. worker HTTP 4xx → Temporal NonRetryable wrap 의무

- 위치: `apps/control-plane/internal/workererror/` + `internal/skills/python_build_client.go`
- 잠금: `workererror.Rejection`(4xx) + `workererror.Upstream`(5xx) 분기. lock test 있음.
- **금지**: 새 worker HTTP client에서 `if resp.StatusCode != 200 { return fmt.Errorf(...) }` 식의 generic wrap을 쓰면 안 된다. 4xx가 Temporal default retry policy로 들어가면 무한 retry 회귀(5/6 issue_summary 사고 재발).

### 2-4. Anthropic strict-mode schema 호환성

- 위치: `workers/python-ai/src/python_ai_worker/clients/anthropic.py:_strict_object_schema`
- 잠금: 모든 nested object에 `additionalProperties:false` 자동 강제. Anthropic structured-output strict mode가 missing/`true`면 HTTP 400 거부.
- **금지**: schema를 직접 만들어 `create_json_response`에 넘기지 마라 — helper를 통하지 않으면 missing 검증으로 prod 400 실패.

### 2-5. planner_v2 prompt에 `{{today}}` placeholder 주입

- 위치: `planner_v2/prompt.py:render_planner_v2_prompt`
- 잠금: today가 비어 있으면 자동으로 `datetime.utcnow().date()` 사용.
- **금지**: prompt 본문에서 `{{today}}` placeholder 제거 금지. LLM 학습 cutoff과 운영 시점이 달라 "작년/올해" 같은 상대 시간 해석이 깨진다.

---

## 3. Silent fallback 패턴 — fallback 분기엔 반드시 obs warning

```python
try:
    result = call_external()
except Exception:
    return []  # ❌ silent — 운영자가 진단 불가
```

올바른 패턴:

```python
try:
    result = call_external()
except Exception as exc:
    _LOG.warning(
        "external_call.failed",
        operation="<context>",
        error_category=type(exc).__name__,
    )
    return []  # ✅ visible
```

PR에 `try/except` 또는 `return []`/`return None` 추가 시 **반드시 obs warning 또는 명시적 reason 동반**.

---

## 4. 영향 범위 검증 명령

PR 직전 일괄 실행:

```bash
# Python (v2 layer 중심, requires-python >= 3.11)
PYTHONPATH=workers/python-ai/src python3.11 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'

# Go 전체
(cd apps/control-plane && go test ./...)

# OpenAPI yaml parse
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); puts "ok"'

# direct-plan e2e smoke (compose dev 띄운 상태)
./scripts/smoke_analyze_v2_e2e.sh --mode direct-plan
```

LLM 호출까지 검증해야 할 때 (ANTHROPIC_API_KEY 있는 환경):

```bash
./scripts/smoke_analyze_v2_e2e.sh --mode user-question
```

---

## 5. 커밋 메시지

한국어로 작성. 접두사는 `feat:`, `fix:`, `refactor:`, `doc:`만 사용한다. **scope** (`feat(api):` 등) 형식은 쓰지 않는다.

PR 메시지에 다음 명시 권장:
- 변경한 invariant가 위 §2 5종 중 하나면 명시
- 새 silent fallback 패턴 추가 시 obs warning 위치
- 깨진 테스트가 본 PR에서 발생인지 pre-existing인지

---

## 6. 컨텍스트 파일 (이미 읽었어야 할 곳)

| 파일 | 언제 |
|---|---|
| `CLAUDE.md` | 첫 PR 전 + 트랙 상태·우선순위 변경 시 |
| `docs/api/openapi.yaml` | API 계약 변경 시 |
| `docs/skill/skill_registry.md` / `skill_implementation_status.md` | plan_v2 skill / dataset_build task 추가·변경 시 |
| `config/task_registry.json` | dataset_build·internal task 추가/변경 시 (ADR-017) |
| `workers/python-ai/src/python_ai_worker/planner_v2/schema.py` | plan_v2 skill 카탈로그 변경 시 |
