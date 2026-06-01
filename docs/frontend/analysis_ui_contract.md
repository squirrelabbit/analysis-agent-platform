# Analysis UI Contract (Frontend)

silverone 2026-05-27 작성. 백엔드 응답을 기준으로 프론트가 분석 채팅 화면을 어떻게 구성해야 하는지 정리한 최소 UI 계약 + 구현 체크리스트.

본 문서는 *프론트 구현 가이드*. 백엔드 응답 shape 자체는 다음을 본다:

- 응답 모델: [`docs/api/openapi.yaml`](../api/openapi.yaml) `AnalysisThreadMessageResponse` / `AnalyzeResponse` / `ComposerOutput` / `PresentResultPayload`
- 예시 5케이스: [`docs/api/analysis_response_examples.md`](../api/analysis_response_examples.md)
- 백엔드 모델 정책: vault `analysis_api_model_2026-05-26`
- 디스플레이 계약(백엔드 평가): vault `analysis_display_contract_2026-05-27`

이번 단계는 *문서/계약*만. 프론트 코드 변경 없음.

---

## 1. 화면 영역 정의

분석 채팅 화면을 다음 5영역으로 나눈다.

| 영역 | 화면 위치 | 사용자 노출 | 백엔드 출처 |
|---|---|---|---|
| **message list** | 채팅 중앙 (말풍선 시간순) | ✓ | `assistant_message.content`, `user_message.content`, thread 조회 시 `messages[]` |
| **result display** | 직전 assistant 말풍선 옆/아래 | ✓ | `result.composer.display` (우선) → `result.present` (fallback) |
| **result meta** | display 영역의 헤더/푸터 배지 | ✓ | `display.{total_rows, returned_rows, max_rows, truncated}` + `result.reuse.applied` |
| **debug / operator panel** | 운영자 모드 토글 (default 숨김) | ✗ (운영자만) | `run.status`, `run.error_message`, `result.plan`, `result.steps`, `result.planner.usage`, `result.composer.metadata`, `result.reuse` 전체 |
| **failure UI** | assistant placeholder 말풍선 | ✓ | `assistant_message.content` (placeholder 문구), 사용자 재시도 버튼 (선택) |

각 영역의 백엔드 출처와 fallback 규칙은 §2~§6에서 상세.

---

## 2. API 응답 필드 → UI 영역 매핑

`POST /datasets/{did}/analyze` 또는 `POST /analysis_threads/{tid}/messages` 응답 (`AnalysisThreadMessageResponse`):

```text
response
├── thread_id, dataset_version_id ............... URL / 상태 보관
├── user_message
│   ├── content ................................ message list (user 말풍선)
│   └── created_at ............................. 시간 표시
├── assistant_message
│   ├── content ................................ message list (assistant 말풍선)
│   ├── context_summary ........................ ✗ 화면 노출 금지. 다음 turn용
│   └── run_id ................................. operator panel 링크
├── run
│   ├── status ................................. result meta (running/completed/failed)
│   ├── error_message .......................... operator panel only
│   └── completed_at ........................... operator panel
└── result
    ├── composer.assistant_content ............. message list와 동일 (assistant_message.content와 같은 값)
    ├── composer.display ....................... result display (우선)
    │   ├── type ............................... 렌더링 분기 (table/chart/json)
    │   ├── title .............................. display 헤더
    │   ├── columns ............................ table header 순서
    │   ├── rows ............................... table body / chart data
    │   ├── total_rows, returned_rows, max_rows, truncated ... result meta
    ├── composer.context_summary ............... ✗ 화면 노출 금지
    ├── composer.metadata ...................... operator panel (mode / template / fallback_reason)
    ├── present ................................ result display fallback (composer 없는 옛 응답)
    ├── reuse .................................. result meta 배지 + operator panel
    │   ├── applied ............................ "이전 분석 기반" 배지 표시 여부
    │   ├── action, action_params, source_run_id ... operator panel
    │   └── fallback_reason .................... operator panel
    ├── plan, steps ............................ operator panel
    ├── planner.usage .......................... operator panel
    └── artifact_paths ......................... operator panel (거의 안 봄)
```

GET `/analysis_threads/{tid}` 응답은 `messages[]` 배열만 — 각 메시지에 `role` / `content` / `context_summary` / `run_id` / `created_at`이 있다. 프론트는 시간순 정렬해서 표시한다.

---

## 3. 우선순위 / fallback 규칙

| 영역 | primary | fallback | 비고 |
|---|---|---|---|
| 채팅 말풍선 (assistant) | `assistant_message.content` | "분석 결과가 생성되었습니다." (백엔드가 빈 값일 때 안전망) | composer 결과가 그대로 저장됨 |
| 표/차트 데이터 | `result.composer.display` | `result.present` | composer 없는 옛 worker 응답 graceful |
| display.type | `result.composer.display.type` | `result.present.format` | enum: `table` / `chart` / `json` |
| total_rows | `result.composer.display.total_rows` | `result.present.total_rows` → `result.present.row_count` | row_count는 deprecated alias |
| columns | `result.composer.display.columns` | `Object.keys(result.composer.display.rows[0])` 또는 `Object.keys(result.present.rows[0])` | 백엔드가 채워주지만 프론트도 안전망 |
| run status | `run.status` | (항상 존재) | sync 모델 3종 (running/completed/failed) |

원칙:
- composer가 있으면 그 값을 신뢰. 없으면 present로 graceful.
- **`context_summary`는 어떤 경우에도 사용자에게 직접 노출하지 않는다.**

---

## 4. table 렌더링 계약

`display.type === "table"`일 때:

| 항목 | 규칙 |
|---|---|
| header | `display.columns` 순서대로. 비어있으면 `Object.keys(display.rows[0])`. 둘 다 비어있으면 헤더 자체 숨김. |
| body | `display.rows`를 각 컬럼 key로 mapping. 값이 `null`이면 `"-"` 또는 빈 셀. |
| column label | v1은 `columns`가 string list라 **key 그대로** 노출. `last_count` → `last_count` (한국어 라벨 매핑은 후속). |
| truncated 안내 | `display.truncated === true`이면 표 위 또는 아래에 "전체 {total_rows}건 중 {returned_rows}건 표시" 텍스트. assistant 말풍선에도 같은 의미 문구가 있지만 시각적 강조는 별도. |
| reuse 배지 | `result.reuse?.applied === true`이면 표 헤더 옆 작은 배지 "이전 분석 기반". |
| 정렬 / 필터 | 1차 cut에서는 없음. 화면 요구 시 후속 (`columns[].type`이 필요해짐). |
| 빈 rows | `display.rows.length === 0`이면 §5 empty state. |

### 예시 응답 → 렌더링 결과

```jsonc
{
  "display": {
    "type": "table",
    "title": "aspect별 작년/올해 건수와 증감률",
    "columns": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
    "rows": [
      {"aspect": "atmosphere", "last_count": 1, "this_count": 2, "delta_count": 1, "delta_rate": 100.0}
    ],
    "total_rows": 3, "returned_rows": 1, "max_rows": 1000, "truncated": true
  }
}
```

→ 헤더 5컬럼 + body 1행 + "전체 3건 중 1건 표시" 안내.

---

## 5. empty UX

**조건**: `display.rows.length === 0` 또는 `display.total_rows === 0` 또는 `result.composer.metadata.template === "empty"`.

| 영역 | 표시 |
|---|---|
| 채팅 말풍선 | `assistant_message.content` ("조건에 맞는 결과가 없습니다." — composer가 deterministic하게 생성) |
| display 영역 | 표/차트 영역 자체 숨김 또는 회색 placeholder ("결과 없음"). 빈 표는 그리지 않는다. |
| result meta | total_rows=0 / truncated=false (이 둘은 자동) |
| 사용자 안내 | placeholder 본문 외 추가 안내 불필요. 프론트가 별도 텍스트를 덧붙이지 않는다. |

---

## 6. failed UX

**조건**: `POST` 응답이 HTTP error (4xx/5xx) — 백엔드는 thread에 placeholder를 저장하고 caller에게는 에러 반환.

화면 동작:

| 단계 | 표시 |
|---|---|
| 직후 (응답 받은 시점) | toast/배너에 "분석 중 오류가 발생했습니다." 짧은 알림 + 채팅에는 user 메시지만 표시 (assistant 응답 대기 중). |
| thread reload 시 | `GET /analysis_threads/{tid}` 응답의 `messages[]`에 user 메시지 + assistant placeholder 한 쌍이 보임. placeholder.content = "분석 실행 중 오류가 발생했습니다. 조건을 조금 단순화해 다시 시도해 주세요." |
| 사용자에게 노출 | 위 placeholder 문구만. **`run.error_message`(기술적 에러 원문)는 절대 노출 X.** |
| operator panel | run_id로 `GET /analysis_runs/{run_id}` 호출. `status=failed` + `error_message` 노출. |
| 재시도 | 화면이 placeholder 말풍선 옆에 "다시 시도" 버튼을 둘 수 있음 (선택). 클릭 시 같은 thread에 같은 user message를 다시 보낸다 (`POST /analysis_threads/{tid}/messages`). v1에서는 버튼 없이 사용자가 직접 입력해도 OK. |

placeholder의 `context_summary`는 비어있어 다음 turn `conversation_context`에서 자동 제외 — 프론트가 별도 처리할 것 없음.

---

## 7. chart / json 정책

### 7.1 chart

현재 백엔드 composer는 `chart_spec`을 만들지 않는다.

| 시나리오 | 프론트 동작 |
|---|---|
| `display.type === "chart"` + `chart_spec`이 응답에 없음 | **table fallback**. type을 무시하고 `columns` + `rows`로 표 렌더링. |
| `display.type === "chart"` + `chart_spec` 있음 | 후속 PR로 진입 시점에 chart 렌더링. v1에서는 발생 X. |

### 7.2 json

| 시나리오 | 프론트 동작 |
|---|---|
| `display.type === "json"` | 운영자/개발자용. default는 숨김 또는 작은 "JSON 보기" 토글. raw rows를 JSON viewer로 표시. |

---

## 8. debug / operator panel

운영자 모드 토글 (default 숨김). 다음 항목 노출:

```text
Run
├── run_id, status, created_at, completed_at
└── error_message (failed일 때만)

Plan
├── plan_version
├── steps[]: {step_id, skill, row_count, sample_rows[5], extra}
└── (전체 plan body는 큰 JSON viewer로)

Planner
├── prompt_version
├── usage: {input_tokens, output_tokens, cache_read_input_tokens}
└── attempts (있으면)

Composer
└── metadata: {mode, template, fallback_reason}

Reuse
├── applied (true/false)
├── action / action_params / source_run_id (applied=true일 때)
└── fallback_reason (applied=false일 때)
```

이 영역은 frontend repo 작업. 본 문서는 *어떤 필드를 어디에 노출할지*만 명시.

---

## 9. 현재 미지원 항목 (백엔드 응답에 없음)

| 항목 | 상태 |
|---|---|
| `columns[].label` (한국어 라벨 매핑) | 후속 PR 후보. 현재는 key 그대로. |
| `columns[].type` (number / string / date) | 후속 PR. 정렬/포맷 필요해지면 진입. |
| `columns[].format` (%, currency, date format) | 후속 PR. |
| `display.warnings[]` (ratio NaN / overflow 등) | 후속 (LLM composer 도입 후). |
| `display.recommended_view` (chart vs table 추천) | 후속 (planner LLM 보강 후). |
| `display.chart_spec` | 후속 (chart 화면 요구사항 진입 시). |
| async run status enum 확장 (queued/planning/executing/composing/cancelled) | ADR-019 트리거 충족 시. 현재는 running/completed/failed 3종만. |
| 재시도 / cancel 버튼 | 사용자 요구 시점에 결정. |
| 추천 follow-up 질문 (`suggestions`) | composer LLM PR-B에서 검토. |

---

## 10. 프론트 구현 체크리스트

새 화면 작업 / 회귀 검증 시 다음을 점검한다.

### 기본 렌더링

- [ ] thread reload 시 `messages[]`를 시간순으로 정렬해 표시한다 (assistant placeholder 포함).
- [ ] `assistant_message.content`를 메시지 말풍선에 표시한다. `composer.assistant_content`와 동일 값.
- [ ] `assistant_message.context_summary`는 어디에도 노출하지 않는다.

### result display (table)

- [ ] `result.composer.display`가 있으면 그걸 사용. 없으면 `result.present`로 fallback.
- [ ] `display.columns` 순서대로 헤더. 없으면 `rows[0]`의 key 순서.
- [ ] `display.truncated === true`이면 안내 텍스트 추가.
- [ ] `display.rows.length === 0`이면 표 자체 숨김, empty 안내만.

### result meta 배지

- [ ] `result.reuse?.applied === true`이면 "이전 분석 기반" 배지.
- [ ] `display.truncated === true`이면 truncated 안내.
- [ ] `run.status === "failed"`이면 실패 배지 + 운영자 모드에서만 error_message.

### chart / json

- [ ] `display.type === "chart"`이고 `chart_spec`이 없으면 table fallback.
- [ ] `display.type === "json"`이면 default 숨김, 토글로 노출.

### failed UX

- [ ] HTTP 4xx/5xx 응답 받으면 toast로 짧은 실패 알림.
- [ ] thread reload 시 user + assistant placeholder 한 쌍이 자연스럽게 보인다.
- [ ] `run.error_message`는 사용자에게 노출하지 않는다 (operator panel만).

### operator panel

- [ ] 토글로만 노출 (default 숨김).
- [ ] §8 항목들이 모두 보인다.

### 백엔드 응답 호환

- [ ] composer가 없는 옛 응답에서도 화면이 깨지지 않는다 (present fallback).
- [ ] `display.columns`가 없는 옛 응답에서도 `rows[0]` key로 fallback.
- [ ] `total_rows`가 없으면 `row_count` (호환 alias) 사용.

---

## 11. 변경 이력

| 날짜 | 변경 | 백엔드 commit |
|---|---|---|
| 2026-05-26 | composer 도입 (PR-A), `result.composer.{assistant_content, display, context_summary, metadata}` 노출 | `2b338571` |
| 2026-05-26 | openapi composer schema 명시 (PR-C) | `5515079e` |
| 2026-05-27 | `display.columns: [string]` 추가 (display-columns) | `43c5af35` |

본 문서는 백엔드 응답 변경이 있을 때마다 갱신한다. 새 필드 도입 시 §2 매핑 / §10 체크리스트 / §11 변경 이력 3곳을 갱신.

---

## 12. 관련 문서

- 백엔드 응답 모델: [vault `analysis_api_model_2026-05-26`]
- 디스플레이 백엔드 계약: [vault `analysis_display_contract_2026-05-27`]
- ADR-020 composer 결정: [vault `ADR-020_answer_composition`]
- 응답 예시 5케이스: [`docs/api/analysis_response_examples.md`](../api/analysis_response_examples.md)
- OpenAPI 계약: [`docs/api/openapi.yaml`](../api/openapi.yaml)
- 사용자 가치 검증 (배경): [vault `사용자_가치_비교_festival_2026-04`]
