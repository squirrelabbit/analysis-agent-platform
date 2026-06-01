# Analysis API 응답 예시

silverone 2026-05-26 작성. analysis API의 sync stateful 흐름이 실제로 어떤
모양으로 응답하는지 프론트/운영자가 바로 이해할 수 있게 5 케이스를 고정한다.

- 기준 모델: [[analysis_api_model_2026-05-26]] §1~§7
- 응답 스키마: `docs/api/openapi.yaml`의 `AnalysisThreadMessageResponse` /
  `AnalyzeResponse` / `PresentResultPayload` / `ComposerOutput`
- composer 정책: ADR-020 PR-A deterministic v1

모든 예시의 ID는 placeholder (`prj_…`, `ds_…`, `dv_…`, `th_…`, `msg_…`,
`run_…`). 실제 응답에는 UUID. `rows`는 가독성 위해 1~2개만 보여준다.

## 공통 필드 의미

| 필드 | 의미 |
|---|---|
| `thread_id` | `analysis_thread` 식별자. 같은 대화 흐름의 모든 message/run이 공유. |
| `dataset_version_id` | thread 생성 시점에 잠긴 active dataset version. 이어질문에서도 동일. |
| `user_message` / `assistant_message` | 이 turn에 새로 저장된 message 2건. 화면이 thread reload 시 시간순으로 본다. |
| `assistant_message.content` | **composer.assistant_content가 그대로 저장된 값** (성공 run) 또는 failed run placeholder. |
| `assistant_message.context_summary` | composer.context_summary 그대로 + `question`은 caller가 user_question으로 덮어쓴다. 사용자 노출 X. |
| `run.status` | `running` / `completed` / `failed`. sync 모델은 3종만 사용 (async 전환은 ADR-019). |
| `result.present` | executor가 만든 raw table (PresentResultPayload). |
| `result.present.row_count` | 호환 필드 — `total_rows`와 동일. |
| `result.present.total_rows` | 전체 결과 row 수 (DuckDB COUNT). |
| `result.present.returned_rows` | 응답에 실제 담은 rows 길이. |
| `result.present.max_rows` | 적용된 반환 한도 (default 1000, present.limit 명시 시 그 값). |
| `result.present.truncated` | `returned_rows < total_rows`이면 true. |
| `result.composer` | post-execution layer가 만든 사용자-facing 답변 (ADR-020 PR-A). |
| `result.composer.assistant_content` | 자연어 답변. `assistant_message.content`로 저장. |
| `result.composer.display` | 화면 렌더링용 데이터. `result.present`와 거의 동일 shape. |
| `result.composer.context_summary` | 다음 turn conversation_context용 짧은 요약. `assistant_message.context_summary`로 저장. |
| `result.composer.metadata` | composer mode / template / fallback_reason. |
| `result.reuse` (있을 때만) | plan reuse 분기 metadata. `applied / action / action_params / source_run_id / fallback_reason`. |

---

## 1. `POST /analyze` — 첫 질문 성공

요청: `POST /projects/prj_festival/datasets/ds_festival/analyze`
body: `{"user_question": "작년과 올해의 aspect 증감수치 계산해줘"}`

응답: `200 OK`, `AnalysisThreadMessageResponse`

```jsonc
{
  "project_id": "prj_festival",
  "dataset_id": "ds_festival",
  "thread_id": "th_a1b2",                       // 새로 생성된 thread
  "dataset_version_id": "dv_2026q1",            // thread 생성 시점에 active version으로 잠금
  "user_message": {
    "message_id": "msg_u1",
    "thread_id": "th_a1b2",
    "project_id": "prj_festival",
    "dataset_id": "ds_festival",
    "role": "user",
    "content": "작년과 올해의 aspect 증감수치 계산해줘",
    "created_at": "2026-05-26T16:42:01+09:00"
  },
  "assistant_message": {
    "message_id": "msg_a1",
    "thread_id": "th_a1b2",
    "project_id": "prj_festival",
    "dataset_id": "ds_festival",
    "role": "assistant",
    "content": "분석 결과 3건을 table 형식으로 정리했습니다.",  // composer.assistant_content
    "context_summary": {                          // composer.context_summary + question 덮어씀
      "question": "작년과 올해의 aspect 증감수치 계산해줘",
      "present_title": "작년(2025) vs 올해(2026) aspect 증감 수치",
      "total_rows": 3,
      "row_count": 3,
      "returned_rows": 3,
      "columns": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
      "key_dimensions": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
      "answer_summary": "분석 결과 3건을 table 형식으로 정리했습니다."
    },
    "run_id": "run_r1",
    "created_at": "2026-05-26T16:42:08+09:00"
  },
  "run": {
    "run_id": "run_r1",
    "thread_id": "th_a1b2",
    "project_id": "prj_festival",
    "dataset_id": "ds_festival",
    "dataset_version_id": "dv_2026q1",
    "user_message_id": "msg_u1",
    "request_json": {
      "user_question": "작년과 올해의 aspect 증감수치 계산해줘"
      // conversation_context는 첫 turn이라 없음
    },
    "result_json": { /* 아래 result와 동일 — full payload */ },
    "status": "completed",
    "error_message": null,
    "created_at": "2026-05-26T16:42:01+09:00",
    "completed_at": "2026-05-26T16:42:08+09:00"
  },
  "mode": "user_question",
  "result": {
    "dataset_version_id": "dv_2026q1",
    "plan_version": "v2",
    "plan": { /* plan_v2 본문 — planner가 만든 step 8~10개 */ },
    "artifact_paths": {
      "docs": "/workspace/data/uploads/projects/prj_festival/.../cleaned.parquet",
      "clauses": "/workspace/data/artifacts/projects/prj_festival/.../clause_label.jsonl",
      "genuineness": "/workspace/data/artifacts/projects/prj_festival/.../doc_genuineness.jsonl"
    },
    "steps": [ /* 각 step별 row_count / sample_rows(5건) / extra */ ],
    "present": {
      "step_id": "out",
      "format": "table",
      "title": "작년(2025) vs 올해(2026) aspect 증감 수치",
      "row_count": 3,        // total_rows와 동일 (호환 필드)
      "total_rows": 3,       // DuckDB COUNT
      "returned_rows": 3,    // rows 배열 길이
      "max_rows": 1000,      // 적용된 반환 한도
      "truncated": false,    // returned_rows == total_rows
      "rows": [
        {"aspect": "atmosphere", "last_count": 1, "this_count": 2, "delta_count": 1,  "delta_rate": 100.0},
        {"aspect": "food",       "last_count": 1, "this_count": 0, "delta_count": -1, "delta_rate": -100.0}
        // ... 1건 더
      ]
    },
    "composer": {
      "assistant_content": "분석 결과 3건을 table 형식으로 정리했습니다.",
      "display": {
        "type": "table",
        "title": "작년(2025) vs 올해(2026) aspect 증감 수치",
        "rows": [ /* present.rows와 동일 */ ],
        "total_rows": 3,
        "returned_rows": 3,
        "max_rows": 1000,
        "truncated": false
      },
      "context_summary": {
        "present_title": "작년(2025) vs 올해(2026) aspect 증감 수치",
        "total_rows": 3,
        "row_count": 3,
        "returned_rows": 3,
        "columns": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
        "key_dimensions": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
        "answer_summary": "분석 결과 3건을 table 형식으로 정리했습니다."
        // question은 worker가 채울 수도/안 채울 수도 — Go control plane이 어차피 덮어쓴다
      },
      "metadata": {
        "mode": "deterministic",
        "template": "table_normal",
        "fallback_reason": null
      }
    },
    "planner": {
      "user_question": "작년과 올해의 aspect 증감수치 계산해줘",
      "prompt_version": "planner-v2-anthropic-v1",
      "attempts": [ /* planner LLM 호출 이력 */ ],
      "usage": { "input_tokens": 4123, "output_tokens": 482, "cache_read_input_tokens": 3664 }
    }
  }
}
```

화면 동작:
- `assistant_message.content`를 메시지 버블에 표시.
- `result.composer.display.rows`로 표/차트 렌더링.
- `result.composer.display.truncated=true`면 "전체 N건 중 M건만 표시" 안내.
- thread reload 시 `user_message → assistant_message` 두 건만 시간순.

---

## 2. `POST /analysis_threads/{tid}/messages` — 이어질문 성공 (reuse 미적용)

요청: `POST /projects/prj_festival/datasets/ds_festival/analysis_threads/th_a1b2/messages`
body: `{"content": "양수 증가만 보고 싶어"}`

이어질문 분기:
- rule classifier가 "양수만"을 reuse action으로 매핑 못 함 → no_match → planner LLM fallback.
- 같은 thread → 같은 `dataset_version_id` 사용.
- 이전 assistant_message.context_summary가 `conversation_context`로 worker에 inject.

응답: `200 OK`

```jsonc
{
  "project_id": "prj_festival",
  "dataset_id": "ds_festival",
  "thread_id": "th_a1b2",                       // 기존 thread 유지
  "dataset_version_id": "dv_2026q1",            // version도 동일
  "user_message": {
    "message_id": "msg_u2",
    "role": "user",
    "content": "양수 증가만 보고 싶어",
    "thread_id": "th_a1b2",
    "created_at": "2026-05-26T16:43:15+09:00"
  },
  "assistant_message": {
    "message_id": "msg_a2",
    "role": "assistant",
    "content": "분석 결과 2건을 table 형식으로 정리했습니다.",
    "context_summary": {
      "question": "양수 증가만 보고 싶어",
      "present_title": "양수 증감 aspect",
      "total_rows": 2,
      "row_count": 2,
      "returned_rows": 2,
      "columns": ["aspect", "delta_count"],
      "key_dimensions": ["aspect", "delta_count"],
      "answer_summary": "분석 결과 2건을 table 형식으로 정리했습니다."
    },
    "run_id": "run_r2",
    "thread_id": "th_a1b2",
    "created_at": "2026-05-26T16:43:21+09:00"
  },
  "run": {
    "run_id": "run_r2",
    "thread_id": "th_a1b2",
    "user_message_id": "msg_u2",
    "request_json": {
      "user_question": "양수 증가만 보고 싶어",
      "conversation_context": [
        // 이전 turn의 assistant_message.context_summary가 압축돼 들어감 (최근 3턴, 2000 bytes 한도)
        {
          "question": "작년과 올해의 aspect 증감수치 계산해줘",
          "answer_summary": "분석 결과 3건을 table 형식으로 정리했습니다.",
          "present_title": "작년(2025) vs 올해(2026) aspect 증감 수치",
          "row_count": 3,
          "columns": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
          "key_dimensions": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"]
        }
      ]
    },
    "status": "completed",
    "created_at": "2026-05-26T16:43:15+09:00",
    "completed_at": "2026-05-26T16:43:21+09:00"
  },
  "mode": "user_question",
  "result": {
    /* ... plan / steps / present / composer / planner 동일 구조 ... */
    "reuse": {
      "applied": false,
      "fallback_reason": "classifier_no_match"
      // action / action_params / source_run_id 모두 없음 (classifier가 매핑 못 함)
    }
  }
}
```

`reuse.applied=false`인 경우에도 `result.reuse`는 metadata 가시성을 위해 항상 노출. 운영자가 "이 turn은 LLM 흐름으로 갔다"는 걸 즉시 안다.

---

## 3. plan reuse 성공 (reuse.applied=true)

요청: `POST /projects/prj_festival/datasets/ds_festival/analysis_threads/th_a1b2/messages`
body: `{"content": "상위 5개만 보여줘"}`

이어질문 분기:
- rule classifier가 "상위 5개"를 `add_limit(n=5)` action으로 매핑.
- 이전 successful run(`run_r1`)의 plan을 patch — sort step에 `limit: 5` 추가.
- planner LLM 호출 없이 patched plan으로 executor 직접 호출 → composer는 `reuse_applied` 템플릿 선택.

응답: `200 OK`

```jsonc
{
  "project_id": "prj_festival",
  "dataset_id": "ds_festival",
  "thread_id": "th_a1b2",
  "dataset_version_id": "dv_2026q1",
  "user_message": {
    "message_id": "msg_u3",
    "role": "user",
    "content": "상위 5개만 보여줘",
    "created_at": "2026-05-26T16:44:02+09:00"
  },
  "assistant_message": {
    "message_id": "msg_a3",
    "role": "assistant",
    "content": "이전 분석 결과를 기준으로 요청한 표시 조건을 반영했습니다.",
    "context_summary": {
      "question": "상위 5개만 보여줘",
      "present_title": "작년(2025) vs 올해(2026) aspect 증감 수치",
      "total_rows": 3,
      "row_count": 3,
      "returned_rows": 3,
      "columns": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
      "key_dimensions": ["aspect", "last_count", "this_count", "delta_count", "delta_rate"],
      "answer_summary": "이전 분석 결과를 기준으로 요청한 표시 조건을 반영했습니다."
    },
    "run_id": "run_r3",
    "created_at": "2026-05-26T16:44:03+09:00"   // planner LLM 안 거쳐 즉시 응답 (festival 기준 1초 미만)
  },
  "run": {
    "run_id": "run_r3",
    "thread_id": "th_a1b2",
    "user_message_id": "msg_u3",
    "request_json": {
      "user_question": "상위 5개만 보여줘"
      // conversation_context는 reuse 흐름에서는 inject 안 함 (plan이 fixed)
    },
    "status": "completed",
    "created_at": "2026-05-26T16:44:02+09:00",
    "completed_at": "2026-05-26T16:44:03+09:00"
  },
  "mode": "user_question",
  "result": {
    "plan": { /* patched plan — run_r1 plan + sort.limit=5 */ },
    "steps": [ /* ... */ ],
    "present": { /* total_rows=3, returned=3, truncated=false (3건이라 5건 limit 무관) */ },
    "composer": {
      "assistant_content": "이전 분석 결과를 기준으로 요청한 표시 조건을 반영했습니다.",
      "display": { /* ... */ },
      "context_summary": { /* ... */ },
      "metadata": {
        "mode": "deterministic",
        "template": "reuse_applied",           // reuse 분기로 선택된 템플릿
        "fallback_reason": null
      }
    },
    "planner": null,                            // reuse 분기에서는 planner LLM 호출 없음
    "reuse": {
      "applied": true,
      "action": "add_limit",
      "action_params": {"n": 5},
      "source_run_id": "run_r1"                // 패치 원본 run
    }
  }
}
```

특징:
- `result.planner === null` — LLM 호출 안 함. 비용 0, latency 단축.
- `composer.metadata.template === "reuse_applied"` — 화면이 "이전 분석 기반" 배지 표시 가능.
- patched plan은 `run.result_json.plan`으로 저장돼 운영자가 무엇이 변경됐는지 확인 가능.

---

## 4. failed run

요청: `POST /projects/prj_festival/datasets/ds_festival/analysis_threads/th_a1b2/messages`
body: `{"content": "음수 sentiment 평균만 보여줘"}`

흐름: planner가 plan을 만들었는데 validator 단계에서 `aggregate.avg(sentiment)` (string column)로 reject (SQL-3.2 audit M3). worker가 4xx를 반환하면 control-plane이 failed 처리.

응답: `5xx` (HTTP error, [[analysis_api_model_2026-05-26]] §4 정책)

```jsonc
// HTTP 500
{
  "error": "analyze worker /tasks/analyze returned 400: {\"detail\":\"plan validation failed: [params.metric_column_not_numeric] aggregate.metrics[0] function='avg' requires a numeric column, but 'sentiment' from RESERVED table 'clauses' is type=string. ...\"}"
}
```

caller는 HTTP error를 즉시 인지. 하지만 thread는 다음 상태로 갱신된다:

| 저장 | 내용 |
|---|---|
| `analysis_messages` (user) | `content="음수 sentiment 평균만 보여줘"` |
| `analysis_messages` (assistant placeholder) | `content="분석 실행 중 오류가 발생했습니다. 조건을 조금 단순화해 다시 시도해 주세요."`, `run_id=run_r4`, `context_summary=null` |
| `analysis_runs` (run_r4) | `status="failed"`, `error_message="<위 worker error>"`, `request_json`은 채워짐, `result_json`은 비어있을 수 있음 |

이후 `GET /projects/prj_festival/datasets/ds_festival/analysis_threads/th_a1b2`로 thread 조회하면:

```jsonc
{
  "thread_id": "th_a1b2",
  "messages": [
    /* ... 이전 turn들 ... */
    {"message_id": "msg_u4", "role": "user", "content": "음수 sentiment 평균만 보여줘", "run_id": null},
    {
      "message_id": "msg_a4_placeholder",
      "role": "assistant",
      "content": "분석 실행 중 오류가 발생했습니다. 조건을 조금 단순화해 다시 시도해 주세요.",
      "context_summary": null,                  // 비어있어 다음 turn conversation_context에서 자동 제외
      "run_id": "run_r4"                        // failed run에 연결
    }
  ]
}
```

그리고 `GET /projects/prj_festival/datasets/ds_festival/analysis_runs/run_r4`:

```jsonc
{
  "run_id": "run_r4",
  "thread_id": "th_a1b2",
  "user_message_id": "msg_u4",
  "status": "failed",
  "error_message": "analyze worker /tasks/analyze returned 400: ...",  // 운영자용
  "request_json": {"user_question": "음수 sentiment 평균만 보여줘"},
  "result_json": null,
  "created_at": "2026-05-26T16:45:00+09:00",
  "completed_at": "2026-05-26T16:45:01+09:00"
}
```

원칙:
- 사용자는 placeholder 메시지로 실패를 인지. 기술적 error 내용은 노출하지 않음.
- 운영자는 `analysis_run.error_message`로 원인 파악.
- placeholder의 `context_summary`가 비어있어 후속 turn에 노이즈 안 됨.

---

## 5. version-specific debug — `POST /versions/{vid}/analyze`

요청: `POST /projects/prj_festival/datasets/ds_festival/versions/dv_2026q1/analyze`
body: `{"plan": { /* plan_v2 JSON */ }}`

특징:
- thread/message/run **저장 없음**. stateless debug/replay.
- 응답은 `AnalyzeResponse` (단순 result wrapper). `AnalysisThreadMessageResponse` 아님.
- 화면 분석은 이 path가 아니라 `/datasets/{did}/analyze` 사용. 이 endpoint는 운영자가 어제 plan을 직접 다시 돌릴 때.

응답: `200 OK`

```jsonc
{
  "project_id": "prj_festival",
  "dataset_id": "ds_festival",
  "version_id": "dv_2026q1",
  "mode": "plan",
  "result": {
    "dataset_version_id": "dv_2026q1",
    "plan_version": "v2",
    "plan": { /* 요청 body의 plan 그대로 (caller가 만든) */ },
    "artifact_paths": { /* docs/clauses/genuineness */ },
    "steps": [ /* ... */ ],
    "present": {
      "step_id": "out",
      "format": "table",
      "row_count": 3,
      "total_rows": 3,
      "returned_rows": 3,
      "max_rows": 1000,
      "truncated": false,
      "rows": [ /* ... */ ]
    },
    "composer": {
      "assistant_content": "분석 결과 3건을 table 형식으로 정리했습니다.",
      "display": { /* ... */ },
      "context_summary": { /* ... */ },
      "metadata": {
        "mode": "deterministic",
        "template": "table_normal",
        "fallback_reason": null
      }
    }
    // planner / reuse 없음 (plan을 직접 받았고 reuse 흐름도 없음)
  }
}
```

caller는 응답을 그대로 받아 plan replay 결과를 확인. thread를 사용하지 않으므로 후속 turn 개념도 없다.

---

## 운영 메모

- 모든 예시에서 `created_at` / `completed_at`은 ISO 8601 (KST). `displaytime.go`가 timezone 처리.
- `result.composer`가 노출되지 않는 옛 worker (5/26 이전)는 Go control plane이 기존 deterministic helper로 fallback해 `assistant_message.content`를 채운다 — 응답 외형은 동일.
- 화면 통합 시 우선순위:
  1. `assistant_message.content` 메시지 버블 표시.
  2. `result.composer.display`로 표/차트 렌더링.
  3. `result.composer.display.truncated=true`이면 안내 배지.
  4. `result.reuse.applied=true`이면 "이전 분석 기반" 배지.
- 후속 LLM composer (ADR-020 PR-B) 도입 시 `composer.metadata.mode`가 `llm_backed` 또는 `deterministic_fallback`으로 바뀐다. 본 5 예시는 PR-A baseline 기준.
