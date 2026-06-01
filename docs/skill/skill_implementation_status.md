# Skill Implementation Status

δ-3 (2026-05-21) — 옛 plan layer(preprocess / aggregate / summarize / presentation 13종 hardcoded skill)는
모두 제거됐다. 이 문서는 새 **planner / executor / analyze** 흐름 (응답 body의
`plan_version: "v2"`는 wire version 식별자로 유지)의 구현 상태를 짧게 정리한다.

제품 계약은 [skill_registry.md](skill_registry.md). 코드 source:

- planner — `workers/python-ai/src/python_ai_worker/planner/`
- executor — `workers/python-ai/src/python_ai_worker/executor/`
- dataset_build — `workers/python-ai/src/python_ai_worker/dataset_build/`
- analyze Go service — `apps/control-plane/internal/service/analyze.go`
- worker HTTP task URL: canonical `POST /tasks/analyze` / `POST /tasks/plan`.
  옛 `/tasks/analyze_v2` / `/tasks/plan_v2`는 backward-compatible alias.

안정도:
- `안정`: 결정론적, 입력/출력 예측 가능
- `중간`: backend / 사전 / 환경 차이에 영향 받음
- `LLM 의존`: 모델 / 프롬프트 / 장애 상태 영향이 큼

## plan skill 상태 (8종, DuckDB executor)

| Skill | 구현 방식 | 안정도 | 메모 |
| --- | --- | --- | --- |
| `join` | DuckDB JOIN | 안정 | RESERVED table 식별자 검증 |
| `filter` | DuckDB WHERE | 안정 | LLM이 컬럼 이름 검증 통과해야 함 |
| `aggregate` | DuckDB GROUP BY + 집계 함수 | 안정 | sum/avg/count 등 표준 |
| `compare` | DuckDB FULL OUTER JOIN diff | 안정 | baseline/target row 변경 감지 |
| `calculate` | DuckDB 산술 표현식 | 안정 | left/right/op 키 잠금 (validator) |
| `sort` | DuckDB ORDER BY | 안정 | by, direction 검증 |
| `present` | column 매핑만 | 안정 | 최종 출력 형식 |
| `summarize` | LLM 호출 + fallback | LLM 의존 | 표 + 질문 → 답변 텍스트 |

## planner LLM 상태

| 구성 요소 | 구현 방식 | 메모 |
| --- | --- | --- |
| `generate_plan` | Anthropic strict-mode JSON 호출 | parse retry 1 + validator self-correct retry 1 |
| `_decode_planner_step_inputs` | inputs는 JSON 문자열로 직렬화 후 strict parse | grammar compiler 한도 회피 (Fix 2) |
| `_strict_object_schema` | 모든 nested object에 `additionalProperties:false` 강제 | Anthropic strict-mode 호환 (Fix 1) |
| today 주입 | `{{today}}` placeholder | "작년/올해" 상대 표현 정확도 보장 |

## dataset_build task 상태 (3종)

ADR-017로 `config/task_registry.json`에 분리. ADR-018 (β2) 결정으로 hot path 3종 축소.

| Task | 구현 방식 | 안정도 | 메모 |
| --- | --- | --- | --- |
| `dataset_clean` | regex/noise scrub + dedup | 안정 | 업로드 직후 자동 실행 |
| `dataset_doc_genuineness` | LLOA 한 호출 doc-level 3-tier 진성 분류 | LLM 의존 | clean 직후 명시 trigger |
| `dataset_clause_label` | LLOA 한 호출 절·문장 분리 + sentiment + aspect | LLM 의존 | doc_genuineness 이후. ADR-018 §5/20 결정으로 44× 단축됨 |

## 운영 메모

- analyze는 stateless. 옛 `executions` / `skill_plans` / `analysis_requests` / `report_drafts` 테이블은 δ-3에서 모두 drop됐다.
- 결과 audit이 필요하면 plan + response body를 호출자가 직접 보관한다.
- `확인 필요:` summarize skill의 LLM 답변 품질은 representative dataset 기준 추가 검증이 더 필요하다.
