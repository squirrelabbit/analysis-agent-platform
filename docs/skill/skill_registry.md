# Skill Registry

δ-3 (2026-05-21) — 옛 plan layer(preprocess / aggregate / summarize / presentation 13종 hardcoded skill)는
모두 제거됐다. 현재 분석은 LLM-driven **planner** + 결정론적 **executor**의 2-계층 구조로 동작한다.
plan body의 wire version 식별자는 `plan_version: "v2"`로 잠금 (rename PR A 정책).
이 문서는 plan skill 카탈로그와 dataset_build task의 짧은 요약이다.

## 새 구조 (analyze)

```
POST /projects/{pid}/datasets/{did}/analyze                   ← active version
POST /projects/{pid}/datasets/{did}/versions/{vid}/analyze    ← explicit version

  1. planner (LLM) — question + table schema → plan JSON (plan_version: "v2")
  2. executor (DuckDB) — plan step 순차 실행
  3. response body로 plan + 결과 반환 (stateless)
```

worker task URL은 canonical `POST /tasks/analyze` / `POST /tasks/plan`. 옛
`/tasks/analyze_v2` / `/tasks/plan_v2`는 backward-compatible alias.

plan은 contract `plan_version: "v2"`로 잠금. DB에 저장되지 않으며, 실행 결과도
response body로만 전달된다 (옛 executions / skill_plans / report_drafts 테이블 모두 제거됨).

## plan skill catalog (8종)

planner가 plan에 포함시킬 수 있는 표준 skill은 다음 8종이다.
모든 skill은 DuckDB executor 안에서 결정론적으로 실행된다.

| Skill | 역할 | 입력 / 출력 |
| --- | --- | --- |
| `join` | 여러 테이블을 key로 결합 | input: left, right, on / output: 결합 테이블 |
| `filter` | row 조건 필터 | input: where / output: 부분집합 테이블 |
| `aggregate` | group + 집계 함수 | input: group_by, aggregations / output: 집계 테이블 |
| `compare` | 두 테이블 동일 컬럼 비교 | input: baseline, target, key / output: diff 테이블 |
| `calculate` | 산술 / 비율 계산 | input: left, right, op / output: 계산 컬럼 추가 |
| `sort` | 행 정렬 | input: by, direction / output: 정렬된 테이블 |
| `present` | 최종 결과 형식 변환 | input: 출력 column 매핑 / output: 사용자 화면 표 |
| `summarize` | 자연어 요약 (LLM 선택) | input: 표 + 질문 / output: 답변 텍스트 |

3 RESERVED input table:

- `docs` — clean 산출물 (cleaned.parquet 직접 read)
- `clauses` — clause_label 산출물 (clause_id auto-generated)
- `genuineness` — doc_genuineness 산출물

artifact_paths는 control plane이 inline으로 plan에 주입한다.

## dataset_build task

dataset_build task는 planner가 plan에 직접 넣는 skill이 아니라 dataset version 준비용 internal task이다.
ADR-017로 `config/task_registry.json`에 분리됐다.

ADR-018 (2026-05-19 β2 결정)으로 hot path는 다음 3종으로 축소됐다.

- `dataset_clean` — 업로드 직후 자동 실행되는 row-level 정제.
- `dataset_doc_genuineness` — clean 직후 LLOA 한 호출씩 doc-level 3-tier 진성 분류(genuine_review / mixed / non_review).
- `dataset_clause_label` — cleaned doc 단위 LLOA 한 호출로 절 분리 + sentiment + aspect 라벨링.

dataset build 단계는 `source → clean → doc_genuineness → clause_label` 순으로 흐른다.

Go control plane은 `internal/registry.TaskPathFor(name)`로 task_registry에서 task_path를 lookup한다 (hardcoded 금지).
Python worker는 `python_ai_worker.task_registry.task_definition(name)`을 사용한다.

## 계약 원칙

- planner가 생성하는 plan은 8 skill + 3 RESERVED table 밖을 쓰지 않는다 (validator로 잠금).
- DuckDB SQL identifier 안전성은 `^[a-zA-Z_][a-zA-Z0-9_]*$` regex로 enforce.
- plan_version 필드는 `"v2"`로 잠금.
