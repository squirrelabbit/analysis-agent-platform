# plan_v2 smoke fixture

silverone 2026-05-21 5단계 결정으로 committed된 deterministic smoke fixture.
LLM planner (6단계)를 붙이기 전 executor_v2의 계산 경로가 결정적으로 동작하는지
재현 가능하게 잠그기 위한 데이터.

## 시나리오

"작년과 올해의 aspect 증감수치 계산해줘"

- 2025 docs 2개
  - d1: atmosphere clause 1개
  - d2: food clause 1개
- 2026 docs 2개
  - d3: atmosphere clause 2개
  - d4: contents clause 1개

→ aspect_delta 결과 (full outer compare 유지):

| aspect | last_year | this_year | delta_count | delta_rate |
|---|---|---|---|---|
| atmosphere | 1 | 2 | +1 | +100.0 |
| food | 1 | NULL | -1 | -100.0 |
| contents | NULL | 1 | +1 | NULL |

`food`는 사라진 aspect, `contents`는 새 aspect. `contents.delta_rate`가 NULL인
건 base=NULL일 때 percent_change가 NULL을 돌려주는 safe path 검증.

## 파일

| file | role |
|---|---|
| `cleaned.parquet` | docs (4 row) — dataset_clean artifact 형태 |
| `clause_label.jsonl` | clauses (5 row) — clause_label artifact 형태. `clause_id` 없음 (executor가 load 시 자동 생성) |
| `doc_genuineness.jsonl` | genuineness (4 row) — doc_genuineness artifact 형태 |
| `aspect_delta_plan.json` | 위 시나리오를 LLM 없이 손으로 작성한 8 step plan_v2 |

## 사용처

- `workers/python-ai/tests/test_smoke_5a_aspect_delta.py` — 5-A unit smoke
- `scripts/smoke_analyze_v2_service.sh` — 동일 fixture로 Python service 직접 호출
- (후속) `scripts/smoke_analyze_v2_endpoint.sh` — compose/dev curl smoke

## 갱신 규칙

이 fixture를 수정할 때:
- `aspect_delta_plan.json` 형태가 바뀌면 5-A test 기대값도 같이 갱신
- artifact column이 변경되면 `planner_v2/schema.py`의 TABLE_SCHEMAS 잠금 test와
  같이 검토
