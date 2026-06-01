# scripts/

## analyze smoke (plan_v2 / planner / executor 검증)

| script | 범위 | 의존 |
|---|---|---|
| `smoke_analyze_service.sh` | Python `execute_analyze_plan` direct 호출 — compose 없이 deterministic 경로만 | Python venv + committed fixture |
| `smoke_analyze_endpoint.sh` | python-ai-worker `/tasks/plan_v2` + `/tasks/analyze_v2` 4 case (direct plan / user_question / ambiguous fail) | compose dev (python-ai-worker만) + ANTHROPIC_API_KEY |
| `smoke_analyze_e2e.sh` | Go control plane `POST /projects/{pid}/datasets/{did}/analyze` active version mode end-to-end | compose dev (control-plane + python-ai-worker + postgres) + (optional) ANTHROPIC_API_KEY |

`smoke_analyze_e2e.sh`는 두 mode를 분리 실행:

```bash
./scripts/smoke_analyze_e2e.sh                          # direct-plan (기본)
./scripts/smoke_analyze_e2e.sh --mode direct-plan
./scripts/smoke_analyze_e2e.sh --mode user-question     # ANTHROPIC_API_KEY 필요
```

direct-plan은 committed fixture의 `aspect_delta_plan.json` + 기대값 6개 lock.
user-question은 real LLM 호출이라 plan이 매번 달라질 수 있어 `present row_count ≥ 3`만 검증.

rename PR (2026-05-21) 메모: HTTP endpoint는 `/analyze`로 이전. `/analyze_v2`는 일정 기간 deprecated alias 유지. Python worker task path `/tasks/analyze_v2`는 wire contract라 유지. response body `plan_version: "v2"`도 유지.

## fixture

committed 단일 출처: `workers/python-ai/tests/fixtures/plan_v2_smoke/`
- `cleaned.parquet` / `clause_label.jsonl` / `doc_genuineness.jsonl` / `aspect_delta_plan.json` / `README.md`

`data/plan_v2_smoke/`는 e2e script가 compose volume mount용으로 임시 mirror하는 위치 — gitignored, smoke 끝나면 cleanup.

## 그 외

| script | 용도 |
|---|---|
| `festival_5_query.sh` | festival dataset 5 query 자동화 (4/30 사용자 가치 검증) |
| `migrate_dataset_build_v2.py` | ADR-012 후속 DB 정리 |
| `migrate_drop_legacy_project_assets.sql` | aspect_taxonomy / garbage_rule / stopwords 9 테이블 drop |
| `skill_usage_summary.sh` | skill.usage obs log 집계 (δ-3에서 Go 측 emit 사라짐 — log line 자체가 더 안 나옴. 후속 instrumentation 시 재활용) |
| `smoke_doc_genuineness.sh` / `smoke_preprocess_pipeline.sh` | dataset_build 단계별 smoke |
