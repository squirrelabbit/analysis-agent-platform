# Quality eval set v1

목적: 개별 skill 출력의 **품질**을 정량 검증한다. planner correctness만 잠그는
`docs/eval/skill_regression_v1.yaml`과는 별도의 트랙이다.

## 두 eval 트랙의 역할 차이

| 항목 | regression v1 | quality v1 |
| --- | --- | --- |
| 검증 대상 | planner가 query → plan을 올바르게 생성하는지 | skill이 입력 → artifact를 의미적으로 적절히 생성하는지 |
| 통과 기준 | active_layers + canonical step family 정확 일치 | summary 키워드 coverage, evidence count 범위, quality_tier 일치 등 |
| 데이터 의존 | 없음 (`dataset_ref`는 라벨일 뿐) | 있음 (`docs/eval/quality_v1/datasets/`의 실 도메인 sample) |
| 깨졌을 때 신호 | 새로 추가한 planner rule이 기존 plan을 바꿈 | prompt 변경/모델 교체로 출력 품질이 떨어짐 |

regression set의 `dataset_ref` 필드(`issues.csv` 등)는 plan 생성 입력일 뿐
실 데이터 내용은 안 읽는다. quality set은 그 빈칸을 메우기 위해 도입됐다.

## 디렉토리 구조

```
docs/eval/quality_v1/
├── README.md                 ← 이 문서
├── cases/                    ← skill별 eval case YAML
│   ├── issue_evidence_summary.yaml
│   └── execution_final_answer.yaml
└── datasets/                 ← case가 참조하는 실 도메인 sample CSV
    ├── festival_sample_50.csv
    └── festival_sample_empty.csv  (negative test fixture, header-only)
```

## Dataset

| 파일 | 도메인 | rows | 출처 |
| --- | --- | --- | --- |
| `festival_sample_50.csv` | 강릉 문화재 야행 SNS 수집 (네이버 블로그·카페) | 50 | `data/festival.csv`(2121 rows) stratified sample |

익명화 처리:
- `작성자` → SHA256 8자 prefix (`anon-XXXXXXXX`)
- `URL` → `https://example.invalid/{cafe|blog|post}/<hash>`
- `본문` → "존재하지 않는 이미지입니다." 노이즈 제거 외에는 원본 유지

추가 sampling 절차는 `scripts/sample_festival_for_eval.py`를 둘 자리. (TODO,
Phase 3에서 reproducible 스크립트로 정리 예정)

## Case 작성 규칙

`cases/<skill_name>.yaml` 파일에 list 형태로 case들을 둔다. 각 case는
다음 필드를 가진다:

```yaml
- case_id: <짧고 stable한 식별자>
  description: <한국어 설명>
  skill_name: <plan_v2 skill name (planner_v2/schema.py:SKILL_CATALOG 기준)>
  payload: <run_task(skill_name, payload) 호출 입력>
  expected:
    summary_must_contain: [str, ...]      # artifact.summary 또는 동등 필드에 포함되어야 함
    summary_must_not_contain: [str, ...]  # 포함되면 안 됨 (anti-leakage)
    min_evidence_count: int               # len(artifact.evidence) >= N
    max_evidence_count: int               # len(artifact.evidence) <= N
    min_key_findings_count: int
    quality_tier: heuristic|deterministic|llm_dependent
    selection_source_in: [str, ...]
    min_caveat_count: int                 # len(artifact.caveats) >= N (final_answer)
    expect_skill_output_error: bool       # negative case (예: strict_fail 검증)
    expected_error_substring: str
```

Skill별 artifact 필드 명이 다른 경우(예: execution_final_answer는 `summary` 대신
`answer_text`, `key_findings` 대신 `key_points`)에도 동일 predicate 이름을
쓸 수 있도록 runner가 union으로 검색한다. 새로운 skill을 추가할 때 필드명이
또 다르다면 `_check_keyword_predicates` 등에 union 항목을 추가한다.

LLM 의존 skill의 경우 case는 default로 mocked stub과 함께 실행된다.
실제 Anthropic 호출이 필요하면 runner에 `--allow-llm`을 넘긴다.

## Runner

```bash
# 모든 case 실행 (mock LLM)
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_eval_case

# 특정 case
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_eval_case --case ev_001

# JSON 리포트 출력
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_eval_case --report report.json

# 실 LLM 사용 (ANTHROPIC_API_KEY 필요, 비용 발생)
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_eval_case --allow-llm
```

리포트 형식:

```json
{
  "summary": {"total": 5, "passed": 5, "failed": 0},
  "results": [
    {"case_id": "ev_001", "status": "pass", "skill_name": "issue_evidence_summary"},
    ...
  ]
}
```

## 추가 작업 (Phase 3+)

- embedding_cluster, cluster_label_candidates case 작성 (실 embedding 생성이
  필요해 stub 설계가 다소 무거움 — Phase 1·2 case와 분리)
- representative dataset 1-2개 추가 (다른 도메인)
- CI 통합 (`.gitlab-ci.yml`에 quality eval 단계 추가)
- `--baseline` 옵션 — 기존 score를 저장해 회귀 감지

ADR-012 (skill 통폐합) 결정 후, `issue_*_summary` 8종이 통폐합되면 case 구조도
함께 재정렬한다.

## 변경 이력

| 날짜 | 변경 |
| --- | --- |
| 2026-04-29 | Phase 1 — schema, festival_sample_50, issue_evidence_summary 5 case, runner |
| 2026-04-29 | Phase 2 — execution_final_answer 3 case, summary/key_findings union 검색, min_caveat_count predicate |
