# LLM Prompt Bundle Mapping Audit (2026-04-27)

## 0. 목적

- `workers/python-ai/src/python_ai_worker/runtime/llm.py`의 planner 프롬프트가 어떤 하드코딩 문자열에 의존하는지 정리한다.
- `config/skill_bundle.json`이 그 문자열의 source of truth가 될 수 있는지 확인한다.
- Step 2 구현에서 필요한 bundle 확장 범위를 최소화한다.

## 1. 현재 하드코딩 위치

기준 파일: [workers/python-ai/src/python_ai_worker/runtime/llm.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py)

- `Allowed skills:` 줄은 `plan_skill_names()`로 생성된다.
  - 근거: [runtime/llm.py:104-109](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:104)
- skill별 설명 문장은 모두 하드코딩이다.
  - 근거: [runtime/llm.py:110-131](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:110)
- sequence 추천 규칙도 모두 하드코딩이다.
  - 근거: [runtime/llm.py:132-143](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:132)
- `issue_evidence_summary`, `semantic_search` 입력 규칙도 하드코딩이다.
  - 근거: [runtime/llm.py:142-144](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:142)
- planner 응답 검증은 여전히 `plan_skill_names()` 집합에 의존한다.
  - 근거: [runtime/llm.py:1308-1315](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:1308)

## 2. Bundle 필드 매핑 판단

### 2.1 skill 이름 목록

- 현재 source:
  - `plan_skill_names()`
  - 근거: [workers/python-ai/src/python_ai_worker/skill_bundle.py:22-29](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skill_bundle.py:22)
- 문제:
  - deprecated alias도 그대로 노출된다.
  - 현재 `keyword_frequency`, `evidence_pack`가 planner-visible 이름 집합에 포함된다.
  - 근거: [config/skill_bundle.json:196](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:196), [config/skill_bundle.json:673](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:673)
- 판단:
  - Step 2에서는 planner prompt용 visible skill 집합을 별도로 만들어 deprecated alias를 숨기는 것이 맞다.

### 2.2 skill 설명 문구

- 현재 source 후보:
  - `description`
  - 근거: [workers/python-ai/src/python_ai_worker/skill_bundle.py:69-77](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skill_bundle.py:69)
- 샘플 확인:
  - `term_frequency`: "Count top terms from filtered unstructured rows..."
  - `issue_evidence_summary`: "Build user-facing evidence summaries for text analysis."
  - 근거: [config/skill_bundle.json:208](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:208), [config/skill_bundle.json:652](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:652)
- 판단:
  - 현재 bundle `description`은 영어이며 LLM prompt용 한 줄 요약으로 재사용 가능하다.
  - 별도 `planner_prompt_summary` 필드는 이번 단계에서 불필요하다.

### 2.3 sequence 추천 규칙

- 현재 source 후보:
  - `planner_sequences`
  - 근거: [workers/python-ai/src/python_ai_worker/skill_bundle.py:52-56](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skill_bundle.py:52)
- 문제:
  - `planner_sequences`는 skill 배열만 있고, "언제 이 sequence를 추천하는지"에 대한 영어 문구는 없다.
  - 현재 LLM prompt의 추천 규칙은 코드 안 문자열이다.
  - 근거: [runtime/llm.py:132-143](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:132)
- 판단:
  - Step 2에서는 top-level `planner_recommendations`가 필요하다.
  - 권장 스키마:

```json
{
  "planner_recommendations": [
    {
      "sequence_name": "unstructured_default",
      "when": "For general unstructured text analysis",
      "requires_query_goal_input_for": ["issue_evidence_summary"]
    }
  ]
}
```

### 2.4 입력 규칙

- 현재 하드코딩 규칙:
  - `issue_evidence_summary`, `semantic_search`에 `inputs.query = goal`
  - 근거: [runtime/llm.py:142-144](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:142)
- bundle 대응:
  - 이미 `goal_input` 필드가 있다.
  - 근거: [config/skill_bundle.json:651](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:651), [config/skill_bundle.json:618](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/skill_bundle.json:618)
- 판단:
  - Step 2에서는 입력 규칙용 별도 필드 추가 없이 `goal_input`을 prompt 설명에 반영하면 된다.

## 3. 권장 구현 방향

### 결론 1. 새 skill-level 필드는 불필요

- `description`은 이미 planner prompt용으로 충분하다.
- Step 2에서 deprecated alias를 제외한 visible skill 목록과 `description`을 직렬화하면 된다.

### 결론 2. 새 top-level 필드는 필요

- `planner_sequences`만으로는 "언제 어떤 sequence를 권장하는가"를 영어로 설명할 수 없다.
- 따라서 bundle top-level에 `planner_recommendations`를 추가하는 것이 맞다.

### 결론 3. planner 검증은 canonical 기준으로 보정해야 한다

- 현재 `_normalize_planner_response()`는 raw `skill_name`을 그대로 검증한다.
- Step 2에서는 `canonical_skill_name()`을 먼저 적용한 뒤 visible skill 집합 기준으로 검증해야 한다.
- 그래야 모델이 `keyword_frequency`를 반환해도 최종 plan은 `term_frequency`로 정규화된다.

## 4. 현재 prompt snapshot

핵심 block 원문:

```text
Allowed skills: {plan_skill_names() 결과}.
Use keyword_frequency to count top terms after document filtering.
Use issue_evidence_summary to return representative snippets and follow-up questions for text analysis.
For general unstructured text analysis, prefer document_filter, keyword_frequency, document_sample, unstructured_issue_summary, then issue_evidence_summary.
For trend analysis, prefer document_filter, time_bucket_count, document_sample, issue_trend_summary, then issue_evidence_summary.
```

근거:
- [runtime/llm.py:109-144](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/llm.py:109)

## 5. Step 2에 바로 넘길 결정

- 채택 권장:
  - `description` 재사용
  - `planner_recommendations` 신설
  - deprecated alias는 planner prompt에서 숨김
  - planner 응답은 canonical skill name으로 정규화
- 현재 미채택:
  - `planner_prompt_summary`
  - 별도 prompt 전용 skill schema 파일

## 6. 확인 필요

- `planner_recommendations.when`의 문체를 완전 자연어로 둘지, `goal_pattern` 같은 더 구조화된 필드로 둘지는 Step 2 구현 중 추가 판단이 필요하다.
- `mixed_default` 추천 문구를 현재처럼 독립 규칙으로 둘지, `structured_default + unstructured_default` 조합 설명으로 풀지는 확인 필요.
