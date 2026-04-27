# Skill Bundle Schema

`config/skill_bundle.json` 의 각 skill entry는 아래 선언 필드를 포함한다.

## Enum

### `result_kind`

- `preprocessing`: downstream selection/정제용 산출물
- `evidence`: 근거 후보/샘플/검색 결과
- `summary_ranked`: 순위형 요약 결과
- `summary_narrative`: 사람 읽는 문장 중심 결과
- `metric_table`: 집계/비교/분포 표형 결과
- `cluster_output`: cluster/label 후보 결과
- `dataset_artifact`: dataset build 산출물

### `result_scope`

- `full_dataset`: 데이터셋 전체가 처리된 provenance
- `document_subset`: 문서 필터/정제/질의 결과로 좁혀진 문서 집합 provenance
- `cluster_subset`: 특정 cluster member 집합 provenance
- `partial_build`: dataset build 단계에서 `max_rows` 등으로 잘린 부분 빌드 provenance

### `result_scope_policy`

- `static`: 선언된 `result_scope` 가 runtime provenance 와 동일해야 한다.
- `inherits_from_input`: 가장 최근 prior artifact 의 provenance 를 계승한다.
- `dynamic`: handler 가 `runtime_result_scope` 를 직접 채우고, 선언된 허용 집합 안에 있어야 한다.

### `fallback_policy`

- `strict_fail`: 빈 결과나 fallback 허용 없이 실패
- `graceful_empty`: 빈 결과를 명시적으로 반환 가능
- `rule_fallback_allowed`: 규칙 기반 fallback 허용

### `quality_tier`

- `deterministic`: 입력이 같으면 동일 결과를 기대하는 계산
- `heuristic`: 휴리스틱/모델/유사도 로직이 포함되지만 재현 가능한 계산
- `llm_dependent`: LLM 응답 품질에 의존하는 결과

## Validation Rule

- planner prompt에서 skill 설명은 각 skill의 `description`을 사용한다.
- planner prompt에서 deprecated alias(`deprecated_alias_of`가 있는 entry)는 기본적으로 노출하지 않는다.
- `planner_recommendations`는 LLM planner의 추천 sequence 문구 source다.
  - `sequence_name`: `planner_sequences`의 key
  - `when`: 해당 sequence를 추천하는 영어 문장
- 모든 skill artifact 는 `result_scope` 와 `runtime_result_scope` 를 함께 가진다.
- `result_scope` 는 선언값이고, `runtime_result_scope` 는 실제 provenance 다.
- `result_scope_policy=static` 인 skill 은 `runtime_result_scope == result_scope` 여야 한다.
- `result_scope_policy=inherits_from_input` 인 skill 은 prior artifact 의 provenance 를 계승해야 한다.
- `result_scope_policy=dynamic` 인 skill 은 `allowed_runtime_result_scopes` 선언 범위 안에서만 runtime provenance 를 반환할 수 있다.
- `fallback_policy=strict_fail` 인 skill은 post-check에서 graceful empty 결과를 허용하지 않는다.
- `quality_tier=llm_dependent` 인 skill이 planner sequence에 포함되면 plan metadata에 `contains_llm_stage`, `llm_stages`를 기록한다.
- analyst-facing skill은 runtime artifact에도 `coverage`, `quality_tier`를 포함해 실제 적용 범위를 드러낸다.
