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

- `full_dataset`: 전체 dataset 기준
- `subset_filtered`: prior artifact 등으로 좁혀진 subset 기준
- `sample_n`: 제한된 샘플 수 기준
- `single_record`: 단일 결과 문서/응답 기준

### `fallback_policy`

- `strict_fail`: 빈 결과나 fallback 허용 없이 실패
- `graceful_empty`: 빈 결과를 명시적으로 반환 가능
- `rule_fallback_allowed`: 규칙 기반 fallback 허용

### `quality_tier`

- `deterministic`: 입력이 같으면 동일 결과를 기대하는 계산
- `heuristic`: 휴리스틱/모델/유사도 로직이 포함되지만 재현 가능한 계산
- `llm_dependent`: LLM 응답 품질에 의존하는 결과

## Validation Rule

- `fallback_policy=strict_fail` 인 skill은 post-check에서 graceful empty 결과를 허용하지 않는다.
- `quality_tier=llm_dependent` 인 skill이 planner sequence에 포함되면 plan metadata에 `contains_llm_stage`, `llm_stages`를 기록한다.
- analyst-facing skill은 runtime artifact에도 `result_scope`, `coverage`, `quality_tier`를 포함해 실제 적용 범위를 드러낸다.

## 확인 필요

- `result_scope` 는 bundle 선언값과 실제 runtime artifact 값이 완전히 일치하지 않을 수 있다.
  예: `embedding_cluster` 는 전체 dataset 실행과 filtered subset 실행을 모두 가질 수 있다.
