# Skill Registry

## 목적

Skill Registry는 planner, workflow, worker가 함께 공유하는 공식 Skill 계약이다.

- planner는 registry 밖의 Skill을 제안하면 안 된다.
- workflow는 registry 밖의 Skill을 스케줄링하면 안 된다.
- worker는 registry에 없는 Skill 이름을 실행하면 안 된다.

즉, registry는 언어별 구현 목록이 아니라 **제품이 공식 지원하는 분석 동작 목록**이다.

## Skill 계약 원칙

- Skill 이름은 고정 문자열이어야 한다.
- 입력/출력 스키마는 명시적이어야 한다.
- 실행은 같은 입력과 같은 데이터에서 결정론적이어야 한다.
- Skill 구현 언어는 달라도 계약 이름과 스키마는 같아야 한다.
- 동적 코드 생성에 의존하지 않는다.

## 구현 경계

- `Go`는 Skill registry 메타데이터와 execution routing을 담당한다.
- `DuckDB`는 structured Skill 계산을 담당한다.
- `Python worker`는 planner, embeddings, semantic search, evidence generation을 담당한다.
- 현재 Python worker 실행 경로에는 `document_filter`, `keyword_frequency`, `time_bucket_count`, `meta_group_count`, `document_sample`, `unstructured_issue_summary`, `issue_breakdown_summary`, `issue_trend_summary`, `issue_period_compare`, `issue_sentiment_summary`, `semantic_search`, `issue_evidence_summary`, `evidence_pack`가 연결되어 있다.
- `Rust worker`는 CPU 집약 Skill 커널을 담당한다.

## Core Skill 방향

Structured:
- `structured_kpi_summary`
- `period_compare_summary`
- `dimension_breakdown_summary`
- `topn_rank_summary`

Unstructured:
- `unstructured_issue_summary`
- `issue_trend_summary`
- `issue_period_compare`
- `issue_breakdown_summary`
- `issue_sentiment_summary`
- `issue_evidence_summary`
- `issue_cluster_summary`

## Support Skill 방향

Structured support:
- `dataset_profile`
- `aggregate`
- `rank`
- `timeseries_peak`
- `compare_period`

Unstructured support:
- `dataset_prepare`
- `sentiment_label`
- `document_filter`
- `keyword_frequency`
- `time_bucket_count`
- `meta_group_count`
- `document_sample`
- `evidence_pack`
- `semantic_search`
- `embedding_cluster`
- `cluster_label_candidates`
- `deduplicate_documents`

## 제품 관점

재현성은 질문 문자열이 아니라 `plan + dataset_version + skill contract + execution metadata` 조합으로 성립한다.

따라서 registry는 단순 목록이 아니라 replay contract의 일부다.
