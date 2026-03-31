# Skill별 분석 기술

## 목적

이 문서는 현재 저장소에 구현된 skill이 어떤 분석 기술을 사용하는지 빠르게 확인하기 위한 기준 문서다.

- 코드 기준으로 정리한다.
- 실험적이거나 외부 모델 의존이 있는 경우는 명시한다.
- 문서화 대상은 plan 실행 skill과 dataset build task를 모두 포함한다.

## Structured

| Skill | 사용 기술 | 설명 |
| --- | --- | --- |
| `structured_kpi_summary` | SQL 집계, 시계열 bucket 집계, DuckDB | row count, sum, avg, min, max와 bucket series를 DuckDB query로 계산한다. |

## Dataset Build Task

| Skill | 사용 기술 | 설명 |
| --- | --- | --- |
| `dataset_prepare` | 텍스트 정규화, 노이즈 판별, 선택적 LLM 정제 | 기본은 정규식 기반 normalization과 noise 판별을 수행하고, 설정 시 Anthropic 모델로 row 단위 정제를 시도한다. |
| `sentiment_label` | 감성 사전 기반 분류, 선택적 LLM 분류 | fallback은 긍정/부정 용어 사전 매칭으로 라벨을 붙이고, 설정 시 Anthropic 분류를 우선 시도한다. |
| `embedding` | Bag-of-Words 벡터화, token count, vector norm | 문서를 토큰 카운트 벡터와 norm으로 저장하는 deterministic embedding sidecar를 만든다. |

## Unstructured Support

| Skill | 사용 기술 | 설명 |
| --- | --- | --- |
| `document_filter` | Lexical retrieval, token overlap ranking | 질의와 문서의 토큰 중첩 개수로 문서를 좁힌다. |
| `deduplicate_documents` | 텍스트 정규화, exact match, token-set Jaccard similarity | 정규화 텍스트 동일성 또는 토큰 집합 유사도로 대표 문서와 중복 문서를 묶는다. |
| `keyword_frequency` | 토큰화, term frequency | 선택된 문서의 상위 용어 빈도를 계산한다. |
| `time_bucket_count` | 시계열 bucket 집계, term frequency by bucket | 날짜 컬럼을 day/week/month bucket으로 묶고 bucket별 건수와 상위 용어를 만든다. |
| `meta_group_count` | categorical group-by, term frequency by group | 메타데이터 차원값별 건수와 상위 용어를 계산한다. |
| `document_sample` | query overlap ranking, representative sampling | 질의 중첩 점수 또는 source order로 대표 문서를 고른다. |
| `dictionary_tagging` | rule-based taxonomy tagging, pattern matching | 사전 기반 taxonomy 규칙으로 문서에 카테고리 태그를 붙인다. |
| `embedding_cluster` | token vector cosine similarity, greedy clustering | 미리 생성된 token vector를 읽어 유사도 기준으로 문서를 cluster에 할당한다. |
| `cluster_label_candidates` | top-term labeling, heuristic phrase generation | cluster 상위 용어로 label 후보를 만든다. |
| `semantic_search` | cosine similarity search over token vectors | query vector와 문서 vector의 cosine similarity로 evidence 후보를 찾는다. |
| `evidence_pack` | snippet selection, optional LLM summarization, fallback summarizer | 선택된 문서 조각을 기반으로 evidence bundle을 만든다. |

## Unstructured Core

| Skill | 사용 기술 | 설명 |
| --- | --- | --- |
| `unstructured_issue_summary` | term frequency summary, representative sampling | 상위 용어와 대표 문서 샘플로 일반 이슈 요약 artifact를 만든다. |
| `issue_breakdown_summary` | metadata breakdown summary | `meta_group_count` 결과를 재사용하거나 직접 group-by 요약을 만든다. |
| `issue_cluster_summary` | cluster ranking, label reuse, representative cluster summary | `embedding_cluster`와 `cluster_label_candidates` 결과를 묶어 주요 이슈 군집을 요약한다. |
| `issue_trend_summary` | timeseries summary | `time_bucket_count` 결과를 재사용하거나 직접 시계열 요약을 만든다. |
| `issue_period_compare` | period delta analysis, term delta analysis | 현재 기간과 직전 기간의 문서 수와 상위 용어 변화를 비교한다. |
| `issue_sentiment_summary` | label distribution analysis | 감성 라벨 분포와 대표 예시를 집계한다. |
| `issue_taxonomy_summary` | taxonomy distribution analysis | dictionary tagging 결과를 요약해 dominant taxonomy와 category breakdown을 만든다. |
| `issue_evidence_summary` | evidence selection, optional LLM summarization, fallback summarizer | 최종 사용자용 근거 요약과 follow-up question을 만든다. |

## Planner

| Skill | 사용 기술 | 설명 |
| --- | --- | --- |
| `planner` | rule-based intent routing, optional LLM structured planning | 기본은 goal keyword 기반 skill plan 조합이며, 설정 시 Anthropic structured JSON planner를 우선 사용한다. |

## 해석 메모

- 현재 비정형 skill의 핵심 deterministic 기술은 `토큰화`, `사전 매칭`, `빈도 분석`, `bucket 집계`, `bag-of-words cosine similarity`, `greedy clustering`이다.
- LLM은 planner, evidence summary, dataset prepare, sentiment labeling에서 선택적으로 사용되고, 실패 시 deterministic fallback으로 내려간다.
- 확인 필요: 실제 운영 단계에서 clustering이나 dedup을 Rust hot path로 옮길지는 별도 성능 측정 결과에 따라 결정해야 한다.
