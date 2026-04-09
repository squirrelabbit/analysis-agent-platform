# Skill Implementation Status

이 문서는 `config/skill_bundle.json`에 등록된 runtime skill이 실제로 어떤 방식으로 구현되어 있는지 빠르게 보기 위한 상태표다.
제품 계약은 [skill_registry.md](skill_registry.md), 코드 source는 `workers/python-ai/src/python_ai_worker/skills/*`, `apps/control-plane/internal/skills/duckdb_runner.go`를 기준으로 본다.

안정도:
- `안정`: 결정론적이고 입력/출력이 예측 가능
- `중간`: backend, 사전, 환경 차이에 영향 받음
- `주의`: threshold, fallback, upstream 품질에 크게 좌우됨
- `LLM 의존`: 모델/프롬프트/장애 상태 영향이 큼

## plan skill 상태

| Skill | 구현 방식 | 안정도 | 메모 |
| --- | --- | --- | --- |
| `structured_kpi_summary` | DuckDB SQL 집계 | 안정 | 가장 예측 가능함 |
| `garbage_filter` | 룰베이스 필터 | 안정 | sidecar parquet 저장 지원 |
| `document_filter` | lexical overlap 필터 | 안정 | semantic retrieval은 아님 |
| `deduplicate_documents` | 휴리스틱 dedup | 중간 | threshold 민감 |
| `keyword_frequency` | 토큰 카운팅 | 안정 | 단순 집계 |
| `noun_frequency` | Kiwi 우선, regex fallback | 중간 | 형태소 backend 영향 |
| `sentence_split` | `kss` 우선, regex fallback | 중간 | 문장 span 저장 가능 |
| `time_bucket_count` | deterministic bucket 집계 | 안정 | 추세 요약 upstream |
| `meta_group_count` | deterministic group-by | 안정 | breakdown upstream |
| `document_sample` | query/source 기반 샘플링 | 안정 | preview seed |
| `dictionary_tagging` | 사전 태깅 | 중간 | taxonomy coverage 영향 |
| `embedding_cluster` | materialized cluster 우선, subset fallback | 주의 | mode와 threshold 민감 |
| `cluster_label_candidates` | top term + sample 기반 라벨 후보 | 주의 | label 품질 보정 필요 |
| `semantic_search` | pgvector 우선, sidecar fallback | 중간 | 인덱스 품질 의존 |
| `evidence_pack` | grounded LLM presenter | LLM 의존 | evidence 선택 품질 중요 |
| `unstructured_issue_summary` | prior artifact 재사용 | 중간 | fallback 존재 |
| `issue_breakdown_summary` | `meta_group_count` 재사용 | 안정 | deterministic 위주 |
| `issue_cluster_summary` | cluster + membership 요약 | 주의 | cluster upstream 영향 큼 |
| `issue_trend_summary` | `time_bucket_count` 재사용 | 안정 | deterministic 위주 |
| `issue_period_compare` | bucket 비교 | 안정 | period 해석만 주의 |
| `issue_sentiment_summary` | sentiment artifact 집계 | 중간 | label 품질 영향 |
| `issue_taxonomy_summary` | dictionary tagging 집계 | 중간 | taxonomy coverage 영향 |
| `issue_evidence_summary` | evidence selection + LLM/fallback | LLM 의존 | 사용자 체감 영향 큼 |

## dataset build / presentation

| Task | 구현 방식 | 안정도 | 메모 |
| --- | --- | --- | --- |
| `dataset_prepare` | regex 정제 + LLM prepare/fallback | LLM 의존 | 비정형 파이프라인 입구 |
| `sentiment_label` | batch sentiment labeling | LLM 의존 | labeled parquet 생성 |
| `embedding` | dense embedding + token-overlap fallback | 중간 | dense unavailable 시 fallback |
| `dataset_cluster_build` | materialized clustering | 중간 | `summary JSON + membership parquet` 생성 |
| `execution_final_answer` | LLM 후처리 + fallback summarizer | LLM 의존 | 최종 답변 레이어 |

## 지금 우선적으로 볼 축

- 가장 안정적인 축
  - `structured_kpi_summary`, `garbage_filter`, `keyword_frequency`, `time_bucket_count`, `meta_group_count`
- 품질 보정이 가장 필요한 축
  - `embedding_cluster`, `cluster_label_candidates`, `issue_cluster_summary`
- 모델/프롬프트 의존 축
  - `dataset_prepare`, `sentiment_label`, `issue_evidence_summary`, `evidence_pack`, `execution_final_answer`

## 운영 메모

- python-ai 구현은 현재 `preprocess / aggregate / retrieve / summarize / presentation` 기준으로 나뉘어 있다.
- `support.py`, `core.py`는 이전 import path 호환용 shim이고, 실제 구현 본문은 private `*_impl.py` 파일로 나뉘어 있다.
- `embedding_cluster`, `cluster_label_candidates`, `issue_evidence_summary`는 `config/skill_policies/*.json` 기반 versioned policy를 읽는다.
- `확인 필요:` cluster label 품질과 final answer prompt 품질은 representative dataset 기준 추가 검증이 더 필요하다.
