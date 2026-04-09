# Skill Implementation Status

## 목적

이 문서는 `config/skill_bundle.json`에 등록된 runtime skill이 **실제로 어떤 방식으로 구현되어 있는지**를 빠르게 파악하기 위한 상태표다.

- `docs/skill/skill_registry.md`가 제품 계약 문서라면,
- 이 문서는 구현 방식, 의존성, 안정도, 운영 리스크를 설명하는 구현 상태 문서다.

코드 기준 source:
- `workers/python-ai/src/python_ai_worker/skills/preprocess.py`
- `workers/python-ai/src/python_ai_worker/skills/aggregate.py`
- `workers/python-ai/src/python_ai_worker/skills/retrieve.py`
- `workers/python-ai/src/python_ai_worker/skills/summarize.py`
- `workers/python-ai/src/python_ai_worker/skills/dataset_build.py`
- `workers/python-ai/src/python_ai_worker/skills/presentation.py`
- `apps/control-plane/internal/skills/duckdb_runner.go`

참고:
- `workers/python-ai/src/python_ai_worker/skills/support.py`
- `workers/python-ai/src/python_ai_worker/skills/core.py`
  는 현재 legacy import 호환용 shim이다.

## 분류 기준

| 구분 | 의미 |
| --- | --- |
| 전처리 | 필터링, 샘플링, dedup, sentence split |
| 집계 | counting, grouping, dictionary tagging |
| 검색/군집 | semantic retrieval, cluster retrieval, cluster labeling |
| 요약 | prior artifact를 재사용하거나 여러 deterministic 결과를 합성 |
| 룰베이스/집계 | 정규식, 토큰 카운팅, group-by, deterministic rule 중심 |
| 경량 NLP | 형태소 분석, 문장 분리, 사전 태깅, 휴리스틱 유사도 |
| 임베딩/검색 | dense embedding, token-overlap, pgvector, cluster materialization |
| LLM 생성 | Anthropic prompt 호출 또는 fallback summarizer 사용 |
| 조합형 요약 | prior artifact를 재사용하거나 여러 deterministic 결과를 합성 |

안정도 기준:
- `안정`: 구현이 결정론적이고 입력/출력이 예측 가능함
- `중간`: backend/사전/환경에 따라 품질 차이가 있음
- `주의`: threshold, fallback, upstream 품질에 크게 좌우됨
- `LLM 의존`: 모델 상태, 프롬프트, 비용/장애에 영향 받음

## Plan Skill 상태

| Skill | 엔진 | 구현 방식 | 주요 의존성 | 안정도 | 메모 |
| --- | --- | --- | --- | --- | --- |
| `structured_kpi_summary` | DuckDB | SQL 집계 | DuckDB, 요청 dataset | 안정 | 시계열/메트릭 column만 맞으면 가장 예측 가능함 |
| `garbage_filter` | python-ai | 룰베이스 필터 | garbage rule set | 안정 | sidecar parquet 저장 지원 |
| `document_filter` | python-ai | lexical overlap 필터 | tokenizer | 안정 | semantic retrieval이 아니라 token overlap 기반 |
| `deduplicate_documents` | python-ai | 경량 NLP/휴리스틱 | normalized text, token set similarity | 중간 | threshold에 민감함 |
| `keyword_frequency` | python-ai | 토큰 카운팅 | tokenizer | 안정 | 가장 단순한 support skill |
| `noun_frequency` | python-ai | 경량 NLP | Kiwi 우선, regex fallback | 중간 | `확인 필요:` Kiwi 미설치 시 품질 저하 가능 |
| `sentence_split` | python-ai | 경량 NLP | `kss` 우선, regex fallback | 중간 | sentence span sidecar parquet 저장 가능 |
| `time_bucket_count` | python-ai | deterministic bucket 집계 | timestamp parse, bucket helper | 안정 | `issue_trend_summary` upstream |
| `meta_group_count` | python-ai | deterministic group-by | dimension column | 안정 | `issue_breakdown_summary` upstream |
| `document_sample` | python-ai | ranking/샘플링 | query overlap, source order | 안정 | preview/evidence seed 역할 |
| `dictionary_tagging` | python-ai | 경량 NLP/사전 태깅 | taxonomy dictionary | 중간 | taxonomy coverage가 품질 핵심 |
| `embedding_cluster` | python-ai | 임베딩/군집 | cluster summary JSON, membership parquet, on-demand fallback | 주의 | full-dataset은 materialized cluster 우선, subset은 fallback |
| `cluster_label_candidates` | python-ai | 경량 NLP/휴리스틱 | `top_terms` | 주의 | label 품질이 가장 약한 축 중 하나 |
| `semantic_search` | python-ai | 임베딩/검색 | pgvector 우선, sidecar fallback | 중간 | 인덱스와 embedding 품질에 영향 받음 |
| `evidence_pack` | python-ai | LLM 생성 | selected evidence, Anthropic/fallback | LLM 의존 | grounded presenter 성격 |
| `unstructured_issue_summary` | python-ai | 조합형 요약 | keyword/sample artifact 재사용 | 중간 | 없으면 token/sample fallback |
| `issue_breakdown_summary` | python-ai | 조합형 요약 | `meta_group_count` | 안정 | prior artifact reuse가 기본 |
| `issue_cluster_summary` | python-ai | 조합형 요약 | cluster result, membership parquet | 주의 | cluster upstream 품질 영향 큼 |
| `issue_trend_summary` | python-ai | 조합형 요약 | `time_bucket_count` | 안정 | deterministic 재사용 위주 |
| `issue_period_compare` | python-ai | deterministic 비교 | bucket grouping, term delta | 안정 | period inference만 주의 |
| `issue_sentiment_summary` | python-ai | 조합형 요약 | sentiment labeled parquet | 중간 | label 품질에 좌우됨 |
| `issue_taxonomy_summary` | python-ai | 조합형 요약 | dictionary tagging result | 중간 | taxonomy coverage 의존 |
| `issue_evidence_summary` | python-ai | LLM 생성 | evidence candidate selection, Anthropic/fallback | LLM 의존 | 사용자 체감 영향 가장 큼 |

## Dataset Build Task 상태

| Task | 구현 방식 | 주요 의존성 | 안정도 | 메모 |
| --- | --- | --- | --- | --- |
| `dataset_prepare` | regex 정제 + LLM prepare/fallback | prepare prompt, regex rules, Anthropic | LLM 의존 | 전체 비정형 파이프라인의 입구 |
| `sentiment_label` | batch sentiment labeling | sentiment prompt, Anthropic | LLM 의존 | parquet로 labeled dataset 생성 |
| `embedding` | dense embedding + token-overlap fallback | OpenAI/local model 또는 token-overlap | 중간 | dense unavailable 시 fallback |
| `dataset_cluster_build` | materialized clustering | embedding index parquet, chunk parquet | 중간 | summary JSON + membership parquet 생성 |

## Presentation Layer

| Task | 구현 방식 | 주요 의존성 | 안정도 | 메모 |
| --- | --- | --- | --- | --- |
| `execution_final_answer` | LLM 후처리 + fallback summarizer | `result_v1`, evidence candidates, prompt template | LLM 의존 | 최종 사용자 답변 레이어 |

## 구현 경계 요약

### 1. deterministic / 운영 안정성이 높은 축

- `structured_kpi_summary`
- `garbage_filter`
- `keyword_frequency`
- `time_bucket_count`
- `meta_group_count`
- `issue_trend_summary`
- `issue_breakdown_summary`
- `issue_period_compare`

이 축은 입력이 같으면 결과가 거의 그대로 재현된다.

### 2. backend / 사전 / threshold에 민감한 축

- `deduplicate_documents`
- `noun_frequency`
- `sentence_split`
- `dictionary_tagging`
- `semantic_search`
- `issue_sentiment_summary`
- `issue_taxonomy_summary`
- `embedding`
- `dataset_cluster_build`

이 축은 설치 환경, 사전 coverage, 인덱스 상태, similarity threshold에 따라 체감 품질이 달라질 수 있다.

### 3. 현재 품질 보정이 가장 필요한 축

- `embedding_cluster`
- `cluster_label_candidates`
- `issue_cluster_summary`

이 축은 cluster threshold, fallback mode, top term quality에 크게 좌우된다.

### 4. 모델/프롬프트 의존 축

- `dataset_prepare`
- `sentiment_label`
- `issue_evidence_summary`
- `evidence_pack`
- `execution_final_answer`

이 축은 deterministic contract 위에 LLM을 얹는 구조라서, fallback과 prompt version 관리가 중요하다.

## 운영 관점 메모

- python-ai skill 구현은 현재 `preprocess / aggregate / retrieve / summarize / presentation` 기준으로 나뉘어 있다.
- `support.py`, `core.py`는 runtime 주 진입점이 아니라 legacy import 호환 레이어다.
- broad cluster 질문은 planner가 `materialized cluster` 경로를 우선 타도록 최근 정리되었다.
- subset/filter가 앞에 붙는 cluster 질문은 현재도 on-demand fallback을 사용한다.
- worker capability surface는 실제 실행 가능한 `python-ai` task만 노출하도록 정리되었다.
- `확인 필요:` cluster label 품질과 final answer prompt 품질은 representative dataset 기준 추가 검증이 더 필요하다.
