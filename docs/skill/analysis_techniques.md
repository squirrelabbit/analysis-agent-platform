# Skill별 분석 기술

## 목적

이 문서는 현재 저장소에 구현된 skill이 어떤 방식으로 동작하는지와, 이후 어떤 방향으로 향상할 수 있는지를 빠르게 확인하기 위한 기준 문서다.

- 코드 기준으로 정리한다.
- `현재 구현 방식`은 현재 런타임에 반영된 내용만 적는다.
- `향상 가능 방향`은 후보 아이디어이며, 확정 로드맵으로 간주하지 않는다.
- 문서화 대상은 plan 실행 skill과 dataset build task를 모두 포함한다.

## 공통 구현 원칙

- 현재 비정형 skill의 기본 축은 `deterministic 전처리 + 규칙 기반 집계/검색 + 선택적 LLM 보강`이다.
- support skill이 먼저 artifact를 만들고, core skill이 그 결과를 재사용하는 구조를 우선한다.
- planner, evidence summary, dataset prepare, sentiment labeling은 Anthropic 경로가 있더라도 fallback 경로를 유지한다.
- 현재 `embedding`의 기본값은 `intfloat/multilingual-e5-small` FastEmbed local model 기반 dense 경로이고, 필요하면 OpenAI Embeddings API override를 줄 수 있다. 호출이 실패하면 `token-overlap-v1` fallback을 유지한다.
  - `embedding` task는 dense vector가 있더라도 `token_counts`와 `norm`을 같이 저장해 기존 clustering/debug 경로를 유지한다.
  - `semantic_search`는 dense index metadata가 있으면 같은 model로 query embedding을 만들고, 없으면 token vector cosine similarity로 fallback한다.
  - `embedding_cluster`는 dense vector가 있으면 lexical guardrail을 둔 `dense-hybrid` similarity를 우선 사용하고, 없으면 token vector cosine similarity로 fallback한다.

## Structured

| Skill | 한국어 이름 | 주요 구현 파일 | 현재 구현 방식 | 향상 가능 방향 |
| --- | --- | --- | --- | --- |
| `structured_kpi_summary` | 구조화 KPI 요약 | `apps/control-plane/internal/skills/duckdb_runner.go` | DuckDB SQL로 row count, sum, avg, min, max, time bucket series를 계산한다. | percentile/window function, metric template, schema validation, richer KPI contract를 추가할 수 있다. |

## Dataset Build Task

| Skill | 한국어 이름 | 주요 구현 파일 | 현재 구현 방식 | 향상 가능 방향 |
| --- | --- | --- | --- | --- |
| `dataset_prepare` | 데이터셋 정제 | `workers/python-ai/src/python_ai_worker/skills/dataset_build.py` | 정규식 기반 text normalization과 noise 판별을 기본으로 수행하고, 설정 시 Anthropic batch prepare를 우선 사용한다. 기본 prepare model은 `claude-3-5-haiku-latest`이며 prompt는 version registry에서 관리한다. 기본 `prepare_batch_size`는 8이며 결과는 `prepared.parquet` artifact로 저장한다. 각 row에는 `row_id`를 부여하고 artifact에는 `prepared_ref`, `prepare_format=parquet`을 함께 남긴다. 명시적으로 `.jsonl` output path를 주면 호환용 JSONL도 유지할 수 있다. | 언어별 normalization, quality score, column-aware cleaning, duplicate/noise classifier, adaptive batch sizing, sentiment/chunk sidecar까지 Parquet로 확장할 수 있다. |
| `sentiment_label` | 감성 라벨링 | `workers/python-ai/src/python_ai_worker/skills/dataset_build.py` | 감성 사전 기반 fallback 분류를 수행하고, 설정 시 Anthropic 분류를 우선 사용한다. 기본 model은 `claude-3-5-haiku-latest`이며 prompt는 version registry에서 관리한다. 입력은 prepare 완료 후 `prepared.parquet`를 직접 읽을 수 있고, 결과는 현재 `row_id`, `source_row_index`, 감성 컬럼 중심의 `sentiment.parquet` sidecar로 저장한다. | 도메인 특화 classifier, confidence calibration, aspect sentiment, label guideline 강화, prepared dataset join을 control plane 공통 helper로 끌어올리는 방향을 검토할 수 있다. |
| `embedding` | 임베딩 생성 | `workers/python-ai/src/python_ai_worker/skills/dataset_build.py` | 현재는 text-window 기반 `chunks.parquet`를 먼저 만들고, 기본 `embedding_model=intfloat/multilingual-e5-small` 기준 FastEmbed local model을 시도한다. `text-embedding-*` override를 주면 OpenAI 경로를 탄다. dense 호출이 가능하면 vector를 `embeddings.jsonl` record에 함께 저장하고, 불가하면 `token-overlap-v1` 방식의 `token_counts`와 `norm`만 남긴다. record에는 `row_id`, `chunk_id`, `chunk_index`, `char_start`, `char_end`를 함께 저장한다. control plane은 이 sidecar를 읽어 dense vector가 있으면 그대로, 없으면 token count를 64차원 hashed projection vector로 바꿔 `pgvector` table에 적재한다. | hybrid retrieval, ANN index, adaptive chunking, multilingual embedding, embedding cost control, cache 정책을 검토할 수 있다. |

## Unstructured Support

| Skill | 한국어 이름 | 주요 구현 파일 | 현재 구현 방식 | 향상 가능 방향 |
| --- | --- | --- | --- | --- |
| `document_filter` | 문서 필터링 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 질의와 문서의 token overlap 점수로 문서를 좁힌다. 매칭이 약하면 전체 row fallback도 사용한다. | BM25, hybrid retrieval, query expansion, field weighting, metadata filter를 추가할 수 있다. |
| `deduplicate_documents` | 중복 문서 제거 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 정규화 텍스트 exact match와 token-set Jaccard similarity로 대표 문서와 중복 문서를 묶는다. | MinHash/LSH, dense embedding 기반 near-duplicate 탐지, source-aware dedup, threshold tuning을 추가할 수 있다. |
| `keyword_frequency` | 키워드 빈도 집계 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 선택된 문서의 token frequency를 집계한다. | n-gram, TF-IDF, keyphrase extraction, stopword 자동 보정, domain lexicon weighting을 추가할 수 있다. |
| `time_bucket_count` | 시계열 버킷 집계 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 날짜 컬럼을 day/week/month bucket으로 묶고 bucket별 문서 수와 top term을 만든다. | anomaly detection, moving average, change point 탐지, seasonality 비교를 붙일 수 있다. |
| `meta_group_count` | 메타데이터 그룹 집계 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 메타데이터 차원값별 건수와 top term을 집계한다. | significance test, drill-down, 다중 dimension breakdown, low-volume suppression을 추가할 수 있다. |
| `document_sample` | 대표 문서 샘플링 | `workers/python-ai/src/python_ai_worker/skills/support.py` | query overlap ranking 또는 source order로 대표 문서를 뽑는다. | MMR/diversity sampling, cluster-aware sampling, recency weighting, confidence-based sampling을 추가할 수 있다. |
| `dictionary_tagging` | 사전 기반 태깅 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 사전 기반 taxonomy rule과 pattern matching으로 문서에 태그를 붙인다. | weighted rule, regex/operator 확장, synonym dictionary, weak supervision classifier를 추가할 수 있다. |
| `embedding_cluster` | 임베딩 군집화 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 미리 생성된 chunk 단위 sidecar를 읽고, dense vector가 있으면 dense cosine similarity에 token-overlap guardrail을 곱한 `dense-hybrid` 점수로 greedy clustering을 수행한다. dense가 없으면 token vector cosine similarity로 fallback한다. `source_index`를 유지해 row 단위 prior filter/dedup artifact와 연결한다. | dense-only clustering 품질 비교, HDBSCAN/agglomerative clustering, automatic threshold search, cluster centroid 학습을 검토할 수 있다. |
| `cluster_label_candidates` | 군집 라벨 후보 생성 | `workers/python-ai/src/python_ai_worker/skills/support.py` | cluster top term으로 heuristic label 후보를 만든다. | c-TF-IDF, representative document title generation, LLM-assisted label proposal을 붙일 수 있다. |
| `semantic_search` | 의미 검색 | `workers/python-ai/src/python_ai_worker/skills/support.py` | 가능하면 `pgvector`의 `embedding_index_chunks`를 먼저 조회한다. index metadata가 dense model이면 같은 model로 query embedding을 다시 만들고, dense가 아니면 token count를 같은 차원으로 projection해 조회한다. 불가하면 `embeddings.jsonl`의 chunk 단위 `token_counts` cosine similarity로 fallback한다. 결과는 `retrieval_backend`, `chunk_id`, `chunk_index`, `char_start`, `char_end`, `chunk_ref`, `row_id`, `source_index`를 함께 보존한다. | dense semantic retrieval 품질 비교, hybrid search, reranker, claim-aware evidence ranking을 도입할 수 있다. |
| `evidence_pack` | 근거 묶음 생성 | `workers/python-ai/src/python_ai_worker/skills/core.py` | 선택된 snippet을 묶고, 가능하면 LLM으로 요약하며 실패 시 fallback summarizer를 사용한다. 현재 `semantic_search` prior artifact가 있으면 chunk citation 필드를 그대로 보존한다. | citation scoring, redundancy 제거, evidence diversity ranking, claim-to-evidence linking을 추가할 수 있다. |

## Unstructured Core

| Skill | 한국어 이름 | 주요 구현 파일 | 기본 support 조합 | 현재 구현 방식 | 향상 가능 방향 |
| --- | --- | --- | --- | --- | --- |
| `unstructured_issue_summary` | 비정형 이슈 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> keyword_frequency -> document_sample` | top term과 대표 문서 샘플을 묶어 일반 이슈 요약 artifact를 만든다. support skill artifact를 재사용할 수 있다. | issue type classification, anomaly-driven summary, confidence score, sectioned narrative 요약을 추가할 수 있다. |
| `issue_breakdown_summary` | 이슈 분해 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> meta_group_count -> document_sample` | `meta_group_count` 결과를 재사용하거나 직접 group-by 요약을 만든다. | decomposition 기준 추천, multi-level breakdown, driver analysis를 붙일 수 있다. |
| `issue_cluster_summary` | 이슈 군집 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> deduplicate_documents -> embedding_cluster -> cluster_label_candidates` | `embedding_cluster`와 `cluster_label_candidates` 결과를 묶어 주요 군집을 요약한다. 필요하면 fallback cluster 생성도 수행한다. | better cluster ranking, cluster drift tracking, cluster naming quality 평가를 추가할 수 있다. |
| `issue_trend_summary` | 이슈 추세 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> time_bucket_count -> document_sample` | `time_bucket_count` 결과를 재사용하거나 직접 시계열 요약을 만든다. peak bucket과 대표 샘플을 함께 제공한다. | anomaly explanation, baseline 대비 변화율, seasonal comparison, trend significance를 추가할 수 있다. |
| `issue_period_compare` | 기간 비교 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> document_sample` | 현재 기간과 직전 기간의 문서 수, 상위 용어 변화를 비교한다. | 기간 자동 정렬, segmented comparison, driver term attribution, confidence band를 추가할 수 있다. |
| `issue_sentiment_summary` | 이슈 감성 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> document_sample` + dataset build `sentiment_label` | 감성 라벨 분포와 대표 예시를 집계한다. 현재는 `sentiment.parquet`의 `row_id`, `source_row_index`를 기준으로 `prepared.parquet`를 join해 텍스트 샘플을 복원한다. | aspect별 감성, 채널/기간별 감성 비교, 감성 강도 분포, prepared join 공통화, sample ranking 고도화를 추가할 수 있다. |
| `issue_taxonomy_summary` | 이슈 분류체계 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | `document_filter -> dictionary_tagging` | dictionary tagging 결과를 재사용해 dominant taxonomy와 category breakdown을 만든다. | taxonomy hierarchy, multi-label rollup, unknown bucket 분석, tagging coverage 지표를 추가할 수 있다. |
| `issue_evidence_summary` | 이슈 근거 요약 | `workers/python-ai/src/python_ai_worker/skills/core.py` | 우선 `semantic_search`, 없으면 `document_sample`, 둘 다 없으면 lexical overlap fallback | 선택된 문서를 기반으로 최종 사용자용 근거 요약, key finding, follow-up question을 만든다. 가능하면 LLM을 쓰고 실패 시 fallback summary를 사용한다. trend/breakdown/compare/cluster/taxonomy/sentiment 계열 prior artifact는 `analysis_context`로 함께 반영하고, `semantic_search`가 있으면 chunk citation 필드를 evidence artifact까지 유지한다. | claim/evidence alignment, citation granularity, confidence score, contradiction detection을 추가할 수 있다. |

## Planner

| Skill | 한국어 이름 | 주요 구현 파일 | 현재 구현 방식 | 향상 가능 방향 |
| --- | --- | --- | --- | --- |
| `planner` | 분석 계획 생성기 | `workers/python-ai/src/python_ai_worker/planner.py`, `config/skill_bundle.json` | 기본은 goal keyword 기반 rule-based intent routing이며, 설정 시 Anthropic structured JSON planner를 우선 사용한다. skill bundle 메타데이터를 참조해 step을 구성한다. | dependency-aware planning, cost-aware planning, validation loop, planner evaluation set, user intent memory를 추가할 수 있다. |

## 현재 해석 포인트

- 현재 비정형 skill의 핵심 deterministic 기술은 `토큰화`, `사전 매칭`, `빈도 분석`, `bucket 집계`, `bag-of-words cosine similarity`, `greedy clustering`이다.
- `semantic_search`는 이제 dense index metadata가 있으면 query embedding도 dense model로 맞춘다. `embedding_cluster`는 dense vector가 있으면 `dense-hybrid` 유사도를 우선 사용하고, 없으면 token vector 경로로 fallback한다.
- 현재 `pgvector` 적재는 dense model 출력 또는 token count projection fallback의 혼합 단계다.
- `dense-hybrid`는 dense-only collapse를 막기 위한 guardrail로는 유효하지만, 범용 표현이 많이 겹치는 dataset에서는 여전히 1개 군집으로 뭉칠 수 있어 threshold와 fixture 검증을 계속 유지해야 한다.
- LLM은 planner, evidence summary, dataset prepare, sentiment labeling에서 선택적으로 사용되고, 실패 시 deterministic fallback으로 내려간다.
- 확인 필요: OpenAI key를 넣은 dense embedding end-to-end smoke와 local/OpenAI retrieval 품질 비교는 아직 별도 검증이 더 필요하다.
- 확인 필요: 실제 운영 단계에서 clustering, dedup, retrieval 중 어떤 부분을 Rust hot path 또는 별도 inference path로 옮길지는 성능 측정 결과와 분석팀 품질 기준을 함께 보고 결정해야 한다.
