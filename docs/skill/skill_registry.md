# Skill Registry

## 목적

Skill Registry는 planner, workflow, control plane, worker가 함께 공유하는 공식 skill 계약 목록이다.

- planner는 registry 밖의 plan skill을 제안하면 안 된다.
- workflow는 registry 밖의 plan skill을 실행하면 안 된다.
- worker는 registry에 없는 plan skill 이름을 실행하면 안 된다.

즉, registry는 언어별 구현 파일 목록이 아니라 **제품이 공식 지원하는 분석 동작 목록**이다.

현재 runtime source는 저장소 루트의 `config/skill_bundle.json`이며, 이 문서는 그 bundle이 표현하는 제품 계약을 설명하는 문서다.

구현 상태와 안정도는 별도 문서 [skill_implementation_status.md](skill_implementation_status.md) 를 본다.
운영 정책 버전은 `config/skill_policies/` 와 `GET /skill_policy_catalog` 기준으로 본다.

## 계약 원칙

- skill 이름은 고정 문자열이어야 한다.
- 같은 입력과 같은 데이터에서는 같은 결과를 내는 결정론적 실행을 우선한다.
- 구현 언어가 달라도 contract 이름과 artifact 형태는 유지한다.
- LLM이 개입하는 경우에도 fallback과 metadata를 남겨 재현성을 최대한 유지한다.

## 현재 구현 경계

- `Go`
  - skill registry 노출
  - execution routing
  - dataset build / analysis request / execution API
- `DuckDB`
  - 현재 `structured_kpi_summary` 1종 실행
- `Python worker`
  - planner
  - dataset build task
  - 비정형 `preprocess / aggregate / retrieve / summarize / presentation` skill 실행
- `Rust worker`
  - 확인 필요: 저장소에는 스캐폴드만 있고 현재 runtime 경로에는 연결되지 않았다.

## 현재 runtime source

- `config/skill_bundle.json`
  - skill 이름
  - 실행 엔진
  - task path
  - 기본 입력값
  - dataset source
  - readiness 요구사항
  - planner sequence
  - bundle version
- `Go control plane`
  - `/skills`, 기본 plan, plan normalize, readiness 판정을 bundle 기준으로 읽는다.
- `Python worker`
  - `/capabilities`, rule-based planner, LLM planner 허용 skill, task capability 노출을 bundle 기준으로 읽는다.

## 현재 plan 실행 skill

Structured:
- `structured_kpi_summary`

Unstructured preprocess:
- `garbage_filter`
- `document_filter`
- `deduplicate_documents`
- `sentence_split`
- `document_sample`

Unstructured aggregate:
- `keyword_frequency`
- `noun_frequency`
- `time_bucket_count`
- `meta_group_count`
- `dictionary_tagging`

Unstructured retrieve:
- `embedding_cluster`
- `cluster_label_candidates`
- `semantic_search`

Unstructured summarize:
- `evidence_pack`
- `unstructured_issue_summary`
- `issue_breakdown_summary`
- `issue_cluster_summary`
- `issue_trend_summary`
- `issue_period_compare`
- `issue_sentiment_summary`
- `issue_taxonomy_summary`
- `issue_evidence_summary`

## Dataset Build Task

아래 항목은 현재 planner가 직접 plan에 넣는 skill이라기보다 dataset version 준비를 위한 build task다.

- `dataset_prepare`
- `sentiment_label`
- `embedding`

즉, `/skills`가 반환하는 공식 plan skill 목록과 dataset build API가 직접 호출하는 worker task는 구분해서 본다.
현재 bundle에서도 build task는 `kind=dataset_build`, `plan_enabled=false`로 구분한다.

## 전처리 확장 메모

- `noun_frequency`
  - `keyword_frequency` 옆에 두는 한국어 명사 중심 support skill이다.
  - 가능하면 Kiwi 형태소 분석기를 쓰고, 없으면 regex token fallback으로 내려간다.
  - `top_nouns`에 `term_frequency`, `document_frequency`를 함께 남긴다.
- `sentence_split`
  - 문장 단위 citation과 sentence-level downstream 처리를 위한 support skill이다.
  - 가능하면 `kss`를 쓰고, 없으면 regex fallback으로 내려간다.
  - 실행 안에서는 문장 row를 `rows.parquet` sidecar로 저장할 수 있다.

## 아직 TODO로 남겨둔 범위

운영 기능:
- `auth / approval / 권한 관리`
- `artifact` 외부 저장소 분리

Structured skill 확장:
- `period_compare_summary`
- `dimension_breakdown_summary`
- `topn_rank_summary`
- `dataset_profile`
- `aggregate`
- `rank`
- `timeseries_peak`
- `compare_period`

Observability:
- metrics
- tracing
- retry / timeout 정책

## 제품 관점 메모

재현성은 질문 문자열이 아니라 `plan + dataset_version + execution metadata + artifact` 조합으로 성립한다.

따라서 registry는 단순 목록이 아니라 replay contract의 일부다.
- 현재 execution metadata에는 `skill_bundle_version`을 함께 남겨 replay 기준으로 사용한다.
- 확인 필요: `Rust` hot path 전환은 현재 TODO 목록이 아니라, 추후 성능 측정에 따라 별도 판단할 최적화 후보로 본다.
