# Skill Registry

Skill Registry는 planner, workflow, control plane, worker가 함께 공유하는 공식 skill 계약이다.
실제 runtime source는 `config/skill_bundle.json`이며, 이 문서는 그 bundle의 제품 계약만 짧게 요약한다.

구현 방식과 안정도는 [skill_implementation_status.md](skill_implementation_status.md), 운영 정책 버전은 `config/skill_policies/`와 `GET /skill_policy_catalog`를 본다.

## 계약 원칙

- planner, workflow, worker는 registry 밖의 skill 이름을 쓰지 않는다.
- skill 이름과 artifact contract는 구현 언어가 바뀌어도 유지한다.
- LLM이 개입해도 fallback과 metadata를 남겨 replay 가능성을 유지한다.

## 실행 경계

| 엔진 | 역할 |
| --- | --- |
| `DuckDB` | `structured_kpi_summary` |
| `python-ai` | 비정형 `preprocess / aggregate / retrieve / summarize / presentation` skill |
| `Go control plane` | skill registry 노출, plan normalize, readiness 판정, worker routing |
| `Rust worker` | 확인 필요: 현재 hot path runtime에는 연결되지 않았다 |

## 현재 plan skill

| 그룹 | Skill |
| --- | --- |
| structured | `structured_kpi_summary` |
| preprocess | `garbage_filter`, `document_filter`, `deduplicate_documents`, `sentence_split`, `document_sample` |
| aggregate | `keyword_frequency`, `noun_frequency`, `time_bucket_count`, `meta_group_count`, `dictionary_tagging` |
| retrieve | `embedding_cluster`, `cluster_label_candidates`, `semantic_search` |
| summarize | `evidence_pack`, `unstructured_issue_summary`, `issue_breakdown_summary`, `issue_cluster_summary`, `issue_trend_summary`, `issue_period_compare`, `issue_sentiment_summary`, `issue_taxonomy_summary`, `issue_evidence_summary` |

## dataset build task

아래 항목은 planner가 plan에 직접 넣는 skill이 아니라 dataset version 준비용 build task다.
현재 bundle에서는 `kind=dataset_build`, `plan_enabled=false`로 구분한다.

- `dataset_prepare`
- `sentiment_label`
- `embedding`

cluster materialization은 `POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/cluster_jobs`와 worker build 단계로 존재하지만, 현재 `config/skill_bundle.json`의 dataset build skill로는 등록되어 있지 않다.
plan에서는 `embedding_cluster`가 materialized cluster artifact를 우선 사용한다.

## 운영 메모

- `/skills`, `/capabilities`, plan normalize, readiness 판정은 모두 bundle 기준으로 동작한다.
- broad cluster 질문은 materialized cluster 경로를 우선 타도록 정리돼 있다.
- replay 기준에는 `skill_bundle_version`과 policy metadata가 함께 남는다.
