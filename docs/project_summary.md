# 프로젝트 요약

## 한 줄 정의

- 확인 필요: 저장소 루트에서 `project_context.yaml`은 확인되지 않았다.
- 이 프로젝트는 질문이나 strict 시나리오를 재실행 가능한 `Skill Plan`으로 고정하고, 실행 결과를 `result_v1`과 `final_answer`로 남기는 분석 실행 플랫폼이다.

## 핵심 흐름

1. 프로젝트와 dataset, dataset version을 등록한다.
2. unstructured dataset version이면 `prepare` build job을 먼저 enqueue한다.
3. 질문 또는 strict 시나리오를 plan으로 바꾼다.
4. execution 시작 전에 필요한 `sentiment / embedding / cluster` dependency를 자동 build한다.
5. Temporal workflow가 execution을 진행하면서 `STEP_*` event와 partial artifact를 execution에 저장한다.
6. 완료 시 `result_v1 snapshot`과 `final_answer`를 만들고 report draft 같은 후속 문서에 재사용한다.

## 주요 구성 요소

- `Go control plane`
  - dataset, execution, scenario, report draft API
  - dataset build orchestration과 startup reconciliation
  - execution progress / event / step preview surface
- `Temporal runtime`
  - analysis workflow
  - dataset build workflow
- `Python AI worker`
  - planner
  - prepare / sentiment / embedding / cluster build
  - `preprocess / aggregate / retrieve / summarize / presentation` skill
  - `final_answer` 후처리
- `Postgres + artifact storage`
  - execution metadata, build job 상태, snapshot 저장
  - Parquet/JSON artifact 저장
- `DuckDB`
  - 현재 structured skill 실행 경로

## 저장과 실행 구조

- dataset version은 생성 시 resolved profile을 저장한다.
- global prompt registry는 `config/prompts/*.md`, 기본 dataset profile은 `config/dataset_profiles.json`에서 관리한다.
- project prompt version은 `GET/POST /projects/{project_id}/prompts`로 관리하고, project 기본 prompt 선택은 `GET/PUT /projects/{project_id}/prompt_defaults`로 관리한다.
- dataset version이 prompt version을 직접 지정하면 그 값을 우선 사용하고, 지정하지 않으면 project `prompt_defaults`를 fallback으로 사용한다.
- 동일한 prompt version이 project registry와 global registry에 모두 있으면 project prompt가 우선한다.
- project prompt에 batch template가 없으면 row template만 사용하도록 build payload의 batch size를 `1`로 낮춘다.
- 운영/프론트는 `GET /dataset_profiles`, `GET /prompt_catalog`, `GET /rule_catalog`, `GET /skill_policy_catalog`, `GET /dataset_profiles/validate`, `GET /skill_policies/validate`로 현재 registry와 catalog 상태를 조회할 수 있다.
- `prepare`는 eager, `sentiment / embedding / cluster`는 lazy build를 기본 정책으로 둔다.
- full-dataset `embedding_cluster`는 precomputed cluster artifact를 우선 읽고, subset 경로만 on-demand fallback을 허용한다.
- cluster artifact와 step preview에는 `cluster_execution_mode`, `cluster_materialization_scope`, `cluster_fallback_reason`가 포함돼 materialized/full-dataset 경로와 subset fallback 경로를 구분할 수 있다.
- cluster 산출물은 현재 `summary JSON + membership parquet`로 분리 저장한다.
- `embedding_cluster`, `cluster_label_candidates`, `issue_evidence_summary`는 `config/skill_policies/*.json` 기반 versioned policy를 읽는다.
- execution 완료 후에는 `result_v1 snapshot`과 `final_answer snapshot`을 함께 남긴다.
- execution 조회는 현재 `events`, `progress`, `step preview` 경로를 통해 중간 진행 상태를 노출한다.
- step 완료 시 execution row에는 raw artifact 전체 대신 `summary / preview / artifact_ref` 중심 compact payload를 저장한다.

## 현재 범위

- scenario planning mode는 현재 `strict`만 지원한다.
- dataset build와 execution은 startup reconciliation으로 재기동 후 다시 평가한다.
- 현재 dev compose는 `TEMPORAL_PERSISTENCE_MODE=dev_ephemeral`, `TEMPORAL_RETENTION_MODE=temporal_dev_default`, `TEMPORAL_RECOVERY_MODE=startup_reconciliation` 기준으로 동작한다.
- `GET /runtime_status`로 현재 런타임 보장 범위를 조회할 수 있다.
- 프론트는 `apps/web`에 Vite + React + TypeScript 기반 scaffold가 있고, 실행 중간 상태를 붙일 수 있는 backend API는 준비된 상태다.
- 확인 필요: Rust worker는 현재 hot path runtime에 연결되지 않았다.
