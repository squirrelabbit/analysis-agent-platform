# 프로젝트 요약

## 한 줄 정의

- 확인 필요: 저장소 루트에서 `project_context.yaml`은 확인되지 않았다.
- 이 프로젝트는 질문이나 strict 시나리오를 재실행 가능한 `Skill Plan`으로 고정하고, 실행 결과를 `result_v1`과 `final_answer`로 남기는 분석 실행 플랫폼이다.

## 핵심 흐름

1. 프로젝트와 dataset, dataset version을 등록한다.
2. unstructured dataset version이면 `prepare` build job을 먼저 enqueue한다.
3. 질문 또는 strict 시나리오를 plan으로 바꾼다.
4. execution 시작 전에 필요한 `sentiment / embedding / cluster` dependency를 자동 build한다.
5. Temporal workflow가 execution을 진행하고, 완료 시 `result_v1 snapshot`을 저장한다.
6. 같은 실행 결과를 바탕으로 `final_answer`를 생성하고 report draft 같은 후속 문서에 재사용한다.

## 주요 구성 요소

- `Go control plane`
  - dataset, execution, scenario, report draft API
  - dataset build orchestration과 startup reconciliation
- `Temporal runtime`
  - analysis workflow
  - dataset build workflow
- `Python AI worker`
  - planner
  - prepare / sentiment / embedding / cluster build
  - support/core skill
  - `final_answer` 후처리
- `Postgres + artifact storage`
  - execution metadata, build job 상태, snapshot 저장
  - Parquet/JSON artifact 저장
- `DuckDB`
  - 현재 structured skill 실행 경로

## 저장과 실행 구조

- dataset version은 생성 시 resolved profile을 저장한다.
- prompt template는 `config/prompts/*.md`, 기본 dataset profile은 `config/dataset_profiles.json`에서 관리한다.
- `prepare`는 eager, `sentiment / embedding / cluster`는 lazy build를 기본 정책으로 둔다.
- full-dataset `embedding_cluster`는 precomputed cluster artifact를 우선 읽고, subset 경로만 on-demand fallback을 허용한다.
- execution 완료 후에는 `result_v1 snapshot`과 `final_answer snapshot`을 함께 남긴다.

## 현재 범위

- scenario planning mode는 현재 `strict`만 지원한다.
- dataset build와 execution은 startup reconciliation으로 재기동 후 다시 평가한다.
- 프론트는 `apps/web`에 Vite + React + TypeScript scaffold만 준비된 상태다.
- 확인 필요: Rust worker는 현재 hot path runtime에 연결되지 않았다.
- 확인 필요: Temporal workflow history 장기 보존은 아직 dev server 기본값을 따른다.
