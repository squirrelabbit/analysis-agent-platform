# 프로젝트 요약

## 1. 한 줄 정의

- 확인 필요: 저장소 루트에서 `project_context.yaml`은 확인되지 않았다.
- 이 프로젝트는 질문을 재실행 가능한 `Skill Plan`으로 고정하고, 실행 결과를 `result / rerun / diff` 단위로 남기는 분석 실행 플랫폼이다.

## 2. 핵심 흐름

- 프로젝트와 dataset, dataset version을 등록한다.
- 원본 dataset을 upload한 뒤 필요하면 `prepare`, `sentiment`, `embedding` 산출물을 만든다.
- 분석 요청을 제출하면 planner가 최소 skill plan을 만들고, Temporal workflow가 실행과 `waiting / resume`를 오케스트레이션한다.
- 실행 결과는 artifact와 execution metadata로 남고, 같은 execution context 기준으로 `rerun / diff` 할 수 있다.

## 3. 현재 런타임 경계

- `Go control plane`
  - 프로젝트, dataset, analysis request, execution API
- `Temporal runtime`
  - execution lifecycle와 `waiting / resume`
- `DuckDB`
  - 현재 연결된 structured skill은 `structured_kpi_summary` 1종
- `Python AI worker`
  - planner, task router, runtime helper, dataset build task, 비정형 support/core skill
- `Rust worker`
  - 확인 필요: 저장소에는 스캐폴드가 있으나 현재 실행 hot path에는 연결되지 않았다.

## 4. 현재 상태

- dataset build task `dataset_prepare`, `sentiment_label`, `embedding`이 연결돼 있다.
- 비정형 support/core skill은 taxonomy, dedup, clustering 계열까지 현재 실행 경로에 포함된다.
- `dataset_prepare`는 Anthropic prepare가 켜지면 batch 정제를 사용하고, `issue_evidence_summary`는 prior artifact를 `analysis_context`로 재사용한다.
- plan skill 메타데이터는 공용 `skill bundle`인 `config/skill_bundle.json`으로 중앙화됐다.
- Python worker 내부는 `task_router`, `planner`, `runtime`, `skills/support`, `skills/core` 중심으로 분리됐다.
- 상세 skill 목록과 계약은 `docs/skill/skill_registry.md`를 기준으로 본다.
- skill별 분석 기법은 `docs/skill/analysis_techniques.md`에 정리돼 있다.
- GitHub Actions CI는 Python worker 테스트와 Go 테스트/빌드를 현재 구조 기준으로 실행한다.

## 5. 문서 구분

- `docs/project_summary.md`
  - 현재 제품 정의와 실행 흐름의 짧은 스냅샷
- `docs/devlog/`
  - 매일의 고민, 챌린지, 실험 메모, 다음 액션 기록
- `docs/chat-notes/`
  - 확정된 결정 로그와 Codex 대화 보관본

## 6. 확인 필요

- 이번 문서 갱신 turn에서는 Python worker 재빌드 후 smoke 8종을 다시 실행했다.
- Rust worker를 실제 hot path로 넘길 성능 기준과 시점은 별도 측정이 필요하다.
