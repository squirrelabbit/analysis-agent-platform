# 협업 기준선 정리

## 배경

- 현재 저장소는 strict 시나리오 등록/일괄 등록, 시나리오 기반 plan 생성, one-shot execution, `result_v1 snapshot`, report draft, local embedding 기반 비정형 분석 흐름까지 연결돼 있다.
- 반면 데이터별 전처리 프롬프트, 감성 프롬프트, rule/profile 운영 방식은 아직 분석팀과 정책 합의가 끝나지 않았다.
- 이 상태에서 다른 개발자와 바로 병렬 구현을 시작하면, 엔진 코어와 사용자 플로우 API의 경계가 불명확해져 재작업 가능성이 높다고 판단했다.

## 결정 사항

- 협업 시작 전 기준선은 `공유 가능한 현재 상태`와 `아직 확정하지 않은 정책 영역`을 분리하는 방식으로 잡는다.
- 당분간 시나리오 실행은 `planning_mode=strict`를 전제로 유지하고, 시나리오는 고정 step 조합을 실행하는 템플릿으로 본다.
- 다른 개발자와의 분업은 `사용자 플로우 API`와 `분석 엔진 코어`를 분리하는 방식으로 잡는다.
  - 사용자 플로우 API:
    - 시나리오 등록/조회/import
    - 시나리오 기반 plan/execute
    - execution list/result/report draft
  - 분석 엔진 코어:
    - prepare/sentiment/embedding
    - prompt/rule/profile
    - support/core skill 내부 로직
- 현재 기준에서 다른 개발자에게 바로 열어도 되는 영역은 `시나리오/결과 API`와 관련 문서/fixture이다.
- 현재 기준에서 한 사람이 계속 책임지고 정리해야 하는 영역은 `데이터별 prompt/profile 정책`, `rule snapshotting`, `자동 orchestration`, `artifact 계약`이다.

## 확인한 근거

- 관련 파일:
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/scenarios.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/analysis.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/workflows/analysis_runtime.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/dataset_build.py`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/runtime/rule_config.py`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/project_summary.md`
- 관련 명령:
  - `cd apps/control-plane && go test ./...`
  - `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
  - 시나리오 등록/import/plan/execute 및 execution result API 수동 확인

## 남은 이슈

- 분석팀 논의 필요:
  - 데이터별 전처리 프롬프트 분리 여부
  - 데이터별 감성 프롬프트 분리 여부
  - dataset/scenario별 profile 도입 여부
- backlog:
  - `upload -> prepare -> sentiment -> embedding` 자동 orchestration
  - profile snapshot 저장 구조
  - guided scenario planner / guardrail 설계
  - support skill 대용량 intermediate artifact 정책 정교화
- 확인 필요:
  - 협업자에게 공유할 최소 문서 세트가 `manual.md`, `docs/project_summary.md`, `docs/skill/scenario_templates.md`만으로 충분한지는 실제 온보딩 시점에 다시 확인이 필요하다.
