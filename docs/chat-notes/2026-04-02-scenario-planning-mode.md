# 시나리오 planning mode 결정

## 배경

- 시나리오를 `analysis_request + plan`으로 바로 변환하는 기능을 추가하면서, 대표 질문을 완전 고정 step으로 실행할지, LLM planner가 일부를 조정할 수 있게 할지 결정이 필요했다.
- 현재 control plane에는 `POST /projects/{project_id}/scenarios/{scenario_id}/plans`가 추가돼 저장된 시나리오 step을 runtime skill plan으로 바꾸는 경로가 생겼다.

## 결정 사항

- 현재 시나리오 planning mode는 `strict`로 고정한다.
- `strict`에서는 저장된 step 순서를 그대로 plan으로 변환한다.
- 자동 변환은 `runtime_skill_name`이 명시된 step 또는 control plane에 등록된 `function_name -> skill_name` 매핑만 허용한다.
- 직접 매핑되지 않는 step은 에러로 돌리고, 시나리오 등록 시 `runtime_skill_name`을 명시하도록 한다.
- `guided`나 guardrail 기반 planner는 이번 작업 범위에 넣지 않고 backlog로 남긴다.

## 확인한 근거

- 관련 파일:
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/domain/models.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/scenarios.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/store/postgres.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/http/server.go`
  - `/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/project_summary.md`
- 관련 명령:
  - `cd apps/control-plane && go test ./...`
  - dev stack에서 `POST /projects/{project_id}/scenarios/{scenario_id}/plans` 호출 확인

## 남은 이슈

- backlog:
  - `guided` planning mode 추가
  - 시나리오별 `mandatory_steps`, `optional_steps`, `allowed_skills`, `forbidden_skills` 같은 guardrail 스키마 설계
  - `scenario -> execute` one-shot endpoint 추가
  - 시나리오 step의 기획용 기능명과 runtime skill 매핑 테이블 확장
- 확인 필요:
  - guardrail을 planner prompt에만 둘지, control plane에서 사후 검증까지 할지는 별도 설계가 필요하다.
