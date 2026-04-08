# dataset build eager/lazy 정책 결정

## 배경

- 비정형 dataset는 현재 `prepare`, `sentiment`, `embedding`을 별도 build 단계로 가진다.
- 시나리오 one-shot 실행은 이미 가능하지만, `prepare`나 `sentiment`가 없으면 execution이 `waiting`으로 멈추고 사용자가 직접 build와 `resume`을 호출해야 했다.
- dataset version에는 현재 `profile v1`을 붙일 수 있고, `prepare_prompt_version`, `sentiment_prompt_version`, `regex_rule_names`, `garbage_rule_names`, `embedding_model` 같은 데이터별 기본 recipe를 저장할 수 있다.

## 결정 사항

- 기본 정책은 `prepare=eager`, `sentiment=lazy`, `embedding=lazy`로 간다.
- 업로드 직후에는 `prepare`만 자동 실행 대상으로 본다.
- `sentiment`와 `embedding`은 실행 step이 실제로 필요로 할 때 dependency를 계산해서 자동 실행한다.
- `waiting`은 정상 기본 경로가 아니라, 자동 orchestration으로 흡수하지 못한 예외 상황으로만 남기는 방향으로 간다.
- dataset version `profile`은 위 build 경로의 기본 prompt/rule/model recipe를 제공하고, 실제 execution에는 `profile_snapshot`을 남겨 재현 가능성을 유지한다.

## 왜 이렇게 정했는가

- `prepare`는 거의 모든 비정형 분석 step의 전제라서 업로드 시점에 미리 만들어 두는 편이 사용자 경험상 가장 단순하다.
- `sentiment`와 `embedding`은 상대적으로 비용과 시간이 더 크고, 실제 사용하지 않는 dataset까지 미리 만들면 낭비가 커진다.
- `sentiment`, `embedding`은 데이터별 prompt/model 변경 가능성이 더 높아서, 필요할 때 만드는 `lazy` 정책이 운영상 덜 꼬인다.
- 이미 dataset version `profile`과 execution `profile_snapshot` 구조가 들어갔기 때문에, build 시점이 늦어져도 어떤 설정으로 돌았는지 추적할 수 있다.

## 확인한 근거

- 관련 파일:
  - [/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/datasets.go](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/datasets.go)
  - [/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/analysis.go](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/service/analysis.go)
  - [/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/workflows/analysis_runtime.go](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/internal/workflows/analysis_runtime.go)
  - [/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/roadmap.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/roadmap.md)
- 관련 관찰:
  - 현재 `scenario execute`는 선행 artifact가 없으면 `waiting_for=dataset_prepare`, `waiting_for=sentiment_label` 같은 상태로 멈춘다.
  - `prepare`는 대부분의 비정형 skill이 사실상 기대하는 공통 전처리다.
  - `sentiment`, `embedding`은 사용 시나리오에 따라 필요 여부가 갈린다.

## 남은 이슈

- 다음 구현:
  - upload 직후 `prepare` 자동 실행
  - execution 시작 전 dependency 계산
  - `sentiment`, `embedding` lazy auto-build
  - build 완료 후 execution 자동 재개
- backlog:
  - 특정 dataset를 warm-up 대상으로 지정해 `sentiment`, `embedding`까지 미리 생성하는 정책
- 확인 필요:
  - warm-up 대상 선정을 dataset version flag로 둘지, profile 정책으로 둘지는 아직 정하지 않았다.
