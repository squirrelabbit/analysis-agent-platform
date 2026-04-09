# 2026-04-09 skill runtime refactor

## 오늘의 질문

- `python-ai` 스킬 구현이 `core.py`, `support.py`에 뭉쳐 있는 상태로 계속 유지돼도 되는가?
- 역할 기준으로 `preprocess / aggregate / retrieve / summarize / presentation`로 나눴다면, 실제 구현 본문도 그 기준으로 정리돼야 하지 않는가?
- 운영 정책은 밖으로 빼더라도, 코드 구조 자체가 애매하면 이후 스킬 품질 개선과 유지보수가 계속 느려지지 않는가?

## 왜 이게 문제인가

- 기존 구조에서는 스킬 이름과 실제 구현 위치가 잘 맞지 않았다.
- `support.py`, `core.py`는 이미 public 개념으로는 폐기된 이름인데, 실제 구현 본문이 여전히 그 내부 또는 `_legacy_*` 파일에 남아 있었다.
- 이 상태에서는 새로 들어온 사람이 `retrieve.py`나 `summarize.py`를 열어도 결국 legacy 파일을 다시 추적해야 했다.
- `cluster_label_candidates`, `embedding_cluster`, `issue_evidence_summary`처럼 지금 우선 개선해야 할 스킬도 구현 위치가 흐리면 수정 비용이 커진다.

## 시도한 것

- 먼저 public entrypoint를 역할 기준으로 고정했다.
  - [workers/python-ai/src/python_ai_worker/skills/preprocess.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/preprocess.py)
  - [workers/python-ai/src/python_ai_worker/skills/aggregate.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/aggregate.py)
  - [workers/python-ai/src/python_ai_worker/skills/retrieve.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/retrieve.py)
  - [workers/python-ai/src/python_ai_worker/skills/summarize.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/summarize.py)
  - [workers/python-ai/src/python_ai_worker/skills/presentation.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/presentation.py)
- 그 다음 실제 구현 본문을 역할별 private module로 분리했다.
  - [workers/python-ai/src/python_ai_worker/skills/_preprocess_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_preprocess_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_aggregate_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_aggregate_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_retrieve_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_retrieve_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_summarize_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_summarize_impl.py)
- `task_router`와 public import surface는 이미 새 모듈 기준으로 쓰고 있었으므로, legacy 이름을 실제로 걷어낼 수 있는지 확인했다.
  - [workers/python-ai/src/python_ai_worker/task_router.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/task_router.py)
  - [workers/python-ai/src/python_ai_worker/tasks.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/tasks.py)
- repo 내부 검색 기준으로 `skills.core`, `skills.support` import 사용처가 없는 것을 확인한 뒤, 실제 호환 shim까지 삭제했다.
  - 삭제:
    - [workers/python-ai/src/python_ai_worker/skills/core.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/core.py)
    - [workers/python-ai/src/python_ai_worker/skills/support.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/support.py)
- 문서도 현재 구조 기준으로 맞췄다.
  - [workers/python-ai/README.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/README.md)
  - [docs/skill/skill_implementation_status.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/skill/skill_implementation_status.md)

## 관찰한 점

- 1차 리팩터링만으로는 `역할 기준 public module`은 생겼지만, 실제 구현이 `_legacy_*` 파일에 남아 있어서 구조 정리가 덜 끝난 상태였다.
- `retrieve.py`, `summarize.py`는 policy binding과 helper rebinding 때문에 public surface 역할이 더 중요했고, 실제 구현 본문을 private impl로 따로 두는 편이 읽기 쉬웠다.
- `_legacy_support_impl.py`는 1200줄이 넘는 큰 파일이라 완전한 함수 단위 재작성보다는 역할별 private impl 파일로 안전하게 분리하는 편이 더 현실적이었다.
- 실제 import 사용처를 확인해보니 `core.py`, `support.py`는 현재 repo 런타임에서는 필요 없었다.
- Python 테스트에서 `Counter` import 하나가 빠진 문제가 바로 드러났고, 이 정도 수준의 누락은 전체 unittest를 돌리면서 빠르게 잡을 수 있었다.

## 현재 판단

- 지금 기준 public skill 구조는 아래로 보는 게 맞다.
  - preprocess
  - aggregate
  - retrieve
  - summarize
  - presentation
- 실제 구현은 public module이 아니라 private `*_impl.py`에 두고, public module은 policy binding이나 helper patch를 포함한 안정된 import surface로 유지하는 쪽이 낫다.
- `core.py`, `support.py`, `_legacy_core_impl.py`, `_legacy_support_impl.py`는 이제 현재 구조 설명에 오히려 방해가 되는 이름이었다.
- 즉 이번 정리로 `스킬 계약 이름`, `공개 진입점`, `실제 구현 파일`의 경계가 이전보다 훨씬 분명해졌다.

## 남은 리스크

- 확인 필요: 외부 개인 스크립트나 로컬 노트북에서 예전 `python_ai_worker.skills.core` 또는 `support` import를 쓰고 있었다면 이번 정리 이후 깨질 수 있다.
- 확인 필요: `retrieve.py`의 helper rebinding 구조는 테스트 친화적이지만, 장기적으로는 일부 helper를 더 작은 내부 모듈로 분리할 여지가 있다.
- 확인 필요: `cluster_label_candidates`, `embedding_cluster`, `issue_evidence_summary` 자체의 품질 문제는 구조 정리와 별개로 계속 봐야 한다.

## 다음 액션

- representative dataset 기준으로 `cluster_label_candidates`, `issue_evidence_summary`, `embedding_cluster` 품질을 다시 본다.
- policy layer가 붙은 스킬 3종에 대해 profile 또는 scenario 레벨에서 version을 어떻게 지정할지 정리한다.
- 필요하면 `retrieve`와 `summarize` 내부 helper도 더 작은 단위로 쪼개서 테스트 범위를 줄인다.

## 참고 파일 / 명령

- 파일:
  - [workers/python-ai/src/python_ai_worker/skills/preprocess.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/preprocess.py)
  - [workers/python-ai/src/python_ai_worker/skills/aggregate.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/aggregate.py)
  - [workers/python-ai/src/python_ai_worker/skills/retrieve.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/retrieve.py)
  - [workers/python-ai/src/python_ai_worker/skills/summarize.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/summarize.py)
  - [workers/python-ai/src/python_ai_worker/skills/_preprocess_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_preprocess_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_aggregate_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_aggregate_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_retrieve_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_retrieve_impl.py)
  - [workers/python-ai/src/python_ai_worker/skills/_summarize_impl.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/skills/_summarize_impl.py)
- 명령:
  - `rg -n "python_ai_worker\\.skills\\.(core|support)|from \\.core|from \\.support" workers/python-ai apps -g '!apps/web/node_modules/**'`
  - `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
  - `cd apps/control-plane && go test ./...`
