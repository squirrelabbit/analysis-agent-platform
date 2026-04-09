# Skill Policies

`config/skill_policies/`는 python-ai worker의 운영 정책값을 버전 파일로 관리하는 디렉터리다.

목적:
- 자주 바뀌는 threshold, ranking, fallback 정책을 코드와 분리한다.
- 실행 artifact에 `policy_version`, `policy_hash`를 남겨 재현성을 유지한다.
- policy 파일만 교체하거나 추가해서 운영 중 정책 실험을 할 수 있게 한다.

현재 대상:
- `embedding_cluster`
- `cluster_label_candidates`
- `issue_evidence_summary`

정책 파일 규칙:
- 파일명은 `<version>.json`
- 최상위 필드:
  - `version`
  - `skill_name`
  - `status`
  - `summary`
  - `policy`

`확인 필요:`
- policy 변경 승인 절차와 rollout 기준은 운영 규칙으로 따로 더 정리할 수 있다.
