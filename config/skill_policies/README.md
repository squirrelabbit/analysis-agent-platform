# Skill Policies

`config/skill_policies/`는 python-ai worker의 운영 정책값을 버전 파일로 관리하는 디렉터리다.

현재 대상:
- `embedding_cluster`
- `cluster_label_candidates`
- `issue_evidence_summary`

규칙:
- 파일명은 `<version>.json`
- 실행 artifact에는 `policy_version`, `policy_hash`를 남긴다.
- 자주 바뀌는 threshold, ranking, fallback만 여기 두고, 알고리즘 커널은 코드에 남긴다.

`확인 필요:` policy 변경 승인 절차와 rollout 기준은 별도 운영 규칙으로 더 정리할 수 있다.
