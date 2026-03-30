# TF 문서 운영 방식 정리

## 배경

- 내부 TF는 PM과 전담 기획자가 없고, 분석 담당과 개발 담당 중심으로 운영된다.
- 개발 담당은 1명일 가능성이 높고, 분석 담당은 GitHub를 사용하지 않을 수 있다.
- 기존 repo 문서는 기술 참조와 운영 문서가 섞여 있어, 팀이 어디를 기준으로 상태를 관리해야 하는지 헷갈릴 수 있었다.

## 이번에 정한 내용

1. 팀 운영의 마스터는 Notion으로 둔다.
2. repo 문서는 확정된 요구사항, 기술설계, 운영 기준, 공수 기준안 중심으로 유지한다.
3. 실시간 WBS, 회의록, 이슈/리스크는 Notion에서 관리한다.
4. `docs/project/wbs.md`는 Notion 업무보드에 옮기기 위한 기준안 또는 스냅샷으로 본다.
5. 운영용 결정/이슈 혼합 문서는 유지하지 않고, repo에는 `docs/project/open_issues_register.md` 형태의 운영 이슈 기준안만 남긴다.
6. 확정 결정은 `docs/chat-notes/` 아래 날짜 기반 메모로 남긴다.

## 반영 파일

- `docs/README.md`
- `docs/project/notion_workspace_guide.md`
- `docs/project/project_operating_model.md`
- `docs/project/wbs.md`
- `docs/project/effort_estimate.md`
- `docs/project/open_issues_register.md`

## 열린 항목

- 확인 필요: 실제 Notion 페이지 생성 여부와 팀 내 접근 권한 구성
- 확인 필요: 실무 리드 역할을 누가 맡을지
- 확인 필요: 비정형 기능과 `waiting` 경로를 1차 필수 범위에 포함할지 여부
