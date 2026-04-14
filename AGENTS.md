# Repository Instructions

## Documentation Defaults

- 프로젝트 문서는 기본적으로 한국어로 작성한다.
- 구조도나 흐름도는 Mermaid를 우선 사용한다.
- 코드나 문서로 확인되지 않은 내용은 단정하지 말고 `확인 필요:`로 표기한다.
- `src/skills/`와 `docs/skill/skill_registry.md`는 제품 런타임 Skill 정의이고, `codex/skills/`는 Codex 문서화 스킬이다. 두 체계를 혼동하지 않는다.

## Documentation Workflow

- 프로젝트 요약이나 구조 문서를 갱신할 때는 `$project-doc-writer`를 사용한다.
- 채팅, 구현 작업, 리뷰 결과를 결정 로그로 남길 때는 `$decision-log-updater`를 사용한다.
- Codex 세션 원문을 보관하거나 결정 로그 입력으로 내릴 때는 `$chat-transcript-exporter`를 사용한다.
- `docs/project_summary.md`는 현재 제품 정의와 운영 흐름을 짧게 유지한다.
- `docs/architecture/project_map.mmd`는 주요 컴포넌트 관계만 유지하고 세부 구현 변경에 과하게 흔들리지 않게 관리한다.
- 결정 로그는 `docs/chat-notes/` 아래에 날짜 기반 파일로 저장한다.
- `.agents/skills/`는 Codex가 자동 발견하는 진입점이고, 이 repo에서는 전역 공용 구현 `~/.codex/skills/`를 가리킨다.

## Commit Conventions

- 커밋 메시지는 기본적으로 한국어로 작성한다.
- 커밋 메시지 접두사는 `feat:`, `fix:`, `refactor:`, `doc:`만 사용한다.
- `feat(scope):`처럼 scope를 붙이는 형식은 사용하지 않는다.
- 한 커밋은 하나의 작업 단위만 담고, 메시지는 해당 작업을 간결하게 설명한다.
