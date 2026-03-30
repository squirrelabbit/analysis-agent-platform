# 문서 안내

## 목적

- 문서를 목표 아키텍처 기준의 최소 세트로 줄인다.
- 제품 정의, 언어 역할, 마이그레이션 경로만 빠르게 확인할 수 있게 한다.
- 회의 메모와 과거 결정은 `docs/chat-notes/`에 두고, 실행 기준 문서와 섞지 않는다.

## 지금 보면 되는 문서

| 문서 | 역할 |
| --- | --- |
| `docs/project_summary.md` | 제품 정의와 현재 마이그레이션 상태 요약 |
| `docs/architecture/target_stack.md` | 목표 아키텍처와 실행 흐름 |
| `docs/architecture/language_roles.md` | Go, Python, Rust, DuckDB, Postgres의 책임 구분 |
| `docs/architecture/migration_plan.md` | 레거시 Python MVP에서 목표 구조로 옮기는 단계 |
| `docs/architecture/project_map.mmd` | 시스템 수준 구조도 |
| `docs/skill/skill_registry.md` | Skill contract와 Core/Support 분류 기준 |
| `docs/api/openapi.yaml` | control-plane HTTP API 명세 |

## 정리 원칙

- 중복되는 상세 문서는 삭제하고, 기준 문서만 남긴다.
- 변동성이 큰 작업관리 문서는 Notion에서 관리한다.
- 확정된 결정은 `docs/chat-notes/` 아래에 남긴다.
- 코드와 문서가 다르면 코드를 우선 보고, 아직 옮기지 않은 항목은 `확인 필요:`로 적는다.
