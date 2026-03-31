# 문서 안내

## 목적

- 문서를 목표 아키텍처 기준의 최소 세트로 줄인다.
- 제품 정의, 언어 역할, 마이그레이션 경로를 빠르게 확인할 수 있게 한다.
- 현재 상태 스냅샷과 daily 작업 로그, 결정 로그를 섞지 않는다.

## 지금 보면 되는 문서

| 문서 | 역할 |
| --- | --- |
| `docs/project_summary.md` | 제품 정의와 현재 실행 흐름의 짧은 스냅샷 |
| `docs/devlog/README.md` | daily 고민/챌린지 로그 안내와 작성 규칙 |
| `docs/architecture/target_stack.md` | 목표 아키텍처와 실행 흐름 |
| `docs/architecture/language_roles.md` | Go, Python, Rust, DuckDB, Postgres의 책임 구분 |
| `docs/architecture/migration_plan.md` | 레거시 Python MVP에서 목표 구조로 옮기는 단계 |
| `docs/architecture/unstructured_storage_transition.md` | 비정형 dataset의 JSONL artifact를 Parquet + vector index 구조로 바꾸는 설계 |
| `docs/architecture/project_map.mmd` | 시스템 수준 구조도 |
| `docs/skill/skill_registry.md` | Skill contract와 Core/Support 분류 기준 |
| `docs/skill/analysis_techniques.md` | skill별 분석 기법과 사용 기술 설명 |
| `docs/api/openapi.yaml` | control-plane HTTP API 명세 |

참고:
- runtime 기준 skill 메타데이터 source는 문서가 아니라 저장소 루트의 `config/skill_bundle.json`이다.

## 정리 원칙

- 루트/요약 문서에는 현재 상태와 입구 정보만 남긴다.
- 매일의 고민과 실험 메모는 `docs/devlog/`에 남긴다.
- 확정된 결정은 `docs/chat-notes/` 아래에 남긴다.
- 코드와 문서가 다르면 코드를 우선 보고, 아직 옮기지 않은 항목은 `확인 필요:`로 적는다.
