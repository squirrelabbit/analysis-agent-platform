# 문서 안내

## 목적

- 루트 문서에는 입구만 남기고, 상세 내용은 성격별 문서로 분리한다.
- 제품 정의, 운영 절차, 테스트 절차, 아키텍처 문서를 서로 섞지 않는다.
- 코드와 문서가 다르면 코드를 우선 보고, 아직 정리되지 않은 내용만 `확인 필요:`로 남긴다.

## 문서 분류

| 구분 | 문서 | 역할 |
| --- | --- | --- |
| 입구 | `README.md` | 제품 개요, 빠른 시작, 핵심 링크 |
| 요약 | `docs/project_summary.md` | 현재 제품 정의와 핵심 실행 흐름의 짧은 스냅샷 |
| 운영 | `manual.md` | 로컬 운영 입구와 상세 문서 링크 |
| 운영 | `docs/operations/local_runbook.md` | stack 실행, health, 로그, artifact 경로 |
| 운영 | `docs/recovery_guide.md` | build failed, waiting, failed 대응 절차 |
| 테스트 | `docs/testing/smoke_and_checks.md` | 코드 테스트와 smoke 검증 순서 |
| 테스트 | `docs/testing/manual_api_walkthrough.md` | 수동 API 호출 예시 |
| 아키텍처 | `docs/architecture/*.md` | 목표 구조, 마이그레이션, 저장 방식 설명 |
| API | `docs/api/openapi.yaml` | control-plane HTTP API 계약 |
| Skill | `docs/skill/*.md` | runtime skill 계약과 분석 기법 |
| 결정 로그 | `docs/chat-notes/*.md` | 확정된 결정과 채팅 보관 |
| 작업 로그 | `docs/devlog/*.md` | daily 고민, 실험, 다음 액션 |

## 지금 가장 자주 보는 문서

- [project_summary.md](project_summary.md)
- [../manual.md](../manual.md)
- [operations/local_runbook.md](operations/local_runbook.md)
- [testing/smoke_and_checks.md](testing/smoke_and_checks.md)
- [recovery_guide.md](recovery_guide.md)
- [api/openapi.yaml](api/openapi.yaml)

참고:
- runtime skill 메타데이터 source는 문서가 아니라 저장소 루트의 `config/skill_bundle.json`이다.
- dataset profile 기본값은 `config/dataset_profiles.json`, prompt template는 `config/prompts/`를 기준으로 본다.
