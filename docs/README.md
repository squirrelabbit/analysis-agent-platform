# 문서 안내

## 원칙

- 루트 [README.md](../README.md) 는 입구만 유지한다.
- component README는 코드맵으로만 쓰고, 제품 설명을 중복하지 않는다.
- 현재 상태는 `project_summary`, 운영은 `manual/runbook`, 계약은 `openapi/skill_registry`, 구현 현실은 `skill_implementation_status`에만 남긴다.
- 완료된 이행 기록은 역사 문서로 취급하고, 메인 동선에서 내린다.

## 먼저 볼 문서

- [project_summary.md](project_summary.md)
- [../manual.md](../manual.md)
- [operations/local_runbook.md](operations/local_runbook.md)
- [api/openapi.yaml](api/openapi.yaml)
- [skill/skill_registry.md](skill/skill_registry.md)
- [skill/skill_implementation_status.md](skill/skill_implementation_status.md)

## 문서 역할

| 축 | 대표 문서 | 역할 |
| --- | --- | --- |
| 입구 | `README.md` | 제품 개요와 빠른 시작 |
| 현재 상태 | `docs/project_summary.md` | 현재 제품 정의와 실행 흐름 |
| 운영 | `manual.md`, `docs/operations/local_runbook.md` | 로컬 실행, 확인, 복구 입구 |
| 계약 | `docs/api/openapi.yaml`, `docs/skill/skill_registry.md` | HTTP/API 및 runtime skill 계약 |
| 구현 현실 | `docs/skill/skill_implementation_status.md` | 스킬 구현 방식과 안정도 |
| 기록 | `docs/chat-notes/*.md`, `docs/devlog/*.md` | 결정과 작업 이력 |

## 역사 문서

- [architecture/migration_plan.md](architecture/migration_plan.md)
  - 완료된 이행 기록
- [architecture/unstructured_storage_transition.md](architecture/unstructured_storage_transition.md)
  - 현재 저장 구조 참고와 전환 배경

참고:
- runtime skill 메타데이터 source는 문서가 아니라 `config/skill_bundle.json`이다.
- prompt/profile/policy 기본값은 `config/prompts/`, `config/dataset_profiles.json`, `config/skill_policies/`를 기준으로 본다.
