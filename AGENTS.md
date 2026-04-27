# Repository Working Rules

이 문서는 팀원과 AI agent가 같은 기준으로 작업하기 위한 저장소 공통 규칙이다.

## 기본 원칙

- 프로젝트 문서는 기본적으로 한국어로 작성한다.
- 코드나 문서로 확인되지 않은 내용은 단정하지 말고 `확인 필요:`로 표시한다.
- 구조도나 흐름도는 Mermaid를 우선 사용한다.
- 현재 구현과 장기 설계를 섞지 않는다. 미구현 방향은 `현재 미구현`, `장기 방향`, `확인 필요:`로 구분한다.
- 사용자-facing 설명은 제품 흐름 중심으로 쓰고, 내부 구현 세부는 필요한 문서나 component README에만 둔다.

## 문서 기준

- 루트 `README.md`는 제품 설명, 핵심 흐름, 실행/검증 진입점의 기준 문서다.
- `docs/api/openapi.yaml`은 전체 HTTP API 계약의 기준이다.
- `docs/api/openapi.frontend.yaml`은 프론트 필수 API 계약의 기준이다.
- `docs/api/local.http`는 로컬 API 호출 예시와 프론트 흐름 확인용 request 모음이다.
- `docs/skill/skill_registry.md`는 runtime skill 계약 요약이다.
- `docs/skill/skill_implementation_status.md`는 skill별 구현 방식과 안정도 요약이다.
- `docs/architecture/project_map.mmd`는 현재 컴포넌트 관계만 짧게 유지한다.
- component README는 코드맵 용도로만 사용한다. 제품 설명을 중복해서 길게 쓰지 않는다.

## Skill 정의 구분

- 제품 runtime skill의 source는 `config/skill_bundle.json`이다.
- Python runtime skill 구현은 `workers/python-ai/src/python_ai_worker/skills/`를 기준으로 본다.
- Go control plane의 skill 연동은 `apps/control-plane/internal/skills/`를 기준으로 본다.
- `docs/skill/*`는 runtime skill 계약과 구현 상태를 사람이 읽기 쉽게 요약한 문서다.
- Codex나 개인 자동화용 skill과 제품 runtime skill을 혼동하지 않는다.

## 코드 변경 원칙

- 변경 전 관련 코드, 테스트, 설정, 문서를 먼저 확인하고 현재 동작을 기준으로 작업한다.
- 관련 없는 파일 변경, formatting-only 변경, 대규모 재정렬은 피한다.
- 기존 사용자의 미커밋 변경을 되돌리지 않는다.
- HTTP API 계약에 영향을 주는 변경이면 `docs/api/openapi.yaml`을 반드시 갱신하고, 프론트 영향이 있으면 `docs/api/openapi.frontend.yaml`, 호출 예시가 바뀌면 `docs/api/local.http`를 함께 갱신한다.
- Dataset, build stage, planner, execution 흐름을 바꾸면 `README.md` 또는 `docs/architecture/project_map.mmd` 갱신 필요 여부를 확인한다.
- Skill을 추가하거나 제거하면 `config/skill_bundle.json`, worker handler, 테스트, `docs/skill/*`를 함께 점검한다.

## 작업 범위 원칙

- 요청된 범위 안에서만 수정한다.
- 직접 관련 없는 리팩토링, rename, 파일 이동, import 정리, formatting-only 변경은 하지 않는다.
- 미사용으로 보이는 코드나 파일도 호출 경로와 사용처 확인 전 임의 삭제하지 않는다.
- generated file 여부를 먼저 확인하고, 생성 규칙이 있으면 수동 수정 대신 생성 경로를 우선 확인한다.
- TODO, placeholder, mock 코드는 현재 적용 범위를 문서나 주석으로 명확히 남긴다.
- 동작을 깨뜨린 상태로 TODO만 남기고 작업을 끝내지 않는다.

## 문서 표현 규칙

- `구현됨`, `사용됨`, `지원함` 같은 표현은 코드, 테스트, API 계약으로 확인된 경우에만 사용한다.
- 예정, 고려, 가능, 추정은 현재 구현 설명과 분리한다.
- 예시가 실제 계약과 다를 수 있으면 예시라고 명시한다.
- 체크리스트 문서는 완료/미완료 상태를 구분해 표시한다.
- 장기 방향은 현재 기능처럼 읽히지 않게 `장기 방향` 또는 `현재 미구현`으로 표시한다.

## 검증 기준

기본 검증 명령:

```bash
(cd apps/control-plane && go test ./...)
PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'
PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.run_skill_case --validate
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); puts "ok"'
```

변경 범위가 API, workflow, dataset build에 닿으면 관련 smoke script도 확인한다.

## 검증 결과 기록 원칙

- 실행한 검증 명령과 결과를 남긴다.
- 실행하지 못한 검증은 사유를 남긴다.
- 실패한 검증을 성공처럼 요약하지 않는다.
- 일부만 확인한 경우 전체 검증 완료처럼 표현하지 않는다.
- 남아 있는 위험 요소, 확인 필요 항목은 별도로 정리한다.

## 리뷰 / PR 기준

- 변경 목적과 영향 범위를 짧게 정리한다.
- API, config, workflow, dataset build, skill registry에 영향이 있으면 관련 문서와 테스트 갱신 여부를 함께 확인한다.
- 하위 호환성 영향이 있으면 반드시 표시한다.
- 현재 미구현 또는 추정 사항은 구현 완료처럼 표현하지 않는다.
- 관련 없는 변경이 섞이지 않도록 작업 단위를 유지한다.

## Git / Commit 규칙

- 커밋 메시지는 기본적으로 한국어로 작성한다.
- 커밋 메시지 접두사는 `feat:`, `fix:`, `refactor:`, `doc:`만 사용한다.
- `feat(scope):`처럼 scope를 붙이는 형식은 사용하지 않는다.
- 한 커밋은 하나의 작업 단위만 담는다.
- 하나의 PR은 하나의 목적만 담는다.
- 기능 변경과 문서 정리는 같은 목적이면 함께 묶되, unrelated fix는 분리한다.
- main 또는 공유 브랜치에 올리기 전에는 현재 브랜치의 변경 범위와 삭제 파일을 반드시 확인한다.
- 머지 전 `git diff --name-only`, `git diff --stat`, 삭제 파일 목록을 확인한다.
- 의도하지 않은 lock file, generated file, local config 변경이 포함되지 않았는지 확인한다.
- 대규모 변경 전에는 작은 단위로 나눌 수 있는지 먼저 검토한다.

## AI Agent 사용 시 참고

- AI agent 작업도 사람 작업과 동일한 문서 기준, 변경 원칙, 검증 기준을 적용하며 예외를 두지 않는다.
- 프로젝트 요약이나 구조 문서를 갱신할 때는 현재 code/API/config를 먼저 확인하도록 지시한다.
- 추정으로 API 계약, 환경변수, 설정값을 만들지 않는다.
- deprecated처럼 보이는 코드도 호출 경로 확인 전 삭제하지 않는다.
- AI agent가 생성한 devlog, chat note, 임시 문서는 팀 공유 대상인지 확인하고 커밋 여부를 결정한다.
- AI agent가 제안한 장기 방향은 현재 구현으로 오해되지 않게 `현재 미구현` 또는 `장기 방향`으로 표시한다.
- AI agent가 실패한 검증, 미실행 검증, 남은 위험을 숨기지 않도록 결과 기록을 요구한다.
