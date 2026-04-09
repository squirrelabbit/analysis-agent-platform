# 마이그레이션 기록

이 문서는 초기 Python MVP에서 현재 구조로 옮기는 과정의 완료 기록이다.
신규 개발이나 운영 확인에는 우선 [../project_summary.md](../project_summary.md), [target_stack.md](target_stack.md), [unstructured_storage_transition.md](unstructured_storage_transition.md)를 본다.

이미 반영된 축:
- `Go control plane`, `Temporal`, `DuckDB`, `Python AI worker` 분리
- dataset build의 `prepare / sentiment / embedding / cluster` 비동기화
- `result_v1 snapshot`, `final_answer`, progress/event/step preview surface

남은 항목은 이행 계획이라기보다 후속 최적화 영역이다.
- 프론트 연결
- 성능 최적화
- 운영 환경의 Temporal persistence/retention
- `확인 필요:` Rust hot path 도입 여부
