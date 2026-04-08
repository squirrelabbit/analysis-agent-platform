# 마이그레이션 계획

이 문서는 초기 Python MVP에서 현재 구조로 옮기는 과정의 기준과 남은 축을 간단히 정리한 기록이다.

## 이미 반영된 축

- `Go control plane` 분리
- `Temporal` 기반 analysis/build workflow 도입
- `DuckDB` structured 실행 경로 연결
- `Python AI worker` 분리
- dataset build를 `prepare / sentiment / embedding / cluster` 비동기 job으로 정리
- `result_v1 snapshot`과 `final_answer` 후처리 레이어 도입

## 현재 남은 축

1. 프론트 연결
2. 시나리오 품질 검증
3. 추가 성능 최적화
4. 운영 환경의 Temporal persistence/retention 정책 고정
5. `확인 필요:` Rust hot path 도입 여부 판단

## 판단 기준

- 새 구조가 request -> plan -> execute -> result 흐름을 재현 가능하게 유지하는가
- build job과 execution이 재시작 후 다시 평가 가능한가
- profile, prompt, artifact metadata가 재실행 기준으로 충분한가
- smoke와 unit test로 주요 경로가 잠기는가

## 참고

- 현재 제품 정의는 [../project_summary.md](../project_summary.md)
- 런타임 목표 구조는 [target_stack.md](target_stack.md)
- 비정형 저장 경로 변화는 [unstructured_storage_transition.md](unstructured_storage_transition.md)
