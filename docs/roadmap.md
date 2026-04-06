# 앞으로 해야 할 작업

이 문서는 현재 구현 상태를 기준으로, 다음 작업을 **순서대로** 정리한 실행용 로드맵이다.
설명보다 실행 순서를 우선하고, 아직 정책 합의가 없는 항목은 `확인 필요:`로 표시한다.

## 기준

- 현재 strict 시나리오 등록/import/plan/one-shot execute는 동작한다.
- `prepare/sentiment/embedding`, `result_v1 snapshot`, `report draft`, local embedding 기반 검색/군집도 기본 경로는 연결돼 있다.
- dataset version에는 현재 `profile v1`을 직접 붙일 수 있고, `prepare_prompt_version`, `sentiment_prompt_version`, `regex_rule_names`, `garbage_rule_names`, `embedding_model`을 저장한다.
- execution은 현재 dataset version `profile`을 `profile_snapshot`으로 복사하고 `result_v1.profile`에도 함께 노출한다.
- 남은 핵심은 `자동화`, `성능`, `운영 안정성`, `품질 검증`이다.

## Step 1. dataset dependency 자동 orchestration

목표:
- 사용자가 `scenario execute`나 analysis execute를 눌렀을 때 `prepare/sentiment/embedding` 준비 상태를 몰라도 되게 만든다.

할 일:
1. `scenario execute`에서 필요한 dependency를 먼저 계산한다.
   - `requires_prepare`
   - `requires_sentiment`
   - `requires_embedding`
2. 부족한 artifact가 있으면 자동 build를 시작한다.
3. build 완료 후 본 execution을 이어서 진행한다.
4. `waiting`은 예외 상황으로만 남긴다.

결과물:
- 자동 dependency build 경로
- `waiting` 발생 조건 축소
- 사용자가 수동 `resume`을 덜 해도 되는 흐름

주의:
- 지금 dataset build service는 동기식 worker 호출이라, 이 단계 전후로 async job abstraction이 필요할 가능성이 높다.

## Step 2. dataset build를 비동기 job으로 분리

목표:
- `prepare/sentiment/embedding`이 API 응답 시간과 강하게 묶이지 않게 만든다.

할 일:
1. dataset build를 execution과 별도 job 단위로 정의한다.
2. `prepare/sentiment/embedding`을 동기 API 호출 대신 queue/worker 기반으로 바꾼다.
3. 상태 조회 API를 정리한다.
4. 중복 실행 방지와 재시도 정책을 붙인다.

결과물:
- dataset build job 모델
- 상태 조회/재시도 정책
- API timeout/backpressure 리스크 완화

## Step 3. 성능 최적화 1차

목표:
- 한 작업당 메모리와 중복 IO를 줄인다.

할 일:
1. `prepare/sentiment/embedding`에서 full in-memory 적재를 줄인다.
   - streaming write
   - batch flush
2. `sentiment` batch labeling을 도입한다.
3. `prior_artifacts` 전달 크기를 줄인다.
   - full JSON 대신 `artifact_ref + summary` 중심으로 축소
4. 같은 dataset을 step마다 다시 읽는 패턴을 줄인다.
   - selection materialization
   - reusable filtered parquet

결과물:
- 메모리 피크 감소
- worker 요청 payload 감소
- 대용량 dataset 실행 안정성 개선

## Step 4. 시나리오 S1~S5 품질 검증

목표:
- 대표 질문 세트가 실제 결과 품질도 맞는지 확인한다.

할 일:
1. `S1`, `S2`, `S4`를 우선 기준 시나리오로 잡는다.
2. 실제 dataset으로 실행해 결과를 비교한다.
3. 아래 기준으로 보정한다.
   - step 과다/과소 여부
   - keyword vs noun_frequency 선택
   - evidence 품질
   - trend/compare/breakdown의 부족한 support skill
4. fixture와 smoke를 시나리오 기준으로 강화한다.

결과물:
- 시나리오별 품질 메모
- 수정된 strict 시나리오 템플릿
- 회귀 테스트 자산

## Step 5. execution 결과 활용 흐름 강화

목표:
- 실행 결과를 사용자가 다시 소비하는 흐름을 안정화한다.

할 일:
1. execution list에서 결과 preview 품질을 다듬는다.
2. report draft를 여러 execution 기준으로 실제 초안 생성 흐름과 연결한다.
3. 결과 snapshot 재생산 정책을 정한다.
   - 언제 갱신할지
   - 언제 고정할지

결과물:
- 결과 목록 UX 기준 문서
- 보고서 초안 생성 기준
- snapshot 운영 규칙

## Step 6. 운영 최소 기능

목표:
- 내부 파일럿을 넘어서 반복 운영 가능한 수준으로 간다.

할 일:
1. auth/권한 최소 버전
2. usage/cost 모니터링 정리
3. retry/timeout 정책 명문화
4. failure/waiting 대응 매뉴얼 정리

결과물:
- 최소 운영 정책
- 비용/장애 대응 기준

## Step 7. 운영 hardening

목표:
- 운영형 서비스로 갈 때 필요한 안정성 작업을 마무리한다.

할 일:
1. review queue 또는 human-in-the-loop 흐름
2. rule snapshotting 강화
3. observability
   - metrics
   - tracing
   - alert 기준
4. artifact 외부 저장소/보존 정책

결과물:
- 운영 안정화 설계
- 장기 보존/감사 기준

## 지금 당장 추천 순서

1. dataset dependency 자동 orchestration
2. dataset build async job화
3. 메모리/중복 IO 최적화
4. S1~S5 실제 결과 품질 보정
5. 보고서/결과 활용 흐름 강화

## 보류해도 되는 것

- guided scenario planner
- guardrail 기반 planner
- Rust hot path 이관
- 대규모 UI/결과 탐색 기능 확대

이 항목들은 현재 핵심 실행 흐름이 안정화된 뒤로 미뤄도 된다.
