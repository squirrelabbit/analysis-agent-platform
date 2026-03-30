# 언어별 역할

## 원칙

- 언어를 기능별로 나누지 말고 책임 경계별로 나눈다.
- 한 기능이 여러 언어를 오가더라도 계약은 하나로 유지한다.
- 운영형 control plane, AI runtime, high-performance kernel을 분리한다.

## 역할 표

| 기술 | 주 역할 | 맡기면 좋은 일 | 맡기지 않을 일 |
| --- | --- | --- | --- |
| Go | control plane | API, auth, request validation, Temporal client, execution orchestration entrypoint | 임베딩, LLM 실험 코드, 대규모 텍스트 전처리 |
| Temporal | workflow engine | retry, waiting, resume, rerun/diff, long-running execution history | 데이터 계산 자체 |
| DuckDB | structured compute | aggregate, compare, rank, period analysis, dataset scan | 인증, workflow 상태관리 |
| Postgres | metadata store | project, dataset_version, plan, execution, registry metadata | 대규모 analytical scan |
| Python | AI worker | planner, LLM 호출, embedding, semantic search, evidence generation | 전체 control plane, durable workflow 상태 저장 |
| Rust | high-performance worker | 토큰화, 군집화, 대용량 텍스트 처리, CPU 집약 Skill | 잦은 제품 정책 변경이 있는 orchestration |

## Go

- 추천 이유:
  - 운영형 API와 동시성 처리에 적합하다.
  - 배포 산출물이 단순하다.
  - control plane과 worker supervisor를 단순하게 유지하기 좋다.
- 주요 책임:
  - project / dataset / analysis / execution API
  - 인증과 권한 확인
  - workflow 시작과 결과 조회
  - skill registry 메타데이터 조회

## Python

- 추천 이유:
  - LLM, embedding, NLP 도구 생태계가 가장 풍부하다.
  - 분석팀과 실험 코드 연결이 쉽다.
- 주요 책임:
  - planner
  - embedding pipeline
  - semantic search
  - evidence bundle 생성
- 제한:
  - structured 집계 엔진과 workflow 상태관리까지 모두 Python에 남기지 않는다.

## Rust

- 추천 이유:
  - CPU 집약 Skill에서 높은 성능과 예측 가능한 메모리 사용을 기대할 수 있다.
  - 텍스트 전처리와 clustering 같은 hot path를 분리하기 좋다.
- 주요 책임:
  - 고성능 support Skill
  - 대규모 문서 토큰화/정규화
  - clustering / dedup / cooccurrence 같은 연산
- 제한:
  - 자주 바뀌는 제품 정책과 API orchestration은 Rust 중심으로 두지 않는다.

## DuckDB와 Postgres를 같이 두는 이유

- `DuckDB`는 계산 엔진이다.
- `Postgres`는 운영 메타데이터 저장소다.
- 둘을 합치려 하지 않는 편이 역할이 더 명확하다.

## 팀 운영 관점 추천

- 개발팀 백엔드 중심 인력은 Go를 기준으로 본다.
- AI/분석 실험과 모델 연동은 Python을 기준으로 본다.
- 성능 최적화가 명확히 필요한 Skill만 Rust로 올린다.
- 1차에서는 Rust가 없어도 시작할 수 있지만, 구조는 처음부터 분리해 두는 편이 낫다.
