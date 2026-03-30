# 목표 스택

## 1. 목적

- Python MVP에서 운영형 구조로 옮기기 위한 목표 런타임을 정의한다.
- Skill 수가 많아져도 성능, 관측성, 재시도, 확장성을 유지하는 것을 목표로 한다.

## 2. 핵심 구성

| 구성 | 역할 |
| --- | --- |
| Go control plane | API, auth, request validation, Temporal 시작점 |
| Temporal | durable workflow, retry, waiting, rerun/diff orchestration |
| DuckDB | structured Skill 실행 엔진 |
| Postgres | 프로젝트, dataset version, plan, execution 메타데이터 저장 |
| Python AI workers | planner, embeddings, semantic search, evidence generation |
| Rust skill workers | CPU 집약 Skill와 대용량 텍스트 처리 |
| Artifact storage | 결과 계약, 로그, evidence bundle 저장 |

## 3. 실행 흐름

1. 클라이언트가 분석 요청을 생성한다.
2. Go control plane이 프로젝트, dataset version, request payload를 검증한다.
3. control plane이 Temporal workflow를 시작한다.
4. workflow가 planner step을 호출해 `SkillPlan`을 만든다.
5. workflow가 registry와 dataset contract를 기준으로 plan을 검증한다.
6. structured step은 DuckDB runtime으로 실행한다.
7. AI/embedding/evidence step은 Python worker로 실행한다.
8. CPU 집약 step은 Rust worker로 실행한다.
9. workflow가 결과, 로그, artifacts를 Postgres와 storage에 남긴다.
10. 같은 execution context를 기준으로 rerun/diff 한다.

## 4. 왜 이 조합인가

- 현재 Python MVP는 API, planner, validator, executor, waiting 판정이 한 덩어리에 가까워서 Skill 수가 늘수록 병목과 운영 복잡도가 같이 증가한다.
- Go는 control plane과 운영형 API에 적합하다.
- Temporal은 `queued/running/waiting/retry/resume`를 애플리케이션 코드보다 안정적으로 다룬다.
- DuckDB는 structured 계산을 Python loop보다 큰 규모에서 더 효율적으로 처리한다.
- Python은 LLM과 임베딩 생태계 때문에 계속 필요하지만, 전체 control plane까지 맡기지 않는 편이 낫다.
- Rust는 전체 시스템을 다시 쓰기보다, 병목 Skill만 고성능 worker로 분리하는 데 적합하다.

## 5. 설계 원칙

- control plane과 Skill execution runtime을 분리한다.
- Skill은 언어가 아니라 contract 중심으로 관리한다.
- workflow 상태는 worker 메모리가 아니라 Temporal history에 둔다.
- structured와 unstructured 실행 경로를 같은 execution contract 안에 묶는다.
- 결과는 요약치뿐 아니라 evidence와 metadata까지 함께 저장한다.

## 6. 현재 구현 기준

- `apps/control-plane/` 기준으로 `memory/noop` 경로는 테스트로 검증됐다.
- `postgres` 저장소와 `temporal` starter는 코드 경계와 SDK 연동이 들어가 있다.
- 확인 필요:
  실제 Temporal server와 Postgres runtime에 붙인 통합 검증은 아직 수행하지 못했다.

## 7. 데이터 경계

- `Postgres`
  프로젝트, 요청, plan, execution, job, registry 메타데이터
- `DuckDB`
  structured dataset scan, 집계, 순위, 기간 비교
- `Artifact storage`
  result bundle, logs, evidence pack
- 확인 필요:
  비정형 원문 저장을 Postgres 중심으로 둘지, object storage까지 확장할지는 별도 합의가 필요하다.

## 8. 저장소 구조 방향

- `apps/control-plane/`
- `workers/python-ai/`
- `workers/rust-skills/`
- `docs/`
- `src/`
  레거시 Python MVP. 완전 이관 전까지는 비교 기준으로만 본다.
