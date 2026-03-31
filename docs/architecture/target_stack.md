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
| Python AI workers | planner, embeddings, semantic search, evidence generation, 현재 구현의 비정형 deterministic skill |
| Rust skill workers | 확인 필요: 현재는 스캐폴드만 있고 향후 CPU 집약 Skill 후보를 위한 경로 |
| Artifact storage | 결과 계약, 로그, evidence bundle 저장 |

## 3. 실행 흐름

1. 클라이언트가 분석 요청을 생성한다.
2. Go control plane이 프로젝트, dataset version, request payload를 검증한다.
3. control plane이 Temporal workflow를 시작한다.
4. workflow가 planner step을 호출해 `SkillPlan`을 만든다.
5. workflow가 registry와 dataset contract를 기준으로 plan을 검증한다.
6. structured step은 DuckDB runtime으로 실행한다.
7. AI/embedding/evidence step과 현재 구현된 비정형 deterministic skill은 Python worker로 실행한다.
8. CPU 집약 step이 실제 hot path로 확인되면 Rust worker로 분리한다.
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

## 6. Skill 확장 전략

- 현재 단계에서는 완전 동적 플러그인보다 `skill bundle` 중심 확장을 우선한다.
- `skill bundle`은 skill 이름, 실행 엔진, task path, 기본 입력, dataset readiness, planner 노출 여부, artifact contract version 같은 메타데이터를 묶는 공용 계약으로 본다.
- 목적은 skill 추가나 수정 시 control plane과 worker 코드에 흩어진 switch/list 수정 범위를 줄이고, 메타데이터 변경과 코드 변경을 가능한 한 분리하는 데 있다.
- 내부 사용 단계에서는 공식 지원 skill을 제품 소유 코드베이스 안에서 관리한다.
- 고객용 확장 단계에서 tenant별 custom skill, 외부 파트너 확장, skill별 독립 배포, 격리 실행 요구가 커지면 동적 플러그인 모델을 다시 검토한다.
- 동적 플러그인은 보안, 버전 호환, replay 재현성, 운영 복잡도를 함께 높이므로 기본 전략으로 두지 않는다.
- 확인 필요:
  `skill bundle`의 저장 위치와 배포 방식은 repo 포함 정적 파일, config, metadata store 중 어떤 방식으로 둘지 별도 합의가 필요하다.

## 7. 현재 구현 기준

- `apps/control-plane/` 기준으로 `memory/noop` 경로는 테스트로 검증됐다.
- `postgres` 저장소와 `temporal` starter는 코드 경계와 SDK 연동이 들어가 있다.
- plan skill 메타데이터의 runtime source는 `config/skill_bundle.json`이고, Go control plane과 Python worker가 이 bundle을 함께 읽는다.
- Python worker에는 planner, evidence, semantic search뿐 아니라 `deduplicate_documents`, `dictionary_tagging`, `embedding_cluster`, `issue_cluster_summary`, `issue_taxonomy_summary`가 현재 구현돼 있다.
- GitHub Actions CI는 Go 테스트/빌드와 Python worker 테스트를 현재 구조 기준으로 실행한다.
- 개발용 smoke script는 `/uploads` API를 통해 입력 파일을 먼저 올리고 dataset version을 생성하도록 정리돼 있다.
- 확인 필요:
  이번 turn에서는 `smoke.sh`, `smoke_cluster.sh`, `smoke_taxonomy.sh`만 재실행했고, 나머지 smoke는 문법 점검만 수행했다.

## 8. 데이터 경계

- `Postgres`
  프로젝트, 요청, plan, execution, job, registry 메타데이터
- `DuckDB`
  structured dataset scan, 집계, 순위, 기간 비교
- `Artifact storage`
  result bundle, logs, evidence pack
- 비정형 dataset build artifact는 현재 JSONL 중심이고, Parquet + vector index 전환 설계는 `docs/architecture/unstructured_storage_transition.md`에 별도로 정리한다.
- 확인 필요:
  비정형 원문 저장을 Postgres 중심으로 둘지, object storage까지 확장할지는 별도 합의가 필요하다.

## 9. 저장소 구조 방향

- `apps/control-plane/`
- `workers/python-ai/`
- `workers/rust-skills/`
- `docs/`
- 확인 필요:
  레거시 Python MVP용 `src/` 디렉터리는 현재 저장소에 없다.
