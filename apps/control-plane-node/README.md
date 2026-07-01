# control-plane-node (Go → Node 전환 PoC)

control-plane을 Go에서 Node/TS로 전환하는 strangler 마이그레이션. **openapi.yaml 계약 동결,
내부 OO 재설계.** 계획: vault `계획/Node전환_계획.md`.

## 스택 (하위 결정)
- **NestJS** (module/controller/service/repository 계층 — 인수인계용 구조)
- **pg + Kysely** (raw SQL 근접 포팅, god-file postgres.go 대체)
- **@temporalio/client** (dataset build — 예정)
- DuckDB artifact 집계는 Python worker로 이동 (ADR-024 정합 — 예정)

## 실행
```bash
npm install
npm run build
DATABASE_URL='postgresql://platform:platform@127.0.0.1:15432/analysis_support' PORT=18081 node dist/main.js
# Go control-plane(:18080)과 별도 포트. strangler로 포팅된 경로만 Node.
```

## PoC 진행 (2026-07-01)

### ✅ 1단계 — DB + 계약 parity 증명 (완료)
- NestJS 기동 + Postgres 연결(`GET /healthz` → `{status:ok, db:up}`)
- `GET /projects` — Go `handleListProjects` 계약 포팅. **counts·metadata·description 완전 일치.**
- Go의 dataset별 version 조회(N+1)를 **단일 상관 서브쿼리**로 대체 (계약 동일, 쿼리 수 개선).

**parity 잔여 1건**: `created_at` 정밀도 — Go µs(`.995016+09:00`) vs Node ms(`.995000Z`). 같은 순간이나
표현 다름. 원인: pg가 timestamptz를 JS `Date`(ms)로 반환. **해결법**: pg 타입 파서로 timestamptz(OID 1184)를
문자열 passthrough → Go 포맷 정합. (계약-parity 게이트에서 처리)

### ✅ 2단계 — Temporal 상호운용 de-risk (완료, 최대 미지수 해소)
`src/poc/temporal-check.ts` (실행: `node dist/poc/temporal-check.js`). 아무것도 새로 안 띄우고
기존 Go 워크플로를 읽어 검증:
- describeTaskQueue(`analysis-support-build`) → **Go temporal-worker 폴러 확인** (Node↔server↔Go worker 동일 채널)
- Go가 start한 `dataset.build.v1` 실행 목록 조회 (Node가 Go 워크플로 상태 읽음)
- **시작 입력 페이로드 Go→Node byte-perfect 디코딩** (`DatasetBuildWorkflowInput` = job_id/build_type/...) →
  **payload 상호운용 확정.** Node client가 같은 JSON으로 start하면 기존 Go worker가 실행(strangler 공존).

**스코프 발견**: 워크플로+5개 액티비티는 **Go temporal-worker**에 있다(Python 아님). 액티비티가 Python
worker를 HTTP 호출. 따라서 전체 마이그레이션은 control-plane + temporal-worker 둘 다 포함 — 단 strangler로
Go worker는 후순위, Node control-plane(client)부터 간다.

### ✅ 3단계 — Temporal 쓰기경로 de-risk (완료)
`src/poc/temporal-start.ts` — **Node client가 워크플로 start → Go worker 실행** 확정.
안전장치: job row 없는 fake job_id로 start → Go 워크플로 첫 액티비티 mark_running이 GetDatasetBuildJob
ErrNotFound로 멈춰 실제 clean(execute)까지 안 감. pickup(mark_running 스케줄) 확인 후 terminate →
데이터 변형 0. → Node↔Go worker **양방향** 상호운용 완결(읽기=2단계, 쓰기=3단계).

**de-risk 결론**: 마이그레이션 최대 미지수(DB/계약 parity, Temporal 양방향 interop) 전부 해소.
남은 건 de-risk가 아니라 **포팅 노동** + 아래 저위험 항목.

### ⬜ 다음 (포팅 단계)
- read 경로 확대 (datasets/versions/artifact views — 집계는 worker 이동)
- build 트리거 엔드포인트 정식 포팅 (job insert + workflow start, 3단계 검증됨)
- Python worker HTTP 프록시 (analyze — 단순 HTTP, 저위험)
- reverse-proxy strangler 라우팅
- (후순위) Go temporal-worker(workflow+activities) → Node worker

## 구조
```
src/
  main.ts              # 엔트리(별도 포트)
  app.module.ts
  db/db.module.ts      # Kysely + pg Pool (DI 토큰 'DB')
  health/              # de-risk 헬스체크
  projects/            # controller → service → repository (Go의 http→service→store 대체)
```
