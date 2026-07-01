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

### ⬜ 다음 PoC 증분 (남은 큰 미지수)
- **@temporalio/client** ↔ Python workflow 상호운용 (dataset build 트리거 1콜)
- Python worker HTTP 프록시 1콜 (analyze 등)
- reverse-proxy로 포팅 경로만 Node 라우팅 (strangler cutover 골격)

## 구조
```
src/
  main.ts              # 엔트리(별도 포트)
  app.module.ts
  db/db.module.ts      # Kysely + pg Pool (DI 토큰 'DB')
  health/              # de-risk 헬스체크
  projects/            # controller → service → repository (Go의 http→service→store 대체)
```
