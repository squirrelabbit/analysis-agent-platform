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

**parity 잔여 1건**: ~~`created_at` 정밀도 — Go µs vs Node ms~~ → **read 경로 확대(4단계)에서 해소.**
pg 타입 파서로 timestamptz(OID 1184)를 raw text로 받고 `common/go-time.ts`가 Go 포맷으로 변환.
핵심 발견: Go는 응답 직전 `displaytime.NormalizeForJSON`이 **모든 time.Time을 KST(+09:00)로 정규화**
하므로, timestamp 계약은 "저장 offset 그대로"가 아니라 **항상 +09:00 표기**다.

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

### ✅ 4단계 — read 경로 확대 1차: 순수 Postgres read 4종 (2026-07-02)
Go 계약 그대로 포팅 + **라이브 parity 검증**(Go :18080 vs Node :18081, jq -S 정규화 diff):
- `GET /projects/{pid}` — counts 포함 project 상세 (목록과 동일 서브쿼리 재사용)
- `GET /projects/{pid}/datasets` / `GET /projects/{pid}/datasets/{did}`
- `GET /projects/{pid}/dataset_build_jobs/{job_id}` — diagnostics 합성 포함
  (retry_count, progress 파일 읽기, llm_fallback/warnings — Go `withBuildJobDiagnostics` +
  `enrichBuildJobDiagnosticsFromVersion` 포팅)

**parity 결과**: projects/datasets 11 endpoint + 404 4종 + **build job 98/98 전부 일치**
(completed/failed × clean/doc_genuineness/clause_label/clause_keywords 전 조합).
에러 계약도 Go `writeError`의 `{"detail": "..."}` shape 유지 (`common/errors.ts`).

progress 파일 경로는 컨테이너 기준(`/workspace/data/...`)이라 host 실행 시
`WORKSPACE_DATA_DIR=<repo>/data`로 prefix 치환 (컨테이너 배포에선 미설정 = Go와 동일).

### ✅ 5단계 — read 경로 확대 2차: dataset versions 목록/상세 + worker 첫 위임 (2026-07-02)
- `GET /projects/{pid}/datasets/{did}/versions` / `GET .../versions/{vid}` 포팅
  (version numbering, clean/doc_genuineness/clause_label stage detail, summary normalize,
  clean_summary 계약, byte_size/original_filename).
- **source 프리뷰(row_count/columns)의 DuckDB 계산을 Python worker로 이동** (ADR-024 첫 적용):
  worker에 read-only task `POST /tasks/source_summary` 신설 (DuckDB + openpyxl, Go
  `loadDatasetSourceSummary` 대응). Node는 metadata `source_summary` 캐시 우선, 없으면
  (2026-06-26 이전 legacy 버전) worker 호출. Node는 DuckDB를 들고 있지 않다.
- **계약 발견 2건**:
  - `clean.completed_at`은 **컬럼 우선** — Go store `normalizeDatasetVersionCleanFields`가
    scan 직후 컬럼값을 metadata["cleaned_at"]에 덮어써서, service의 metadata-우선 로직이
    실질적으로 컬럼값을 본다 (컬럼 NULL일 때만 metadata 파싱).
  - metadata timestamp(doc_genuineness_completed_at 등)는 ns 정밀도 문자열 그대로 파싱되어
    ns로 응답된다 (컬럼은 µs) — goRfc3339가 정밀도를 보존해야 함.
- **parity 결과**: versions 목록 4 + 상세 5 + 404 6종 전부 일치. 기존 suite 회귀
  (projects/datasets 15, build jobs 98/98)도 재통과. Python worker 테스트 940 OK (신규 9).

### ⬜ 다음 (포팅 단계)
- artifact views (clean/doc_genuineness/clause_label view — 집계는 worker 이동,
  source_summary와 같은 패턴)
- build 트리거 엔드포인트 정식 포팅 (job insert + workflow start, 3단계 검증됨)
- Python worker HTTP 프록시 (analyze — 단순 HTTP, 저위험)
- reverse-proxy strangler 라우팅
- (후순위) Go temporal-worker(workflow+activities) → Node worker

## 구조
```
src/
  main.ts              # 엔트리(별도 포트)
  app.module.ts
  common/              # go-time(KST/µs parity), errors({"detail"} shape)
  db/db.module.ts      # Kysely + pg Pool (DI 토큰 'DB') + timestamptz raw text 파서
  health/              # de-risk 헬스체크
  projects/            # controller → service → repository (Go의 http→service→store 대체)
  datasets/            # GET 목록/상세
  build-jobs/          # GET 단건 (diagnostics 합성)
  versions/            # GET 목록/상세 (stage detail + summary normalize)
  worker/              # Python worker HTTP client (source_summary — ADR-024 위임)
```
