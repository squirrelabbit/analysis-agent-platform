-- 0002_drop_resumed_execution_count.sql
--
-- 목적: dataset_build_jobs.resumed_execution_count 컬럼 제거.
--   δ-3(2026-05-21) executions 테이블 drop 잔재로, 코드에서 read/write하지 않는다(항상 0).
--
-- silverone 2026-06-04 — 이 정리는 control-plane boot path(ensureSchema)에서 제거됐다
-- (boot-time DROP COLUMN = destructive 스키마 변경). 운영 코드는 자동 실행하지 않으며
-- **operator가 명시적으로 1회 수동 실행**한다. 컬럼이 남아 있어도 미사용이라 무해하다.
--
-- ⚠️ compose up / 서버 부팅 / CI에 묶지 않는다. 자동 실행 금지.
-- ⚠️ DROP COLUMN은 해당 컬럼 데이터를 영구 삭제한다(이 컬럼은 의미 없는 값이라 손실 없음).
--
-- 실행:
--   psql -U platform -d analysis_support -f scripts/migrations/0002_drop_resumed_execution_count.sql
--   (compose) docker compose -f compose.dev.yml exec -T postgres \
--     psql -U platform -d analysis_support -f - < scripts/migrations/0002_drop_resumed_execution_count.sql

-- [1] 사전 확인 (DROP 전 컬럼 존재 여부). 0 rows면 이미 정리됨 → [2] 건너뛰어도 된다.
SELECT
    'dataset_build_jobs' AS table_name,
    'resumed_execution_count' AS column_name,
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dataset_build_jobs'
          AND column_name = 'resumed_execution_count'
    ) AS column_exists;

-- [2] 정리 실행 (idempotent — IF EXISTS라 없으면 no-op).
BEGIN;
ALTER TABLE dataset_build_jobs DROP COLUMN IF EXISTS resumed_execution_count;
COMMIT;

-- [3] 사후 확인 (column_exists=false 면 완료).
SELECT
    'dataset_build_jobs' AS table_name,
    'resumed_execution_count' AS column_name,
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dataset_build_jobs'
          AND column_name = 'resumed_execution_count'
    ) AS still_exists;
