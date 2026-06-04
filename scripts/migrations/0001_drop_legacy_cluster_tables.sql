-- 0001_drop_legacy_cluster_tables.sql
--
-- 목적: ADR-018 β2 (5/19) document_cluster_profile 제거 후 남는 staging 테이블
--   - dataset_version_cluster_confirmations
--   - dataset_version_cluster_profile_builds
-- 를 정리한다.
--
-- silverone 2026-06-04 (Codex review #4) — 이 정리는 control-plane boot path
-- (ensureSchema)에서 제거됐다. 운영 코드는 destructive cleanup을 자동 실행하지
-- 않는다. **operator가 명시적으로 1회 수동 실행**한다.
--
-- ⚠️ DROP TABLE ... CASCADE는 운영 데이터를 영구 삭제한다. 실행 전 백업 정책을
--    확인하고, 아래 [1] 사전 확인 SELECT로 대상 테이블 존재/행 수를 먼저 본다.
--
-- ⚠️ compose up / 서버 부팅에 묶지 않는다. 자동 실행 금지.
--
-- 실행:
--   psql -U platform -d analysis_support -f scripts/migrations/0001_drop_legacy_cluster_tables.sql
--
-- (compose dev 환경에서 컨테이너로 실행하려면)
--   docker compose -f compose.dev.yml exec -T postgres \
--     psql -U platform -d analysis_support -f - < scripts/migrations/0001_drop_legacy_cluster_tables.sql

-- ────────────────────────────────────────────────────────────────────────────
-- [1] 사전 확인 (DROP 전에 먼저 실행해 대상 존재 여부와 규모를 확인한다)
--     존재하지 않으면 0 rows — 이미 정리됐다는 뜻이므로 [2]를 건너뛰어도 된다.
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    wanted.relname                                            AS table_name,
    to_regclass('public.' || wanted.relname) IS NOT NULL      AS exists,
    pg_total_relation_size(to_regclass('public.' || wanted.relname)) AS total_bytes
FROM (VALUES
    ('dataset_version_cluster_confirmations'),
    ('dataset_version_cluster_profile_builds')
) AS wanted(relname);

-- ────────────────────────────────────────────────────────────────────────────
-- [2] 정리 실행 (idempotent — IF EXISTS라 이미 없으면 no-op)
--     CASCADE: 위 테이블을 참조하는 FK/뷰가 있으면 함께 제거된다.
-- ────────────────────────────────────────────────────────────────────────────
BEGIN;

DROP TABLE IF EXISTS dataset_version_cluster_confirmations CASCADE;
DROP TABLE IF EXISTS dataset_version_cluster_profile_builds CASCADE;

COMMIT;

-- ────────────────────────────────────────────────────────────────────────────
-- [3] 사후 확인 (두 행 모두 exists=false 면 정리 완료)
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    relname AS table_name,
    to_regclass('public.' || relname) IS NOT NULL AS still_exists
FROM (VALUES
    ('dataset_version_cluster_confirmations'),
    ('dataset_version_cluster_profile_builds')
) AS wanted(relname);
