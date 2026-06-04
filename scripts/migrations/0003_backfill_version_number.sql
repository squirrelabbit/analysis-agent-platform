-- 0003_backfill_version_number.sql
--
-- 목적: feat/version-number 이전에 생성된 dataset_versions에 metadata.version_number를
--   생성순(created_at) 1-based로 부여한다. 신규 버전은 control-plane이 생성 시점에
--   부여하므로, 이 migration은 *legacy row*만 영구화한다.
--
-- silverone 2026-06-04 — 미실행 상태에서도 API는 read-time fallback(created_at rank)으로
-- version_number를 노출하지만, 그 fallback은 삭제 시 흔들릴 수 있다. 이 migration을 1회
-- 실행하면 legacy 버전 번호도 stored되어 삭제와 무관하게 고정된다.
--
-- ⚠️ operator-run. compose up / 부팅 / CI에 묶지 않는다. 멱등(이미 있는 값은 보존).
-- 실행:
--   psql -U platform -d analysis_support -f scripts/migrations/0003_backfill_version_number.sql
--   (compose) docker compose -f compose.dev.yml exec -T postgres \
--     psql -U platform -d analysis_support -f - < scripts/migrations/0003_backfill_version_number.sql

-- [1] 사전 확인: version_number 없는 row 수(dataset별).
SELECT dataset_id, COUNT(*) AS missing_version_number
FROM dataset_versions
WHERE metadata->>'version_number' IS NULL
GROUP BY dataset_id
ORDER BY dataset_id;

-- [2] 백필: dataset별 created_at ASC rank를 version_number 없는 row에만 기록.
--     기존 stored 값은 보존(WHERE ... IS NULL). rank는 *모든* sibling 기준이라
--     이미 번호가 있는 버전과 충돌하지 않는다.
BEGIN;
WITH ranked AS (
    SELECT dataset_version_id,
           ROW_NUMBER() OVER (
               PARTITION BY dataset_id
               ORDER BY created_at ASC, dataset_version_id ASC
           ) AS n
    FROM dataset_versions
)
UPDATE dataset_versions dv
SET metadata = COALESCE(dv.metadata, '{}'::jsonb)
               || jsonb_build_object('version_number', ranked.n)
FROM ranked
WHERE dv.dataset_version_id = ranked.dataset_version_id
  AND dv.metadata->>'version_number' IS NULL;
COMMIT;

-- [3] 사후 확인: 남은 NULL 0건이어야 한다.
SELECT COUNT(*) AS still_missing
FROM dataset_versions
WHERE metadata->>'version_number' IS NULL;
