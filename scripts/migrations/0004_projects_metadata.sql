-- 0004_projects_metadata.sql
--
-- 목적: projects 테이블에 프로젝트 레벨 메타데이터(JSONB) 컬럼을 추가한다(#31, 2026-07-01).
--   축제 메타(metadata.festival)의 단일 source. dataset.metadata와 동일 패턴.
--
-- silverone 2026-07-01 — control-plane ensureSchema가 부팅 시 동일한 ADD COLUMN IF NOT
-- EXISTS를 멱등 실행하므로, 이 파일은 *기록/수동 실행용*이다. 신규 배포는 자동 반영된다.
--
-- ⚠️ 멱등(이미 있으면 no-op). DEFAULT '{}'로 기존 row는 빈 메타로 백필된다.
-- 실행:
--   psql "$DATABASE_URL" -f scripts/migrations/0004_projects_metadata.sql

ALTER TABLE projects ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
