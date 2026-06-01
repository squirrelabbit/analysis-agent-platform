-- 2026-05-21: aspect_taxonomy / garbage_rule / stopwords 자산 endpoint
-- + service + store + domain 정리에 따른 9 테이블 + 인덱스 drop.
-- 자산별 (자료/defaults/changes) × 3 자산 = 9 테이블.
--
-- 사전 점검:
--   SELECT COUNT(*) FROM project_aspect_taxonomies;
--   SELECT COUNT(*) FROM project_aspect_taxonomy_defaults;
--   SELECT COUNT(*) FROM project_aspect_taxonomy_changes;
--   SELECT COUNT(*) FROM project_garbage_rules;
--   SELECT COUNT(*) FROM project_garbage_rule_defaults;
--   SELECT COUNT(*) FROM project_garbage_rule_changes;
--   SELECT COUNT(*) FROM project_stopwords;
--   SELECT COUNT(*) FROM project_stopwords_defaults;
--   SELECT COUNT(*) FROM project_stopwords_changes;
-- 위 9 테이블에 데이터가 있으면 백업 후 진행할 것 (pg_dump 권장).
--
-- 실행: psql -U platform -d analysis_support -f scripts/migrate_drop_legacy_project_assets.sql

BEGIN;

DROP INDEX IF EXISTS project_aspect_taxonomy_changes_idx;
DROP INDEX IF EXISTS project_garbage_rule_changes_idx;
DROP INDEX IF EXISTS project_stopwords_changes_idx;

DROP TABLE IF EXISTS project_aspect_taxonomy_changes;
DROP TABLE IF EXISTS project_aspect_taxonomy_defaults;
DROP TABLE IF EXISTS project_aspect_taxonomies;

DROP TABLE IF EXISTS project_garbage_rule_changes;
DROP TABLE IF EXISTS project_garbage_rule_defaults;
DROP TABLE IF EXISTS project_garbage_rules;

DROP TABLE IF EXISTS project_stopwords_changes;
DROP TABLE IF EXISTS project_stopwords_defaults;
DROP TABLE IF EXISTS project_stopwords;

COMMIT;
