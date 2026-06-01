-- =============================================================================
-- dataset_versions β2 deprecated 15 컬럼 제거 마이그레이션
-- silverone 2026-06-01
-- =============================================================================
--
-- 배경:
--   ADR-018 β2 (5/19)로 prepare / sentiment / embedding 단계가 사라졌고,
--   이미 다음 후속 commit으로 코드 의존이 정리됨:
--     - 8f7ca93e refactor: 미사용 컬럼 의존 제거
--         (Go DatasetVersion struct 15 필드 + store INSERT/UPDATE/SELECT/scan 정리)
--     - d61eff98 fix: dataset_versions deprecated 컬럼 기본값 보정
--         (embedding_status DEFAULT 'not_requested' 보정 — 본 PR에서 ensureSchema에서 함께 제거)
--   본 SQL은 코드 정리 완료된 시점에서 DB row footprint를 실제로 제거한다.
--
-- 실행 정책:
--   * 본 파일은 ensureSchema에 포함되지 않는다 (boot-time destructive migration 금지).
--   * Operator가 명시 실행한다 — 백업/preflight를 반드시 먼저 수행.
--   * 단일 트랜잭션 — 중간 실패 시 ROLLBACK으로 원복.
--
-- =============================================================================
-- 사전 백업 (operator가 본 SQL 실행 전 반드시 수행)
-- =============================================================================
--
-- 옵션 A — pg_dump (권장):
--   pg_dump --table=public.dataset_versions --data-only --format=plain \
--     postgresql://user:pass@host:5432/dbname \
--     > /tmp/dataset_versions_backup_$(date +%Y%m%d_%H%M%S).sql
--
-- 옵션 B — 같은 DB 안의 backup 테이블 (소량 dataset 환경):
--   CREATE TABLE dataset_versions_backup_20260601 AS
--     SELECT * FROM dataset_versions;
--
-- =============================================================================
-- Rollback 절차
-- =============================================================================
--
--   ALTER TABLE은 DDL이므로 자동 rollback이 안 된다. 컬럼 복원은 다음 중 하나:
--   1) 옵션 A 백업: pg_restore 또는 psql -f backup.sql
--      ※ 백업 sql은 INSERT data only — 컬럼이 사라진 상태에서는 import 실패.
--        먼저 ALTER TABLE ADD COLUMN으로 컬럼 재생성 후 import. 옛 코드에서
--        ensureSchema의 ADD COLUMN 라인 복원하는 게 가장 빠름.
--   2) 옵션 B 백업 테이블이 있으면:
--      ALTER TABLE dataset_versions ADD COLUMN prepare_status TEXT NOT NULL DEFAULT 'not_requested';
--      ... (15 컬럼 모두)
--      UPDATE dataset_versions dv
--        SET prepare_status = b.prepare_status, ... (15 컬럼)
--        FROM dataset_versions_backup_20260601 b
--        WHERE dv.dataset_version_id = b.dataset_version_id;
--
--   둘 다 row data가 의미 없는 NULL/default 상태였으므로 실용적 복원 가치는 낮다.
--   진짜 위험은 코드/응답에서 의존이 살아 있는데 컬럼만 사라진 경우. 본 SQL
--   실행 전 §preflight의 grep 재확인을 반드시 수행한다.
--
-- =============================================================================
-- §preflight — 실행 전 확인 (Operator가 SELECT 결과를 눈으로 검증)
-- =============================================================================
--
-- 1. 코드 의존 grep — 반드시 0건 출력
--    grep -rn "PrepareStatus\|PrepareLLMMode\|PrepareModel\|PreparePromptVer\|PrepareURI\|PreparedAt\|PrepareSummary\|SentimentStatus\|SentimentLLMMode\|SentimentModel\|SentimentURI\|SentimentLabeledAt\|SentimentPromptVer\|EmbeddingStatus\|EmbeddingModel\|EmbeddingURI" apps/control-plane/internal --include="*.go" | grep -v "_test.go"
--    grep -rn "\.\.\.\\.prepare_status\\|prepare_uri\\|sentiment_status\\|sentiment_uri\\|embedding_status\\|embedding_uri" workers/python-ai/src
--    (Python runtime fallback도 점검 — PR4에서 정식 정리 예정)
--
-- 2. 컬럼 존재 + row 분포 확인 (모두 NULL 또는 default여야 정상):
--    SELECT column_name, is_nullable, column_default
--    FROM information_schema.columns
--    WHERE table_name = 'dataset_versions'
--      AND column_name = ANY(ARRAY[
--        'prepare_status','prepare_llm_mode','prepare_model','prepare_prompt_version','prepare_uri','prepared_at',
--        'sentiment_status','sentiment_llm_mode','sentiment_model','sentiment_uri','sentiment_labeled_at','sentiment_prompt_version',
--        'embedding_status','embedding_model','embedding_uri'
--      ])
--    ORDER BY column_name;
--
--    SELECT COUNT(*) AS total_row_count FROM dataset_versions;
--
--    SELECT
--      COUNT(*) FILTER (WHERE prepare_status NOT IN ('not_requested','')) AS prepare_status_nondefault,
--      COUNT(*) FILTER (WHERE prepare_uri IS NOT NULL) AS prepare_uri_filled,
--      COUNT(*) FILTER (WHERE sentiment_status NOT IN ('not_requested','')) AS sentiment_status_nondefault,
--      COUNT(*) FILTER (WHERE sentiment_uri IS NOT NULL) AS sentiment_uri_filled,
--      COUNT(*) FILTER (WHERE embedding_status NOT IN ('not_requested','')) AS embedding_status_nondefault,
--      COUNT(*) FILTER (WHERE embedding_uri IS NOT NULL) AS embedding_uri_filled
--    FROM dataset_versions;
--
--    nondefault / filled가 모두 0이면 안전하게 DROP 가능.
--    0이 아닌 경우 — 운영 중 옛 row가 남아 있다는 신호. 데이터 가치 확인 후
--    별도 처리(metadata jsonb로 마이그레이션 등) 결정.
--
-- =============================================================================
-- §main — 트랜잭션 안에서 DROP COLUMN × 15
-- =============================================================================

BEGIN;

-- 코드 정리 commit (8f7ca93e) 이후 의존 0건 가정. ROLLBACK 시 위 §rollback 참조.

ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepare_status;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepare_llm_mode;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepare_model;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepare_prompt_version;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepare_uri;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS prepared_at;

ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_status;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_llm_mode;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_model;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_uri;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_labeled_at;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS sentiment_prompt_version;

ALTER TABLE dataset_versions DROP COLUMN IF EXISTS embedding_status;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS embedding_model;
ALTER TABLE dataset_versions DROP COLUMN IF EXISTS embedding_uri;

-- =============================================================================
-- §verify — 트랜잭션 안에서 검증 (실패면 ROLLBACK 하라)
-- =============================================================================

-- 15 컬럼이 모두 사라졌는지 확인 — 결과 0건이어야 한다
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'dataset_versions'
  AND column_name = ANY(ARRAY[
    'prepare_status','prepare_llm_mode','prepare_model','prepare_prompt_version','prepare_uri','prepared_at',
    'sentiment_status','sentiment_llm_mode','sentiment_model','sentiment_uri','sentiment_labeled_at','sentiment_prompt_version',
    'embedding_status','embedding_model','embedding_uri'
  ]);

-- dataset_versions row_count 변화 없음 확인 — 위 preflight 결과와 같아야 한다
SELECT COUNT(*) AS total_row_count FROM dataset_versions;

-- 살아 있는 컬럼만 남은 schema 확인
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'dataset_versions'
ORDER BY ordinal_position;

COMMIT;

-- =============================================================================
-- §post — 실행 후 검증 (control-plane이 정상 동작하는지)
-- =============================================================================
--
-- 1. control-plane 재시작 후 startup error 없는지 — ensureSchema의 15 컬럼
--    참조 라인은 본 PR에서 함께 제거됨. ADD COLUMN IF NOT EXISTS가 컬럼을
--    다시 만들지 않는다.
--
-- 2. API smoke:
--      curl -s http://127.0.0.1:18080/projects/{pid}/datasets/{did}/versions/{vid} | jq
--      curl -s http://127.0.0.1:18080/projects/{pid}/datasets/{did}/versions      | jq
--
--    응답에 prepare_* / sentiment_* / embedding_* 키 없음 + clean /
--    doc_genuineness / clause_label 정상 노출 확인.
--
-- 3. analyze smoke 4/4 PASS:
--      scripts/smoke_analyze_endpoint.sh
--
-- =============================================================================
