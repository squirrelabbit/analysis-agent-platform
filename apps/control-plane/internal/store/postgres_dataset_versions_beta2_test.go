package store

import (
	"os"
	"strings"
	"testing"
)

// silverone 2026-06-01 (β2 cleanup PR3) — dataset_versions β2 deprecated 컬럼이
// scripts/operator/drop_dataset_versions_beta2_columns.sql 로 DROP된 뒤에도
// control-plane이 깨지지 않도록, postgres.go 가 해당 컬럼을 INSERT/SELECT/scan/
// timestamptz 승격 어디에서도 참조하지 않음을 잠근다.
//
// Save/Get/ListDatasetVersion 의 컬럼 제거는 PR2(2026-05-28), CREATE TABLE +
// ADD COLUMN 제거는 commit 0d5bf055(2026-06-01)에서 끝났다. 이 테스트는 그 결과가
// 되살아나지 않도록 source-text 로 고정한다 (실 DB 불필요 — legacy_preserve_test 와
// 동일한 grep-lock 방식).
//
// 주의: prepare_prompt_version / sentiment_prompt_version 는 project_prompt_defaults
// 테이블 컬럼, embedding_model / prepare_model / sentiment_model 은 다른 테이블 또는
// 요청 body 필드라 DROP 대상이 아니다. 따라서 dataset_versions 에만 존재하던 컬럼명만
// 검사한다.
func TestPostgres_DoesNotReferenceDroppedDatasetVersionColumns(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatalf("read postgres.go: %v", err)
	}
	// Go `//` 주석은 제거한 뒤 검사한다 — 설명 주석이 컬럼명을 언급하는 것은
	// 허용하고, 실제 코드/SQL 문자열 안의 참조만 잡는다. (검사 대상 컬럼명은
	// 모두 단순 식별자라 "http://" 같은 문자열 내 `//` 와 충돌하지 않는다.)
	var b strings.Builder
	for _, line := range strings.Split(string(data), "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	source := b.String()

	// dataset_versions 에만 있던 β2 DROP 대상 컬럼 (공유 컬럼명 제외).
	droppedColumns := []string{
		"prepare_status",
		"prepare_llm_mode",
		"prepare_uri",
		"prepared_at",
		"sentiment_status",
		"sentiment_llm_mode",
		"sentiment_uri",
		"sentiment_labeled_at",
		"embedding_status",
		"embedding_uri",
	}

	for _, column := range droppedColumns {
		if strings.Contains(source, column) {
			t.Fatalf(
				"postgres.go must not reference dropped dataset_versions column %q. "+
					"operator 가 drop_dataset_versions_beta2_columns.sql 를 실행하면 해당 컬럼이 "+
					"사라지므로 store read/write/scan/timestamptz-promote 경로에서 참조하면 안 된다. "+
					"단계 상태는 metadata(JSONB)/artifacts 기반으로 읽는다.",
				column,
			)
		}
	}
}
