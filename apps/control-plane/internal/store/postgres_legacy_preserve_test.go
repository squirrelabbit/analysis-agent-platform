package store

import (
	"os"
	"strings"
	"testing"
)

// silverone 2026-05-27 (Codex adversarial review fix-3) — ensureSchema가
// boot-time에 옛 legacy 테이블 4종을 DROP하지 못하게 잠근다.
// 운영/감사 이력이 들어있을 수 있는 테이블을 자동 startup에서 지우면 안 된다.
// destructive cleanup이 필요하면 별도 operator-run migration으로 분리.

func TestEnsureSchema_DoesNotDropLegacyTables(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatalf("read postgres.go: %v", err)
	}
	source := string(data)

	for _, table := range []string{"report_drafts", "executions", "skill_plans", "analysis_requests"} {
		pattern := "DROP TABLE IF EXISTS " + table
		if strings.Contains(source, pattern) {
			t.Fatalf(
				"ensureSchema must not DROP legacy table %q on boot. Found %q in postgres.go. "+
					"destructive cleanup belongs in an operator-run migration with backup/rollback, "+
					"not in automatic schema setup.",
				table, pattern,
			)
		}
	}
}
