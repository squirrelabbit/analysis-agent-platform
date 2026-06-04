package store

import (
	"os"
	"strings"
	"testing"
)

// silverone 2026-05-27 (Codex adversarial review fix-3) / 2026-06-04 강화
// (Codex review #4) — boot path(postgres.go)는 어떤 테이블도 DROP하지 못한다.
// 운영/감사 이력이 들어있을 수 있는 테이블을 자동 startup에서 지우면 안 된다.
// destructive cleanup이 필요하면 scripts/migrations/ 의 operator-run migration으로
// 분리한다 (특정 legacy 4종이 아니라 모든 DROP TABLE 차단으로 강화).
//
// 패턴은 "DROP TABLE " (trailing space)로 — 실제 SQL 문만 잡고, 한국어 주석의
// "DROP TABLE로 제거" 같은 표현(공백 없음)은 잡지 않는다.

func TestEnsureSchema_DoesNotDropAnyTable(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatalf("read postgres.go: %v", err)
	}

	for i, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue // Go 주석 라인은 검사 대상 아님
		}
		if strings.Contains(line, "DROP TABLE ") {
			t.Fatalf(
				"boot schema(postgres.go:%d)에 DROP TABLE 문이 있다: %q\n"+
					"부팅 시 자동 테이블 삭제는 금지. destructive cleanup은 "+
					"scripts/migrations/ 의 operator-run migration으로 분리한다.",
				i+1, trimmed,
			)
		}
	}
}
