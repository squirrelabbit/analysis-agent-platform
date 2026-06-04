package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

// silverone 2026-05-27 (Codex adversarial review fix-3) / 2026-06-04 강화 —
// boot path(postgres.go)는 부팅 시 자동으로 데이터/스키마를 파괴하면 안 된다.
// destructive cleanup이 필요하면 operator-run migration(scripts/migrations/)으로 옮긴다.

//  1. DROP TABLE — 파일 전체에서 차단(런타임 CRUD에도 DROP TABLE은 등장하지 않으므로
//     whole-file로 잡아도 false positive 없음). 주석의 "DROP TABLE로 제거"는 공백 없어 제외.
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
					"부팅 시 자동 테이블 삭제 금지. operator-run migration으로 옮겨라.",
				i+1, trimmed,
			)
		}
	}
}

// boot path 함수 — startup 시 ensureSchema가 직접/간접 실행한다. 새 boot helper가
// 생기면 여기에 추가한다(이름 drift는 아래 존재 확인 테스트가 잡는다).
var bootSchemaFuncs = map[string]bool{
	"ensureSchema":                         true,
	"promoteTimestampColumnsToTimestamptz": true,
	"backfillDatasetVersionArtifacts":      true,
}

// boot path SQL에서 추적·차단하는 mutation. SQL string literal만 스캔하므로 주석/트리거
// DDL("OR UPDATE OF")의 오탐이 없다. DELETE/UPDATE는 statement 시작(^) 기준.
//   - DROP TABLE / TRUNCATE / DELETE FROM : 항상 차단(허용 0).
//   - ALTER TABLE ... DROP : 컬럼/제약 파괴 → 차단. 단 아래 allowlist는 예외.
//   - UPDATE backfill : 당장 유지하되 allowlist로 명시 관리. 새 UPDATE는 차단.
var (
	reDropTable = regexp.MustCompile(`(?i)\bDROP\s+TABLE\b`)
	reTruncate  = regexp.MustCompile(`(?i)\bTRUNCATE\b`)
	reDeleteDML = regexp.MustCompile(`(?i)\bDELETE\s+FROM\b`)
	reAlterHead = regexp.MustCompile(`(?i)^ALTER\s+TABLE\b`)
	reDropWord  = regexp.MustCompile(`(?i)\bDROP\b`)
	reUpdateDML = regexp.MustCompile(`(?i)^UPDATE\s`)
)

// 현재 boot path에 존재하는 mutation 중 "당장 제거하지 않고 유지"하기로 한 것들.
// 새 mutation은 여기 없으면 테스트가 막는다. (substring 매칭 — whitespace 변화에
// 견디도록 statement의 식별 가능한 앞부분만 기재.)
//
// ⚠️ 이 allowlist는 *영구 허용이 아니라 migration 이전까지의 임시 예외*다.
//   - resumed_execution_count DROP COLUMN은 operator-run migration으로 분리 완료
//     (scripts/migrations/0002_drop_resumed_execution_count.sql, ensureSchema에서 제거).
//   - 남은 UPDATE backfill 2건도 동일하게 migration 이전 후보다 — 후속 MR에서 operator
//     migration으로 분리하고 ensureSchema와 이 allowlist에서 함께 제거한다.
var bootMutationAllowlist = []string{
	// 멱등 backfill (β2 cleanup) — dataset_version active/clean 메타 보정. (migration 이전 후보)
	"UPDATE dataset_versions",
	"UPDATE datasets d",
}

// bootMutationKind — SQL literal이 추적 대상 mutation이면 그 종류를, 아니면 ""를 반환.
func bootMutationKind(sql string) string {
	switch {
	case reDropTable.MatchString(sql):
		return "DROP TABLE"
	case reTruncate.MatchString(sql):
		return "TRUNCATE"
	case reDeleteDML.MatchString(sql):
		return "DELETE"
	case reAlterHead.MatchString(sql) && reDropWord.MatchString(sql):
		return "ALTER ... DROP"
	case reUpdateDML.MatchString(sql):
		return "UPDATE"
	}
	return ""
}

// bootMutationAllowed — allowlist(현재 유지 결정한 mutation)에 해당하면 true.
func bootMutationAllowed(sql string) bool {
	for _, a := range bootMutationAllowlist {
		if strings.Contains(sql, a) {
			return true
		}
	}
	return false
}

func TestEnsureSchema_NoDestructiveBootMutation(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "postgres.go", nil, 0)
	if err != nil {
		t.Fatalf("parse postgres.go: %v", err)
	}

	seen := map[string]bool{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !bootSchemaFuncs[fn.Name.Name] {
			continue
		}
		seen[fn.Name.Name] = true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			sql := normalizeSQLLiteral(lit.Value)
			k := bootMutationKind(sql)
			if k == "" || bootMutationAllowed(sql) {
				return true
			}
			snippet := sql
			if len(snippet) > 120 {
				snippet = snippet[:120] + "…"
			}
			t.Errorf(
				"boot path %q에 destructive/관리대상 mutation(%s)이 있다:\n  %s\n"+
					"부팅 시 자동 데이터/스키마 변경 금지. operator-run migration으로 옮겨라"+
					" (유지가 필요하면 bootMutationAllowlist에 명시).",
				fn.Name.Name, k, snippet,
			)
			return true
		})
	}

	for name := range bootSchemaFuncs {
		if !seen[name] {
			t.Errorf("boot func %q를 postgres.go에서 못 찾음 — 이름 변경/삭제 시 bootSchemaFuncs 갱신 필요.", name)
		}
	}
}

// 가드가 no-op이 아님을 증명: 합성 SQL로 탐지/allowlist 동작을 잠근다.
func TestBootMutationDetection(t *testing.T) {
	// 차단되어야 하는 새 destructive (allowlist에 없음).
	blocked := map[string]string{
		"DROP TABLE foo":                    "DROP TABLE",
		"TRUNCATE foo":                      "TRUNCATE",
		"DELETE FROM foo WHERE x = 1":       "DELETE",
		"ALTER TABLE foo DROP COLUMN bar":   "ALTER ... DROP",
		"ALTER TABLE foo DROP CONSTRAINT c": "ALTER ... DROP",
		"UPDATE some_other_table SET x = 1": "UPDATE",
		// resumed_execution_count DROP은 operator migration으로 분리·allowlist 제거됨 →
		// 이제 boot에 재등장하면 차단되어야 한다.
		"ALTER TABLE dataset_build_jobs DROP COLUMN IF EXISTS resumed_execution_count": "ALTER ... DROP",
	}
	for sql, want := range blocked {
		if got := bootMutationKind(sql); got != want {
			t.Errorf("kind(%q)=%q, want %q", sql, got, want)
		}
		if bootMutationAllowed(sql) {
			t.Errorf("allowlist가 차단대상 %q를 허용함", sql)
		}
	}

	// 추적 대상이 아니어야 하는 non-destructive.
	for _, sql := range []string{
		"CREATE TABLE foo (id TEXT)",
		"ALTER TABLE foo ADD COLUMN bar TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE foo ALTER COLUMN bar TYPE TIMESTAMPTZ",
		"CREATE INDEX foo_idx ON foo(id)",
		"CREATE TRIGGER t BEFORE INSERT OR UPDATE OF x ON foo", // "UPDATE OF" 오탐 금지
	} {
		if got := bootMutationKind(sql); got != "" {
			t.Errorf("non-destructive %q가 mutation(%s)으로 탐지됨", sql, got)
		}
	}

	// 현재 allowlist된 기존 mutation은 허용되어야 한다 (UPDATE backfill 2건만 남음).
	for _, sql := range []string{
		"UPDATE dataset_versions SET clean_status = 'ready'",
		"UPDATE datasets d SET active_dataset_version_id = v.id",
	} {
		if bootMutationKind(sql) == "" {
			t.Errorf("allowlist 대상 %q가 mutation으로 탐지되지 않음(분류 누락)", sql)
		}
		if !bootMutationAllowed(sql) {
			t.Errorf("allowlist가 기존 mutation %q를 허용하지 않음", sql)
		}
	}
}

// normalizeSQLLiteral — Go string literal value(backtick/quote 포함)에서 따옴표를 벗기고
// 앞뒤 공백을 제거한다. statement 시작(^) 기준 검사를 위해 필요.
func normalizeSQLLiteral(v string) string {
	if len(v) >= 2 {
		if q := v[0]; (q == '`' || q == '"') && v[len(v)-1] == q {
			v = v[1 : len(v)-1]
		}
	}
	return strings.TrimSpace(v)
}
