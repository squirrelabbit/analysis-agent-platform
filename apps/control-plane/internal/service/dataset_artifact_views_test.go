package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// silverone 2026-05-28 (옵션 A) — loadDocGenuinenessArtifact에 cleaned.parquet
// JOIN 분기 추가. cleanRef 유무 / join miss safe / 컬럼 schema 잠금.

func TestLoadDocGenuinenessArtifactWithCleanedTextJoin(t *testing.T) {
	jsonlPath, parquetPath := setupDocGenuinenessFixture(t, []string{"v:row:0", "v:row:1"}, []string{"v:row:0", "v:row:1"})

	summary, prompt, total, items, err := loadDocGenuinenessArtifact(jsonlPath, parquetPath, 10, 0, "v")
	if err != nil {
		t.Fatalf("loadDocGenuinenessArtifact: %v", err)
	}
	if total != 2 {
		t.Errorf("total: want 2, got %d", total)
	}
	if prompt != "v1" {
		t.Errorf("prompt: want v1, got %q", prompt)
	}
	if _, ok := summary["genuineness"]; !ok {
		t.Errorf("summary.genuineness missing")
	}
	if len(items) != 2 {
		t.Fatalf("items: want 2, got %d", len(items))
	}

	// item schema: doc_id / genuineness / reason / source / cleaned_text 5 필드.
	want := []string{"doc_id", "genuineness", "reason", "source", "cleaned_text"}
	for _, k := range want {
		if _, ok := items[0][k]; !ok {
			t.Errorf("item[0] missing key %q (full: %v)", k, items[0])
		}
	}
	// cleaned_text 값이 fixture와 일치.
	if items[0]["cleaned_text"] != "doc 0 text" {
		t.Errorf("items[0].cleaned_text: want %q, got %v", "doc 0 text", items[0]["cleaned_text"])
	}
	if items[1]["cleaned_text"] != "doc 1 body" {
		t.Errorf("items[1].cleaned_text: want %q, got %v", "doc 1 body", items[1]["cleaned_text"])
	}
}

func TestLoadDocGenuinenessArtifactWithoutCleanRefFallsBackToLegacySchema(t *testing.T) {
	jsonlPath, _ := setupDocGenuinenessFixture(t, []string{"v:row:0"}, []string{"v:row:0"})

	_, _, total, items, err := loadDocGenuinenessArtifact(jsonlPath, "", 10, 0, "v")
	if err != nil {
		t.Fatalf("loadDocGenuinenessArtifact: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("expected total=1 items=1, got total=%d items=%d", total, len(items))
	}
	if _, ok := items[0]["cleaned_text"]; ok {
		t.Errorf("legacy schema must not contain cleaned_text when cleanRef is empty")
	}
	want := []string{"doc_id", "genuineness", "reason", "source"}
	for _, k := range want {
		if _, ok := items[0][k]; !ok {
			t.Errorf("legacy item missing key %q", k)
		}
	}
}

func TestLoadDocGenuinenessArtifactJoinMissKeepsItemWithNullCleanedText(t *testing.T) {
	// jsonl에는 2개 doc_id, parquet에는 1개만 — 두 번째 doc은 본문 join miss.
	jsonlPath, parquetPath := setupDocGenuinenessFixture(t,
		[]string{"v:row:0", "v:row:99"}, // jsonl doc_ids
		[]string{"v:row:0"},              // parquet row_ids (v:row:99는 없음)
	)

	_, _, total, items, err := loadDocGenuinenessArtifact(jsonlPath, parquetPath, 10, 0, "v")
	if err != nil {
		t.Fatalf("loadDocGenuinenessArtifact must not fail on join miss: %v", err)
	}
	if total != 2 {
		t.Errorf("total must be 2 (jsonl base), got %d", total)
	}
	if len(items) != 2 {
		t.Fatalf("items: want 2 (miss row preserved), got %d", len(items))
	}

	// 첫 row는 cleaned_text 있어야 하고, 두 번째 row는 nil.
	if items[0]["cleaned_text"] != "doc 0 text" {
		t.Errorf("items[0].cleaned_text: want %q, got %v", "doc 0 text", items[0]["cleaned_text"])
	}
	if items[1]["cleaned_text"] != nil {
		t.Errorf("items[1].cleaned_text (join miss): want nil, got %v", items[1]["cleaned_text"])
	}

	// schema 5 필드 보장(miss row도).
	want := []string{"doc_id", "genuineness", "reason", "source", "cleaned_text"}
	for _, k := range want {
		if _, ok := items[1][k]; !ok {
			t.Errorf("join-miss item missing key %q", k)
		}
	}
}

// setupDocGenuinenessFixture — DuckDB로 임시 jsonl + parquet 생성. jsonlIDs와
// parquetIDs는 서로 다를 수 있어 join miss 시나리오를 만들 수 있다.
func setupDocGenuinenessFixture(t *testing.T, jsonlIDs, parquetIDs []string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "dg.jsonl")
	parquetPath := filepath.Join(dir, "clean.parquet")

	// jsonl 직접 작성.
	lines := []string{}
	for i, id := range jsonlIDs {
		genuineness := "genuine_review"
		reason := "valid review text"
		if i%2 == 1 {
			genuineness = "non_review"
			reason = "ad post"
		}
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":"%s","genuineness":"%s","reason":"%s","source":"lloa","prompt_version":"v1"}`,
			id, genuineness, reason,
		))
	}
	if err := os.WriteFile(jsonlPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	// parquet — DuckDB로 row_id + cleaned_text 두 컬럼.
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		t.Fatalf("openTempDuckDB: %v", err)
	}
	defer cleanup()

	selects := []string{}
	for i, id := range parquetIDs {
		body := fmt.Sprintf("doc %d %s", i, parquetBodyFor(i))
		selects = append(selects, fmt.Sprintf("SELECT '%s' AS row_id, '%s' AS cleaned_text", id, body))
	}
	if len(selects) == 0 {
		// parquet 빈 case 시도하지 않음 — DuckDB COPY가 schema 강제 필요.
		// 본 helper는 최소 1 row 보장.
		selects = append(selects, "SELECT NULL::VARCHAR AS row_id, NULL::VARCHAR AS cleaned_text WHERE 1=0")
	}
	query := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", strings.Join(selects, " UNION ALL "), parquetPath)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("write parquet: %v\nquery: %s", err, query)
	}
	return jsonlPath, parquetPath
}

// parquetBodyFor — fixture가 doc index별 다른 cleaned_text를 가지도록 한다.
func parquetBodyFor(i int) string {
	switch i {
	case 0:
		return "text"
	case 1:
		return "body"
	default:
		return "extra"
	}
}
