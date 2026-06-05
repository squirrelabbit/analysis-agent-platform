package service

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// silverone 2026-06-05 — clause_label 결과 표 서버 필터(aspect/sentiment) + 페이징 잠금.
// summary(차트)는 전체 분포 유지, items/total은 필터 반영.
func setupClauseLabelFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "clause_label.jsonl")
	lines := []string{
		`{"doc_id":"d1","clause":"c1","sentiment":"positive","aspect":"price","source":"s","prompt_version":"v3"}`,
		`{"doc_id":"d1","clause":"c2","sentiment":"negative","aspect":"service","source":"s","prompt_version":"v3"}`,
		`{"doc_id":"d2","clause":"c3","sentiment":"positive","aspect":"price","source":"s","prompt_version":"v3"}`,
		`{"doc_id":"d2","clause":"c4","sentiment":"neutral","aspect":"food","source":"s","prompt_version":"v3"}`,
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}
	return path
}

func joinLines(lines []string) string {
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

func TestLoadClauseLabelArtifact_NoFilter(t *testing.T) {
	path := setupClauseLabelFixture(t)
	summary, prompt, total, items, err := loadClauseLabelArtifact(path, 10, 0, "", "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if len(items) != 4 {
		t.Fatalf("items = %d, want 4", len(items))
	}
	if prompt != "v3" {
		t.Fatalf("prompt = %q, want v3", prompt)
	}
	if summary["total"] != 4 {
		t.Fatalf("summary.total = %v, want 4", summary["total"])
	}
}

func TestLoadClauseLabelArtifact_SentimentFilter(t *testing.T) {
	path := setupClauseLabelFixture(t)
	summary, _, total, items, err := loadClauseLabelArtifact(path, 10, 0, "", "positive")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// 필터된 total = positive 2건.
	if total != 2 {
		t.Fatalf("filtered total = %d, want 2", total)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	for _, it := range items {
		if fmt.Sprint(it["sentiment"]) != "positive" {
			t.Fatalf("item sentiment = %v, want positive", it["sentiment"])
		}
	}
	// summary는 전체 분포 유지 (필터 무관).
	if summary["total"] != 4 {
		t.Fatalf("summary.total must stay 4 (full), got %v", summary["total"])
	}
}

func TestLoadClauseLabelArtifact_AspectAndSentimentFilter(t *testing.T) {
	path := setupClauseLabelFixture(t)
	_, _, total, items, err := loadClauseLabelArtifact(path, 10, 0, "price", "positive")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// price + positive = c1, c3 → 2건.
	if total != 2 || len(items) != 2 {
		t.Fatalf("price+positive total=%d items=%d, want 2/2", total, len(items))
	}
	for _, it := range items {
		if fmt.Sprint(it["aspect"]) != "price" || fmt.Sprint(it["sentiment"]) != "positive" {
			t.Fatalf("item mismatch: aspect=%v sentiment=%v", it["aspect"], it["sentiment"])
		}
	}
}

func TestLoadClauseLabelArtifact_Pagination(t *testing.T) {
	path := setupClauseLabelFixture(t)
	_, _, total, items, err := loadClauseLabelArtifact(path, 1, 1, "", "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if len(items) != 1 {
		t.Fatalf("limit=1 → items = %d, want 1", len(items))
	}
}
