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

// silverone 2026-06-05 — summary.aspect_sentiment 교차 분포(count/percent) 잠금.
// 필터와 무관하게 항상 전체 기준. 고정 sentiment 3종은 0으로 채워진다.
func TestLoadClauseLabelArtifact_AspectSentimentSummary(t *testing.T) {
	path := setupClauseLabelFixture(t)
	// 필터를 걸어도 summary.aspect_sentiment는 전체 분포여야 한다.
	summary, _, _, _, err := loadClauseLabelArtifact(path, 10, 0, "price", "positive")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	as, ok := summary["aspect_sentiment"].(map[string]any)
	if !ok {
		t.Fatalf("summary.aspect_sentiment type = %T, want map[string]any", summary["aspect_sentiment"])
	}

	// price: total 2, positive 2 (100%), negative/neutral 0.
	price, ok := as["price"].(map[string]any)
	if !ok {
		t.Fatalf("aspect_sentiment.price missing: %v", as)
	}
	if price["total"] != 2 {
		t.Fatalf("price.total = %v, want 2", price["total"])
	}
	priceSent := price["sentiment"].(map[string]any)
	if got := priceSent["positive"].(map[string]any); got["count"] != 2 || got["percent"] != 100.0 {
		t.Fatalf("price.positive = %v, want count 2 percent 100", got)
	}
	// 관측 안 된 표준 sentiment도 0으로 채워져야 한다.
	if got := priceSent["negative"].(map[string]any); got["count"] != 0 || got["percent"] != 0.0 {
		t.Fatalf("price.negative = %v, want count 0 percent 0", got)
	}
	if _, ok := priceSent["neutral"]; !ok {
		t.Fatalf("price.sentiment must zero-fill neutral, got %v", priceSent)
	}

	// service: total 1, negative 1 (100%).
	service := as["service"].(map[string]any)
	if service["total"] != 1 {
		t.Fatalf("service.total = %v, want 1", service["total"])
	}
	if got := service["sentiment"].(map[string]any)["negative"].(map[string]any); got["count"] != 1 || got["percent"] != 100.0 {
		t.Fatalf("service.negative = %v, want count 1 percent 100", got)
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
