package service

import (
	"fmt"
	"testing"
)

// silverone 2026-06-05 — doc_genuineness 결과 표 서버 필터(genuineness) + 페이징 잠금.
// summary(차트)는 전체 분포 유지, items/total은 필터 반영.
func TestLoadDocGenuinenessArtifact_GenuinenessFilter(t *testing.T) {
	// fixture는 i 짝수=genuine_review, 홀수=non_review → non_review 2건.
	ids := []string{"v:row:0", "v:row:1", "v:row:2", "v:row:3"}
	jsonlPath, parquetPath := setupDocGenuinenessFixture(t, ids, ids)

	summary, _, total, items, err := loadDocGenuinenessArtifact(jsonlPath, parquetPath, 10, 0, "v", "non_review")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 2 {
		t.Fatalf("filtered total = %d, want 2", total)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	for _, it := range items {
		if fmt.Sprint(it["genuineness"]) != "non_review" {
			t.Fatalf("item genuineness = %v, want non_review", it["genuineness"])
		}
	}
	// summary는 전체 분포 유지 (필터 무관) → total 4.
	if summary["total"] != 4 {
		t.Fatalf("summary.total must stay 4 (full), got %v", summary["total"])
	}
}

func TestLoadDocGenuinenessArtifact_FilterPagination(t *testing.T) {
	ids := []string{"v:row:0", "v:row:1", "v:row:2", "v:row:3"}
	jsonlPath, parquetPath := setupDocGenuinenessFixture(t, ids, ids)

	// genuine_review 2건(i=0,2) 중 limit=1 → items 1, total 2.
	_, _, total, items, err := loadDocGenuinenessArtifact(jsonlPath, parquetPath, 1, 0, "v", "genuine_review")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 2 {
		t.Fatalf("filtered total = %d, want 2", total)
	}
	if len(items) != 1 {
		t.Fatalf("limit=1 → items = %d, want 1", len(items))
	}
}
