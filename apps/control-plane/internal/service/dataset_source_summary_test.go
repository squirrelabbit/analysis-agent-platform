package service

import (
	"os"
	"path/filepath"
	"testing"
)

// CSV 프리뷰가 SAMPLE_SIZE=-1 없이도 컬럼/행수/샘플을 정확히 채우고(silverone 2026-06-26
// 전체 스캔 제거), metadata 캐시로 round-trip되는지 검증한다.
func TestSourceSummaryCSVAndCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "sample.csv")
	content := "제목,좋아요수,본문\n" +
		"축제 후기,123,맥주가 맛있었어요\n" +
		"둘째날,0,사람이 많았어요\n" +
		"셋째날,5,재밌었다\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	summary := loadDatasetSourceSummary(csvPath, defaultDatasetSourceSummarySampleLimit)
	if summary == nil || summary.Status != "ready" {
		t.Fatalf("summary not ready: %+v", summary)
	}
	if summary.RowCount == nil || *summary.RowCount != 3 {
		t.Fatalf("row count = %v, want 3", summary.RowCount)
	}
	names := make([]string, 0, len(summary.Columns))
	for _, c := range summary.Columns {
		names = append(names, c.Name)
	}
	if len(names) != 3 || names[0] != "제목" || names[2] != "본문" {
		t.Fatalf("columns = %v, want [제목 좋아요수 본문]", names)
	}

	// metadata 캐시 build → cached 복원 라운드트립.
	cache := buildSourceSummaryCache(csvPath)
	if cache == nil {
		t.Fatal("buildSourceSummaryCache returned nil for ready csv")
	}
	restored := cachedSourceSummary(map[string]any{sourceSummaryMetaKey: cache})
	if restored == nil {
		t.Fatal("cachedSourceSummary returned nil")
	}
	if restored.RowCount == nil || *restored.RowCount != 3 {
		t.Fatalf("restored row count = %v, want 3", restored.RowCount)
	}
	if restored.ColumnCount != 3 {
		t.Fatalf("restored column count = %d, want 3", restored.ColumnCount)
	}
}

// 캐시 키가 없으면 nil(legacy 버전 → 호출부가 계산 fallback).
func TestCachedSourceSummaryMissing(t *testing.T) {
	if cachedSourceSummary(nil) != nil {
		t.Error("nil metadata should yield nil")
	}
	if cachedSourceSummary(map[string]any{"other": 1}) != nil {
		t.Error("missing key should yield nil")
	}
}
