package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-06-08 (파일럿) — clean_summary.analysis_columns가 docs_extra_columns
// payload(name/type/label/source_column)로 변환되는지 잠금.
func TestDocsExtraColumnsFromAnalysis(t *testing.T) {
	version := domain.DatasetVersion{
		Metadata: map[string]any{
			"clean_summary": map[string]any{
				"analysis_columns": []any{
					map[string]any{"name": "col_3", "type": "string", "label": "수집채널", "source_column": "수집채널"},
					map[string]any{"name": "likes", "type": "integer", "label": "좋아요 수", "source_column": "좋아요 수"},
				},
			},
		},
	}
	cols := docsExtraColumnsFromAnalysis(version)
	if len(cols) != 2 {
		t.Fatalf("want 2 columns, got %d", len(cols))
	}
	if cols[0]["name"] != "col_3" || cols[0]["type"] != "string" {
		t.Errorf("col0 name/type: %v", cols[0])
	}
	if cols[0]["label"] != "수집채널" || cols[0]["source_column"] != "수집채널" {
		t.Errorf("col0 label/source: %v", cols[0])
	}
	if cols[1]["type"] != "integer" {
		t.Errorf("col1 type want integer, got %v", cols[1]["type"])
	}
	// description은 label로 채워져 옛 렌더 경로와 호환
	if cols[1]["description"] != "좋아요 수" {
		t.Errorf("col1 description want label, got %v", cols[1]["description"])
	}
}

func TestDocsExtraColumnsFromAnalysisEmpty(t *testing.T) {
	// clean_summary 없음 → nil
	if cols := docsExtraColumnsFromAnalysis(domain.DatasetVersion{Metadata: map[string]any{}}); cols != nil {
		t.Errorf("want nil for missing clean_summary, got %v", cols)
	}
	// analysis_columns 없음 → nil
	v := domain.DatasetVersion{Metadata: map[string]any{"clean_summary": map[string]any{}}}
	if cols := docsExtraColumnsFromAnalysis(v); cols != nil {
		t.Errorf("want nil for missing analysis_columns, got %v", cols)
	}
	// name 비면 skip → 결과 비면 nil
	v2 := domain.DatasetVersion{Metadata: map[string]any{
		"clean_summary": map[string]any{"analysis_columns": []any{map[string]any{"type": "string"}}},
	}}
	if cols := docsExtraColumnsFromAnalysis(v2); cols != nil {
		t.Errorf("want nil when all columns lack name, got %v", cols)
	}
}
