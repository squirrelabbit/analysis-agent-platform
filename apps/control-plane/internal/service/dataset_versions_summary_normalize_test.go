package service

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-28 — version-detail summary normalize 잠금. raw count 키
// (tier_counts / aspect_counts / sentiment_counts / clause_count)는 응답에서
// 제거되고 build-detail과 같은 normalized 키(genuineness / aspect / sentiment /
// total)로 옮겨진다. 부수 metadata 필드는 보존된다.

func TestNormalizeDocGenuinenessSummaryHappyPath(t *testing.T) {
	raw := map[string]any{
		"tier_counts": map[string]any{
			"genuine_review": float64(389),
			"mixed":          float64(0),
			"non_review":     float64(1724),
			"uncertain":      float64(8),
		},
		"processed_row_count":     float64(2121),
		"input_row_count":         float64(2121),
		"parse_failures":          float64(0),
		"model":                   "wisenut/wise-lloa-max-v1.2.1",
		"model_display_name":      "WISE LLOA Max v1.2.1",
		"prompt_version":          "v1",
		"total_prompt_tokens":     float64(8758881),
		"total_completion_tokens": float64(241666),
		"applied": map[string]any{
			"subject_name":       "강릉 국가유산야행",
			"model":              "wisenut/wise-lloa-max-v1.2.1",
			"model_display_name": "WISE LLOA Max v1.2.1",
		},
	}
	got := normalizeDocGenuinenessSummary(raw).(map[string]any)

	// raw key 제거
	if _, ok := got["tier_counts"]; ok {
		t.Errorf("tier_counts key must be removed from normalized summary")
	}

	// normalized key 추가
	g, ok := got["genuineness"].(map[string]any)
	if !ok {
		t.Fatalf("genuineness map missing")
	}
	wantG := map[string]any{
		"genuine_review": 389,
		"mixed":          0,
		"non_review":     1724,
		"uncertain":      8,
	}
	if !reflect.DeepEqual(g, wantG) {
		t.Errorf("genuineness counts: want %v, got %v", wantG, g)
	}

	// total — processed_row_count 우선
	if got["total"] != 2121 {
		t.Errorf("total: want 2121, got %v", got["total"])
	}

	// 부수 필드 보존 (model_display_name 포함 — 하드코딩 매핑 없이 passthrough)
	for _, k := range []string{"input_row_count", "parse_failures", "model", "model_display_name", "prompt_version", "total_prompt_tokens", "total_completion_tokens", "applied", "processed_row_count"} {
		if _, ok := got[k]; !ok {
			t.Errorf("metadata field %q must be preserved", k)
		}
	}
	if got["model_display_name"] != "WISE LLOA Max v1.2.1" {
		t.Errorf("model_display_name passthrough: want %q, got %v", "WISE LLOA Max v1.2.1", got["model_display_name"])
	}
}

func TestSummaryMetadataStringReadsModelDisplayName(t *testing.T) {
	// silverone 2026-06-05 — artifact view applied.model_display_name은 build 시
	// 저장된 <stage>_summary metadata에서 summaryMetadataString으로 회수한다.
	// raw model id와 화면 표시용 이름(env 기반)이 함께 들어 있다.
	metadata := map[string]any{
		"doc_genuineness_summary": map[string]any{
			"model":              "wisenut/wise-lloa-max-v1.2.1",
			"model_display_name": "WISE LLOA Max v1.2.1",
		},
	}
	if got := summaryMetadataString(metadata, "doc_genuineness_summary", "model"); got != "wisenut/wise-lloa-max-v1.2.1" {
		t.Errorf("model: want raw id, got %q", got)
	}
	if got := summaryMetadataString(metadata, "doc_genuineness_summary", "model_display_name"); got != "WISE LLOA Max v1.2.1" {
		t.Errorf("model_display_name: want display, got %q", got)
	}
}

func TestSummaryMetadataStringModelDisplayNameAbsentOrEmpty(t *testing.T) {
	// 옛 dataset: summary에 model_display_name 없음 → "" (화면이 model fallback).
	old := map[string]any{
		"clause_label_summary": map[string]any{"model": "wisenut/wise-lloa-max-v1.2.1"},
	}
	if got := summaryMetadataString(old, "clause_label_summary", "model_display_name"); got != "" {
		t.Errorf("absent display name: want empty, got %q", got)
	}
	// 빈 문자열도 trim 후 "" — applied에서 생략된다.
	empty := map[string]any{
		"clause_label_summary": map[string]any{"model_display_name": "  "},
	}
	if got := summaryMetadataString(empty, "clause_label_summary", "model_display_name"); got != "" {
		t.Errorf("blank display name: want empty, got %q", got)
	}
}

func TestNormalizeDocGenuinenessSummaryTotalFallbackToTierSum(t *testing.T) {
	raw := map[string]any{
		"tier_counts": map[string]any{
			"genuine_review": float64(10),
			"non_review":     float64(20),
		},
		// processed_row_count 없음
	}
	got := normalizeDocGenuinenessSummary(raw).(map[string]any)
	if got["total"] != 30 {
		t.Errorf("total fallback to tier_counts sum: want 30, got %v", got["total"])
	}
}

func TestNormalizeDocGenuinenessSummaryNilAndNonMap(t *testing.T) {
	if normalizeDocGenuinenessSummary(nil) != nil {
		t.Errorf("nil summary must pass through as nil")
	}
	if got := normalizeDocGenuinenessSummary("not-a-map"); got != "not-a-map" {
		t.Errorf("non-map summary must pass through unchanged, got %v", got)
	}
}

func TestNormalizeDocGenuinenessSummaryWithoutTierCounts(t *testing.T) {
	// tier_counts 없고 processed_row_count도 없으면 total 자체 생성 안 함.
	raw := map[string]any{
		"model": "wisenut/wise-lloa-max-v1.2.1",
	}
	got := normalizeDocGenuinenessSummary(raw).(map[string]any)
	if _, ok := got["total"]; ok {
		t.Errorf("total must not be set without tier_counts/processed_row_count, got %v", got["total"])
	}
	if _, ok := got["genuineness"]; ok {
		t.Errorf("genuineness must not be set without tier_counts")
	}
	if got["model"] != "wisenut/wise-lloa-max-v1.2.1" {
		t.Errorf("model must be preserved")
	}
}

func TestNormalizeClauseLabelSummaryHappyPath(t *testing.T) {
	raw := map[string]any{
		"aspect_counts": map[string]any{
			"show_program":     float64(958),
			"ambiance_scenery": float64(607),
		},
		"sentiment_counts": map[string]any{
			"positive": float64(2305),
			"neutral":  float64(1124),
			"negative": float64(261),
		},
		"clause_count":            float64(3690),
		"processed_doc_count":     float64(2121),
		"input_row_count":         float64(2121),
		"skipped_by_filter":       float64(1732),
		"skipped_empty":           float64(0),
		"parse_failures":          float64(0),
		"taxonomy_id":             "festival-v2",
		"taxonomy_hash":           "b52c5e...",
		"reasoning_effort":        "low",
		"concurrency":             float64(8),
		"include_genuineness":     []any{"genuine_review", "mixed"},
		"model":                   "wisenut/wise-lloa-max-v1.2.1",
		"prompt_version":          "v3",
		"total_prompt_tokens":     float64(123456),
		"total_completion_tokens": float64(7890),
	}
	got := normalizeClauseLabelSummary(raw).(map[string]any)

	// raw key 제거
	for _, k := range []string{"aspect_counts", "sentiment_counts", "clause_count"} {
		if _, ok := got[k]; ok {
			t.Errorf("raw key %q must be removed from normalized summary", k)
		}
	}

	// normalized
	if aspect, ok := got["aspect"].(map[string]any); !ok {
		t.Errorf("aspect map missing")
	} else if aspect["show_program"] != 958 || aspect["ambiance_scenery"] != 607 {
		t.Errorf("aspect counts mismatch: %v", aspect)
	}

	if sent, ok := got["sentiment"].(map[string]any); !ok {
		t.Errorf("sentiment map missing")
	} else if sent["positive"] != 2305 || sent["negative"] != 261 || sent["neutral"] != 1124 {
		t.Errorf("sentiment counts mismatch: %v", sent)
	}

	if got["total"] != 3690 {
		t.Errorf("total: want 3690 (from clause_count), got %v", got["total"])
	}

	// 부수 필드 보존
	for _, k := range []string{"processed_doc_count", "input_row_count", "skipped_by_filter", "skipped_empty", "parse_failures", "taxonomy_id", "taxonomy_hash", "reasoning_effort", "concurrency", "include_genuineness", "model", "prompt_version", "total_prompt_tokens", "total_completion_tokens"} {
		if _, ok := got[k]; !ok {
			t.Errorf("metadata field %q must be preserved", k)
		}
	}
}

func TestNormalizeClauseLabelSummaryNilAndNonMap(t *testing.T) {
	if normalizeClauseLabelSummary(nil) != nil {
		t.Errorf("nil summary must pass through as nil")
	}
	if got := normalizeClauseLabelSummary(42); got != 42 {
		t.Errorf("non-map summary must pass through unchanged, got %v", got)
	}
}

func TestNormalizeClauseLabelSummaryEmptyMap(t *testing.T) {
	got := normalizeClauseLabelSummary(map[string]any{}).(map[string]any)
	for _, k := range []string{"aspect", "sentiment", "total"} {
		if _, ok := got[k]; ok {
			t.Errorf("empty summary must not produce normalized key %q, got %v", k, got[k])
		}
	}
}

func TestSummaryCountToIntAcceptsJSONNumber(t *testing.T) {
	// jsonb scan 시 json.Number로 들어오는 경우 보호.
	if i, ok := summaryCountToInt(json.Number("123")); !ok || i != 123 {
		t.Errorf("json.Number(123): want (123,true), got (%d,%v)", i, ok)
	}
	if _, ok := summaryCountToInt(json.Number("12.5")); ok {
		t.Errorf("non-integer json.Number must return (0,false)")
	}
	if _, ok := summaryCountToInt("not-a-number"); ok {
		t.Errorf("string must return (0,false)")
	}
}

func TestNormalizeRoundTripJSONMarshalingProducesIntegers(t *testing.T) {
	raw := map[string]any{
		"tier_counts": map[string]any{
			"genuine_review": float64(389),
		},
		"processed_row_count": float64(2121),
	}
	got := normalizeDocGenuinenessSummary(raw)
	bytes, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	s := string(bytes)
	// integer로 직렬화돼야 — float64 389.0이 "389"가 아니라 소수점 없이 나오는지 확인.
	if want := `"genuine_review":389`; !strings.Contains(s, want) {
		t.Errorf("expected %q in marshaled output, got: %s", want, s)
	}
	if want := `"total":2121`; !strings.Contains(s, want) {
		t.Errorf("expected %q in marshaled output, got: %s", want, s)
	}
}

// summarizeDatasetVersionDetail integration — metadata 그대로(raw shape) 들어와도
// 응답에서는 normalized form만 노출.
func TestSummarizeDatasetVersionDetailNormalizesStageSummaries(t *testing.T) {
	version := domain.DatasetVersion{
		DatasetVersionID: "v-1",
		Metadata: map[string]any{
			"doc_genuineness_status": "ready",
			"doc_genuineness_summary": map[string]any{
				"tier_counts": map[string]any{
					"genuine_review": float64(389),
					"mixed":          float64(0),
					"non_review":     float64(1724),
					"uncertain":      float64(8),
				},
				"processed_row_count": float64(2121),
				"model":               "wisenut/wise-lloa-max-v1.2.1",
			},
			"clause_label_status": "ready",
			"clause_label_summary": map[string]any{
				"aspect_counts":    map[string]any{"show_program": float64(958)},
				"sentiment_counts": map[string]any{"positive": float64(2305)},
				"clause_count":     float64(3690),
				"taxonomy_id":      "festival-v2",
			},
		},
	}
	detail := summarizeDatasetVersionDetail(version)

	dg, ok := detail.DocGenuineness.Summary.(map[string]any)
	if !ok {
		t.Fatalf("DocGenuineness.Summary must be normalized map")
	}
	if _, ok := dg["tier_counts"]; ok {
		t.Errorf("DocGenuineness.Summary must not contain raw tier_counts")
	}
	if _, ok := dg["genuineness"]; !ok {
		t.Errorf("DocGenuineness.Summary must contain normalized genuineness")
	}
	if dg["total"] != 2121 {
		t.Errorf("DocGenuineness.Summary total: want 2121, got %v", dg["total"])
	}
	if dg["model"] != "wisenut/wise-lloa-max-v1.2.1" {
		t.Errorf("DocGenuineness.Summary model must be preserved")
	}

	cl, ok := detail.ClauseLabel.Summary.(map[string]any)
	if !ok {
		t.Fatalf("ClauseLabel.Summary must be normalized map")
	}
	for _, k := range []string{"aspect_counts", "sentiment_counts", "clause_count"} {
		if _, ok := cl[k]; ok {
			t.Errorf("ClauseLabel.Summary must not contain raw %q", k)
		}
	}
	for _, k := range []string{"aspect", "sentiment", "total"} {
		if _, ok := cl[k]; !ok {
			t.Errorf("ClauseLabel.Summary must contain normalized %q", k)
		}
	}
	if cl["total"] != 3690 {
		t.Errorf("ClauseLabel.Summary total: want 3690, got %v", cl["total"])
	}
	if cl["taxonomy_id"] != "festival-v2" {
		t.Errorf("ClauseLabel.Summary taxonomy_id must be preserved")
	}
}
