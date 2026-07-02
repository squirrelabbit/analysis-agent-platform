package service

import "testing"

// #31 축제 메타데이터 정규화/검증 + 분석 개요 조립 잠금.
// 2026-07-02 재설계: 연도별 대상기간(target) + 축제기간(festival) + 역할(기준/비교) 모델.

func TestNormalizeFestivalMetadata_Valid(t *testing.T) {
	in := map[string]any{
		"name": "  강릉야행문화축제 ",
		"periods": []any{
			// 문자열 연도("2025년") + role(base), 축제기간 ⊆ 대상기간
			map[string]any{"year": "2025년", "role": "base",
				"target_start": "2025-09-12", "target_end": "2025-09-28",
				"festival_start": "2025-09-19", "festival_end": "2025-09-21"},
			// role 미지정 → compare
			map[string]any{"year": float64(2024),
				"target_start": "2024-09-13", "target_end": "2024-09-29",
				"festival_start": "2024-09-20", "festival_end": "2024-09-22"},
		},
	}
	out, err := normalizeFestivalMetadata(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out["name"] != "강릉야행문화축제" {
		t.Fatalf("name = %v, want trimmed", out["name"])
	}
	periods := out["periods"].([]map[string]any)
	if len(periods) != 2 {
		t.Fatalf("periods len = %d, want 2", len(periods))
	}
	if periods[0]["year"] != 2025 || periods[0]["role"] != "base" {
		t.Fatalf("period[0] = %v, want year 2025 role base", periods[0])
	}
	if periods[0]["target_start"] != "2025-09-12" || periods[0]["festival_start"] != "2025-09-19" {
		t.Fatalf("period[0] dates = %v", periods[0])
	}
	// role 미지정은 compare로 정규화.
	if periods[1]["role"] != "compare" {
		t.Fatalf("period[1].role = %v, want compare", periods[1]["role"])
	}
}

// role 미지정(base 0개)이면 최신 연도를 자동으로 기준(base)으로 승격.
func TestNormalizeFestivalMetadata_AutoPromotesNewestAsBase(t *testing.T) {
	out, err := normalizeFestivalMetadata(map[string]any{
		"name": "x",
		"periods": []any{
			map[string]any{"year": float64(2024), "target_start": "2024-09-13", "target_end": "2024-09-29", "festival_start": "2024-09-20", "festival_end": "2024-09-22"},
			map[string]any{"year": float64(2025), "target_start": "2025-09-12", "target_end": "2025-09-28", "festival_start": "2025-09-19", "festival_end": "2025-09-21"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	periods := out["periods"].([]map[string]any)
	baseCount, baseYear := 0, 0
	for _, p := range periods {
		if p["role"] == "base" {
			baseCount++
			baseYear, _ = p["year"].(int)
		}
	}
	if baseCount != 1 || baseYear != 2025 {
		t.Fatalf("auto-promote: baseCount %d baseYear %d, want 1/2025", baseCount, baseYear)
	}
}

func TestNormalizeFestivalMetadata_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
	}{
		{"name 누락", map[string]any{"periods": []any{}}},
		{"잘못된 날짜", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2025-09-01", "target_end": "2025-09-30", "festival_start": "2025-13-40", "festival_end": "2025-09-21"}}}},
		{"축제 start>end", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2025-09-01", "target_end": "2025-09-30", "festival_start": "2025-09-21", "festival_end": "2025-09-19"}}}},
		{"대상 start>end", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2025-09-30", "target_end": "2025-09-01", "festival_start": "2025-09-19", "festival_end": "2025-09-21"}}}},
		{"축제 연도 불일치", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2024-09-01", "target_end": "2024-09-30", "festival_start": "2024-09-19", "festival_end": "2024-09-21"}}}},
		{"축제가 대상 밖", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2025-09-20", "target_end": "2025-09-28", "festival_start": "2025-09-19", "festival_end": "2025-09-21"}}}},
		{"연도 중복", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "target_start": "2025-09-01", "target_end": "2025-09-30", "festival_start": "2025-09-19", "festival_end": "2025-09-21"},
			map[string]any{"year": float64(2025), "target_start": "2025-10-01", "target_end": "2025-10-30", "festival_start": "2025-10-19", "festival_end": "2025-10-21"}}}},
		{"기준 2개", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "role": "base", "target_start": "2025-09-01", "target_end": "2025-09-30", "festival_start": "2025-09-19", "festival_end": "2025-09-21"},
			map[string]any{"year": float64(2024), "role": "base", "target_start": "2024-09-01", "target_end": "2024-09-30", "festival_start": "2024-09-19", "festival_end": "2024-09-21"}}}},
		{"role 오값", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "role": "primary", "target_start": "2025-09-01", "target_end": "2025-09-30", "festival_start": "2025-09-19", "festival_end": "2025-09-21"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeFestivalMetadata(tc.in); err == nil {
				t.Fatalf("expected error for %q", tc.name)
			} else if _, ok := err.(ErrInvalidArgument); !ok {
				t.Fatalf("err = %T, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestNormalizeProjectMetadata_PassthroughAndFestival(t *testing.T) {
	// festival 없는 key는 그대로 통과 + festival만 검증한다.
	out, err := normalizeProjectMetadata(map[string]any{
		"note": "keep me",
		"festival": map[string]any{
			"name": "군산맥주축제",
			"periods": []any{map[string]any{"year": float64(2025), "role": "base",
				"target_start": "2025-05-01", "target_end": "2025-05-10",
				"festival_start": "2025-05-05", "festival_end": "2025-05-07"}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out["note"] != "keep me" {
		t.Fatalf("note passthrough failed: %v", out["note"])
	}
	fest := out["festival"].(map[string]any)
	if fest["name"] != "군산맥주축제" {
		t.Fatalf("festival name = %v", fest["name"])
	}
	// nil은 빈 map.
	empty, err := normalizeProjectMetadata(nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("nil metadata should normalize to empty map, got %v err %v", empty, err)
	}
}

func TestAnalysisPeriodsView_ShapeAndSort(t *testing.T) {
	// 입력 순서 뒤섞음: 2024(비교) 먼저, 2025(기준) 뒤.
	periods := []map[string]any{
		{"year": 2024, "role": "compare", "target_start": "2024-09-13", "target_end": "2024-09-29", "festival_start": "2024-09-20", "festival_end": "2024-09-22"},
		{"year": 2025, "role": "base", "target_start": "2025-09-12", "target_end": "2025-09-28", "festival_start": "2025-09-19", "festival_end": "2025-09-21"},
	}
	got := analysisPeriodsView(periods)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	// 기준 연도(2025) 먼저.
	if got[0]["year"] != 2025 || got[0]["role"] != "base" || got[0]["role_label"] != "기준 연도" {
		t.Fatalf("row0 = %v, want 2025 base 기준 연도", got[0])
	}
	if got[1]["year"] != 2024 || got[1]["role_label"] != "비교 연도" {
		t.Fatalf("row1 = %v, want 2024 비교 연도", got[1])
	}
	// 대상기간 총 일수(양끝 포함): 09-12~09-28 = 17.
	if got[0]["target_days"] != 17 {
		t.Fatalf("target_days = %v, want 17", got[0]["target_days"])
	}
	if got[0]["target_start"] != "2025-09-12" || got[0]["festival_start"] != "2025-09-19" {
		t.Fatalf("row0 dates = %v", got[0])
	}
}

func TestLoadTypeDefinitions_FestivalGunsan(t *testing.T) {
	defs := loadTypeDefinitions("festival-gunsan")
	if len(defs) == 0 {
		t.Fatal("expected aspect definitions for festival-gunsan")
	}
	// 각 항목은 key/label/description을 가진다.
	first := defs[0]
	if first["key"] == "" || first["label"] == nil {
		t.Fatalf("def[0] missing key/label: %v", first)
	}
}
