package service

import "testing"

// #31 축제 메타데이터 정규화/검증 + 분석 개요 조립 잠금.
// 2026-07-01 재설계: 프로젝트 레벨 저장 + during(축제기간) + ±N일 파생 모델.

func TestNormalizeFestivalMetadata_Valid(t *testing.T) {
	in := map[string]any{
		"name": "  강릉야행문화축제 ",
		"periods": []any{
			// 문자열 연도("2025년") + before/after ±N일(정수/문자열) 정규화
			map[string]any{"year": "2025년", "festival_start": "2025-08-15", "festival_end": "2025-08-17",
				"before_days": float64(3), "after_days": "5"},
			// N 미설정(개방형) + 0은 저장 안 함
			map[string]any{"year": float64(2024), "festival_start": "2024-08-15", "festival_end": "2024-08-17",
				"before_days": float64(0)},
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
	if periods[0]["year"] != 2025 || periods[0]["festival_start"] != "2025-08-15" {
		t.Fatalf("period[0] = %v, want year 2025 start 2025-08-15", periods[0])
	}
	if periods[0]["before_days"] != 3 || periods[0]["after_days"] != 5 {
		t.Fatalf("period[0] days = before %v after %v, want 3/5", periods[0]["before_days"], periods[0]["after_days"])
	}
	// 0/미설정은 저장하지 않는다(개방형).
	if _, ok := periods[1]["before_days"]; ok {
		t.Fatalf("period[1].before_days should be omitted when 0, got %v", periods[1]["before_days"])
	}
	if _, ok := periods[1]["after_days"]; ok {
		t.Fatalf("period[1].after_days should be omitted when unset")
	}
}

func TestNormalizeFestivalMetadata_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
	}{
		{"name 누락", map[string]any{"periods": []any{}}},
		{"잘못된 날짜", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "festival_start": "2025-13-40", "festival_end": "2025-08-17"}}}},
		{"start>end", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "festival_start": "2025-08-20", "festival_end": "2025-08-17"}}}},
		{"연도 불일치", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "festival_start": "2024-08-15", "festival_end": "2024-08-17"}}}},
		{"연도 중복", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "festival_start": "2025-08-15", "festival_end": "2025-08-17"},
			map[string]any{"year": float64(2025), "festival_start": "2025-09-15", "festival_end": "2025-09-17"}}}},
		{"before_days 음수", map[string]any{"name": "x", "periods": []any{
			map[string]any{"year": float64(2025), "festival_start": "2025-08-15", "festival_end": "2025-08-17",
				"before_days": float64(-1)}}}},
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
			"name":    "군산맥주축제",
			"periods": []any{map[string]any{"year": float64(2025), "festival_start": "2025-05-01", "festival_end": "2025-05-03"}},
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

func TestAnalysisPeriodsView_DerivesAndSorts(t *testing.T) {
	// 2025는 ±N일, 2024는 개방형(before 미설정). 입력 순서 뒤섞음.
	periods := []map[string]any{
		{"year": 2024, "festival_start": "2024-08-15", "festival_end": "2024-08-17"},
		{"year": 2025, "festival_start": "2025-08-15", "festival_end": "2025-08-17", "before_days": 3, "after_days": 5},
	}
	// dataStart/dataEnd 미지정("") — 개방형 경계는 빈 값으로 남는다(기존 계약).
	got := analysisPeriodsView(periods, "", "")
	// 연도 내림차순 + during/before/after → 각 연도 3구간 = 6.
	if len(got) != 6 {
		t.Fatalf("len = %d, want 6", len(got))
	}
	want := []struct {
		year   int
		period string
	}{
		{2025, "during"}, {2025, "before"}, {2025, "after"},
		{2024, "during"}, {2024, "before"}, {2024, "after"},
	}
	for i, w := range want {
		if got[i]["year"] != w.year || got[i]["period"] != w.period {
			t.Fatalf("row %d = (%v,%v), want (%d,%s)", i, got[i]["year"], got[i]["period"], w.year, w.period)
		}
	}
	// 2025 before: [08-12, 08-14], after: [08-18, 08-22]
	if got[1]["start_ymd"] != "2025-08-12" || got[1]["end_ymd"] != "2025-08-14" {
		t.Fatalf("2025 before = %v~%v, want 08-12~08-14", got[1]["start_ymd"], got[1]["end_ymd"])
	}
	if got[2]["start_ymd"] != "2025-08-18" || got[2]["end_ymd"] != "2025-08-22" {
		t.Fatalf("2025 after = %v~%v, want 08-18~08-22", got[2]["start_ymd"], got[2]["end_ymd"])
	}
	// 2024 개방형: before start "", after end ""
	if got[4]["start_ymd"] != "" || got[4]["end_ymd"] != "2024-08-14" {
		t.Fatalf("2024 before(open) = %v~%v, want ''~08-14", got[4]["start_ymd"], got[4]["end_ymd"])
	}
	if got[5]["start_ymd"] != "2024-08-18" || got[5]["end_ymd"] != "" {
		t.Fatalf("2024 after(open) = %v~%v, want 08-18~''", got[5]["start_ymd"], got[5]["end_ymd"])
	}
}

// 개방형 경계에 실제 데이터 시작/끝 날짜가 채워지고 open 플래그가 표시되는지.
func TestAnalysisPeriodsView_FillsOpenBoundaryWithDataDates(t *testing.T) {
	periods := []map[string]any{
		{"year": 2026, "festival_start": "2026-08-01", "festival_end": "2026-08-03"},
	}
	got := analysisPeriodsView(periods, "2026-06-01", "2026-09-30")
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	// before(개방형): start = 데이터 시작(2026-06-01), open_start=true, end = start-1일.
	before := got[1]
	if before["period"] != "before" {
		t.Fatalf("row1 period = %v, want before", before["period"])
	}
	if before["start_ymd"] != "2026-06-01" || before["end_ymd"] != "2026-07-31" {
		t.Fatalf("before = %v~%v, want 2026-06-01~2026-07-31", before["start_ymd"], before["end_ymd"])
	}
	if before["open_start"] != true {
		t.Fatalf("before open_start should be true")
	}
	// after(개방형): end = 데이터 끝(2026-09-30), open_end=true, start = end+1일.
	after := got[2]
	if after["start_ymd"] != "2026-08-04" || after["end_ymd"] != "2026-09-30" {
		t.Fatalf("after = %v~%v, want 2026-08-04~2026-09-30", after["start_ymd"], after["end_ymd"])
	}
	if after["open_end"] != true {
		t.Fatalf("after open_end should be true")
	}

	// periodTableData는 경계에 날짜가 있어도 명시 open 플래그를 유지해야 한다.
	table := periodTableData(got)
	rows, _ := table["rows"].([]any)
	if len(rows) != 3 {
		t.Fatalf("table rows = %d, want 3", len(rows))
	}
	brow := rows[1].(map[string]any)
	if brow["open_start"] != true || brow["start_ymd"] != "2026-06-01" {
		t.Fatalf("table before = open_start %v start %v", brow["open_start"], brow["start_ymd"])
	}
	arow := rows[2].(map[string]any)
	if arow["open_end"] != true || arow["end_ymd"] != "2026-09-30" {
		t.Fatalf("table after = open_end %v end %v", arow["open_end"], arow["end_ymd"])
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
