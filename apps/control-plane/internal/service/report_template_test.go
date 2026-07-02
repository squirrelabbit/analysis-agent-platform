package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/registry"
)

func testLabels() reportLabels {
	return reportLabels{
		sentiment:   map[string]string{"positive": "긍정", "neutral": "중립", "negative": "부정"},
		genuineness: map[string]string{"genuine_review": "진성 후기", "non_review": "비후기", "uncertain": "불확실"},
		aspect:      map[string]string{"food": "음식/먹거리", "show_program": "공연/프로그램"},
	}
}

func TestReportTemplateLoaderHasBasicTemplate(t *testing.T) {
	tpl, ok := registry.ReportTemplateByID("unstructured_basic_v1")
	if !ok {
		t.Fatalf("unstructured_basic_v1 템플릿을 못 찾음 (config/report_templates 로드 실패)")
	}
	if len(tpl.Sections) == 0 {
		t.Fatalf("템플릿에 섹션이 없음")
	}
	ids := map[string]bool{}
	for _, s := range tpl.Sections {
		ids[s.ID] = true
	}
	for _, want := range []string{"analysis_overview", "sentiment_distribution", "aspect_sentiment", "keyword_distribution"} {
		if !ids[want] {
			t.Errorf("섹션 %q 누락", want)
		}
	}
}

func TestDistributionDataOrderAndPercent(t *testing.T) {
	node := map[string]any{"positive": float64(6), "neutral": float64(1), "negative": float64(3)}
	src := &registry.ReportTemplateSource{Path: "summary.sentiment", Order: []string{"positive", "neutral", "negative"}}
	out := distributionData(node, src, testLabels())

	if total, _ := toFloat(out["total"]); total != 10 {
		t.Errorf("total = %v, want 10", out["total"])
	}
	items, _ := out["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("items 수 = %d, want 3", len(items))
	}
	first := items[0].(map[string]any)
	if first["key"] != "positive" || first["label"] != "긍정" {
		t.Errorf("첫 항목 = %v, want positive/긍정 (고정 order)", first)
	}
	if pct, _ := toFloat(first["percent"]); pct != 60 {
		t.Errorf("긍정 percent = %v, want 60", first["percent"])
	}
}

func TestDistributionDataOrderByCountDescAndTop(t *testing.T) {
	node := map[string]any{"food": float64(5), "show_program": float64(9), "etc": float64(2)}
	src := &registry.ReportTemplateSource{Path: "summary.aspect", Top: 2}
	out := distributionData(node, src, testLabels())
	items, _ := out["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("top=2 적용 안 됨: %d개", len(items))
	}
	if items[0].(map[string]any)["key"] != "show_program" {
		t.Errorf("count 내림차순 정렬 안 됨: %v", items[0])
	}
}

func TestStackedDataShape(t *testing.T) {
	node := map[string]any{
		"show_program": map[string]any{"total": float64(10), "sentiment": map[string]any{
			"positive": map[string]any{"count": float64(8)},
			"neutral":  map[string]any{"count": float64(1)},
			"negative": map[string]any{"count": float64(1)},
		}},
		"food": map[string]any{"total": float64(4), "sentiment": map[string]any{
			"positive": map[string]any{"count": float64(2)},
			"neutral":  map[string]any{"count": float64(1)},
			"negative": map[string]any{"count": float64(1)},
		}},
	}
	out := stackedData(node, testLabels())
	cats, _ := out["categories"].([]any)
	if len(cats) != 2 || cats[0].(map[string]any)["key"] != "show_program" {
		t.Fatalf("categories total 내림차순 정렬 안 됨: %v", cats)
	}
	series, _ := out["series"].([]any)
	if len(series) != 3 {
		t.Fatalf("series 3개(pos/neu/neg) 아님: %d", len(series))
	}
	pos := series[0].(map[string]any)
	if pos["key"] != "positive" {
		t.Errorf("첫 series = %v, want positive", pos["key"])
	}
	counts, _ := pos["counts"].([]any)
	if v, _ := toFloat(counts[0]); v != 8 {
		t.Errorf("show_program positive count = %v, want 8", counts[0])
	}
}

func TestRankDataFromKeywordList(t *testing.T) {
	node := []any{
		map[string]any{"keyword": "공연", "count": float64(902)},
		map[string]any{"keyword": "분위기", "count": float64(831)},
		map[string]any{"keyword": "야경", "count": float64(712)},
	}
	src := &registry.ReportTemplateSource{Top: 2}
	out := rankData(node, src, testLabels())
	items, _ := out["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("top=2 적용 안 됨: %d", len(items))
	}
	first := items[0].(map[string]any)
	if first["rank"] != 1 || first["label"] != "공연" {
		t.Errorf("첫 키워드 = %v, want rank1/공연", first)
	}
	if v, _ := toFloat(first["value"]); v != 902 {
		t.Errorf("value = %v, want 902", first["value"])
	}
}

func TestRankDataFromAspectSentimentOrderBy(t *testing.T) {
	node := map[string]any{
		"show_program": map[string]any{"sentiment": map[string]any{"positive": map[string]any{"count": float64(100)}}},
		"food":         map[string]any{"sentiment": map[string]any{"positive": map[string]any{"count": float64(300)}}},
	}
	src := &registry.ReportTemplateSource{OrderBy: "positive"}
	out := rankData(node, src, testLabels())
	items, _ := out["items"].([]any)
	if items[0].(map[string]any)["label"] != "음식/먹거리" {
		t.Errorf("positive 순위 1위 = %v, want 음식(300)", items[0])
	}
}

// DuckDB 집계 로더(aggregateGroupedCounts)는 map[string]int를, JSONL 키워드 리스트는
// []map[string]any를 반환한다. transformer가 map[string]any/[]any만 단언하면 verify
// build에서 분포가 통째로 빈값이 되던 회귀를 잠근다.
func TestTransformersHandleConcreteGoTypes(t *testing.T) {
	// map[string]int 분포 — digPath + distributionData 둘 다 통과해야 한다.
	root := map[string]any{"summary": map[string]any{
		"sentiment": map[string]int{"positive": 4206, "neutral": 3072, "negative": 446},
	}}
	node := digPath(root, "summary.sentiment")
	if node == nil {
		t.Fatalf("digPath가 map[string]int를 못 뚫음")
	}
	src := &registry.ReportTemplateSource{Path: "summary.sentiment", Order: []string{"positive", "neutral", "negative"}}
	out := distributionData(node, src, testLabels())
	if total, _ := toFloat(out["total"]); total != 7724 {
		t.Errorf("total = %v, want 7724", out["total"])
	}
	items, _ := out["items"].([]any)
	if len(items) != 3 || items[0].(map[string]any)["label"] != "긍정" {
		t.Fatalf("map[string]int 분포 변환 실패: %v", items)
	}

	// []map[string]any 키워드 리스트 + 문자열 count — rankData 통과해야 한다.
	kw := []map[string]any{
		{"keyword": "드론", "count": "262"},
		{"keyword": "체험", "count": "182"},
	}
	rout := rankData(kw, &registry.ReportTemplateSource{Top: 10}, testLabels())
	ritems, _ := rout["items"].([]any)
	if len(ritems) != 2 {
		t.Fatalf("[]map[string]any rank 변환 실패: %v", ritems)
	}
	first := ritems[0].(map[string]any)
	if first["label"] != "드론" {
		t.Errorf("첫 키워드 label = %v, want 드론", first["label"])
	}
	if v, _ := toFloat(first["value"]); v != 262 {
		t.Errorf("문자열 count 파싱 실패: value = %v, want 262", first["value"])
	}
}

func TestDigPathAndToFloat(t *testing.T) {
	root := map[string]any{"summary": map[string]any{"genuineness": map[string]any{"genuine_review": float64(1182)}}}
	v := digPath(root, "summary.genuineness.genuine_review")
	if f, _ := toFloat(v); f != 1182 {
		t.Errorf("digPath = %v, want 1182", v)
	}
	if digPath(root, "summary.missing.x") != nil {
		t.Errorf("없는 path는 nil이어야")
	}
}

// #31 분석 개요 전용 뷰 transformer 잠금.
func TestPeriodTimelineData(t *testing.T) {
	// analysisPeriodsView가 만든 형태(연도별 대상기간·축제기간·역할).
	node := []map[string]any{
		{"year": 2025, "role": "base", "role_label": "기준 연도",
			"target_start": "2025-09-12", "target_end": "2025-09-28", "target_days": 17,
			"festival_start": "2025-09-19", "festival_end": "2025-09-21"},
		{"year": 2024, "role": "compare", "role_label": "비교 연도",
			"target_start": "2024-09-13", "target_end": "2024-09-29", "target_days": 17,
			"festival_start": "2024-09-20", "festival_end": "2024-09-22"},
	}
	out := periodTimelineData(node)
	rows, _ := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows len = %d, want 2", len(rows))
	}
	base := rows[0].(map[string]any)
	if base["role"] != "base" || base["role_label"] != "기준 연도" {
		t.Fatalf("row0 = %v, want base 기준 연도", base)
	}
	if d, _ := anyToInt(base["target_days"]); d != 17 {
		t.Fatalf("target_days = %v, want 17", base["target_days"])
	}
	if base["target_start"] != "2025-09-12" || base["festival_end"] != "2025-09-21" {
		t.Fatalf("row0 dates = %v", base)
	}
	cmp := rows[1].(map[string]any)
	if cmp["role_label"] != "비교 연도" {
		t.Fatalf("row1 label = %v, want 비교 연도", cmp["role_label"])
	}
}

func TestTagListData(t *testing.T) {
	// []string / []any 혼합 + 공백 제거.
	out := tagListData([]any{"블로그", "  ", "뉴스", 42})
	items, _ := out["items"].([]any)
	if len(items) != 2 || items[0] != "블로그" || items[1] != "뉴스" {
		t.Fatalf("tag_list items = %v, want [블로그 뉴스]", items)
	}
}

func TestDefinitionListData(t *testing.T) {
	node := []map[string]any{
		{"key": "operation", "label": "운영", "description": "행사 진행·인력·안전"},
		{"key": "content", "label": "", "description": "프로그램·공연"}, // label 없으면 key
	}
	out := definitionListData(node)
	items, _ := out["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items len = %d, want 2", len(items))
	}
	if items[0].(map[string]any)["term"] != "운영" {
		t.Fatalf("term[0] = %v, want 운영", items[0].(map[string]any)["term"])
	}
	if items[1].(map[string]any)["term"] != "content" {
		t.Fatalf("term[1] should fall back to key: %v", items[1].(map[string]any)["term"])
	}
}
