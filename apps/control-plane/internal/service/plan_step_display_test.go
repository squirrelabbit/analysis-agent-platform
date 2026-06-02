package service

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// silverone 2026-06-01 (plan step display projection) — 8 skill display 빌더
// 잠금 + projectAnalyzePlan integration 잠금.
//
// keep set: label / expression. raw params는 그대로 유지되어야 한다
// (고급 보기용).

// rawStep — JSON에서 받은 step shape을 시뮬레이션. 실제 result_json.plan.steps
// 와 동일 구조.
func rawStep(skill string, params map[string]any) map[string]any {
	return map[string]any{
		"id":     "s1",
		"skill":  skill,
		"params": params,
	}
}

// ===== skill별 display 잠금 =====

func TestDisplayFilter(t *testing.T) {
	got := buildStepDisplay(rawStep("filter", map[string]any{
		"column":   "genuineness",
		"operator": "neq",
		"value":    "non_review",
	}))
	wantExpr := "WHERE genuineness != 'non_review'"
	if got["label"] != "조건 필터" {
		t.Errorf("label: %v", got["label"])
	}
	if got["expression"] != wantExpr {
		t.Errorf("expression: want %q, got %q", wantExpr, got["expression"])
	}
}

func TestDisplayFilterAllOperators(t *testing.T) {
	cases := []struct {
		op    string
		value any
		want  string
	}{
		{"eq", "active", "WHERE c = 'active'"},
		{"gt", 10.0, "WHERE c > 10"},
		{"gte", 10.5, "WHERE c >= 10.5"},
		{"lt", 0.0, "WHERE c < 0"},
		{"lte", -1.0, "WHERE c <= -1"},
		{"in", []any{"a", "b"}, "WHERE c IN ('a', 'b')"},
		{"not_in", []any{1.0, 2.0}, "WHERE c NOT IN (1, 2)"},
		{"contains", "foo", "WHERE c LIKE 'foo'"},
		{"between", []any{1.0, 10.0}, "WHERE c BETWEEN 1 AND 10"},
		{"is_null", nil, "WHERE c IS NULL"},
		{"not_null", nil, "WHERE c IS NOT NULL"},
	}
	for _, tc := range cases {
		got := buildStepDisplay(rawStep("filter", map[string]any{
			"column": "c", "operator": tc.op, "value": tc.value,
		}))
		if got["expression"] != tc.want {
			t.Errorf("op=%s: want %q, got %q", tc.op, tc.want, got["expression"])
		}
	}
}

func TestDisplayJoin(t *testing.T) {
	got := buildStepDisplay(rawStep("join", map[string]any{
		"left":  "clauses",
		"right": "real_reviews",
		"how":   "inner",
		"on":    []any{"doc_id"},
	}))
	want := "clauses INNER JOIN real_reviews ON doc_id"
	if got["label"] != "데이터 연결" || got["expression"] != want {
		t.Errorf("join display: %v", got)
	}
}

func TestDisplayJoinDefaultsToInnerWhenMissing(t *testing.T) {
	got := buildStepDisplay(rawStep("join", map[string]any{
		"left":  "a",
		"right": "b",
		"on":    []any{"id"},
	}))
	if !strings.Contains(got["expression"].(string), "INNER JOIN") {
		t.Errorf("missing how should default to INNER, got %v", got["expression"])
	}
}

func TestDisplayAggregateGroupAndMetric(t *testing.T) {
	got := buildStepDisplay(rawStep("aggregate", map[string]any{
		"input":    "clauses",
		"group_by": []any{"aspect"},
		"metrics": []any{
			map[string]any{"name": "count", "function": "count", "column": ""},
		},
	}))
	if got["label"] != "aspect별 집계" {
		t.Errorf("label: %v", got["label"])
	}
	wantExpr := "GROUP BY aspect · COUNT(*) AS count"
	if got["expression"] != wantExpr {
		t.Errorf("expr: want %q, got %q", wantExpr, got["expression"])
	}
}

func TestDisplayAggregateMultipleMetrics(t *testing.T) {
	got := buildStepDisplay(rawStep("aggregate", map[string]any{
		"group_by": []any{"aspect"},
		"metrics": []any{
			map[string]any{"name": "cnt", "function": "count"},
			map[string]any{"name": "avg_score", "function": "avg", "column": "score"},
		},
	}))
	wantExpr := "GROUP BY aspect · COUNT(*) AS cnt, AVG(score) AS avg_score"
	if got["expression"] != wantExpr {
		t.Errorf("expr: want %q, got %q", wantExpr, got["expression"])
	}
}

func TestDisplaySort(t *testing.T) {
	got := buildStepDisplay(rawStep("sort", map[string]any{
		"input": "agg",
		"by":    []any{"count"},
		"order": "desc",
	}))
	if got["label"] != "정렬" || got["expression"] != "ORDER BY count DESC" {
		t.Errorf("sort display: %v", got)
	}
}

func TestDisplaySortWithLimit(t *testing.T) {
	got := buildStepDisplay(rawStep("sort", map[string]any{
		"by":    []any{"count"},
		"order": "desc",
		"limit": 10.0,
	}))
	if got["expression"] != "ORDER BY count DESC LIMIT 10" {
		t.Errorf("sort+limit: %v", got["expression"])
	}
}

func TestDisplayCalculateRatio(t *testing.T) {
	got := buildStepDisplay(rawStep("calculate", map[string]any{
		"input": "agg",
		"expressions": []any{
			map[string]any{
				"name":      "negative_ratio",
				"operation": "ratio",
				"left":      "negative_count",
				"right":     "total_count",
			},
		},
	}))
	if got["label"] != "비율 계산" {
		t.Errorf("label: %v", got["label"])
	}
	want := "negative_ratio = negative_count / total_count * 100"
	if got["expression"] != want {
		t.Errorf("ratio expr: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayCalculateShareOfTotalGlobal(t *testing.T) {
	got := buildStepDisplay(rawStep("calculate", map[string]any{
		"input": "sentiment_counts",
		"expressions": []any{
			map[string]any{
				"name":      "ratio",
				"operation": "share_of_total",
				"value":     "count",
			},
		},
	}))
	if got["label"] != "비중 계산" {
		t.Errorf("label: %v", got["label"])
	}
	want := "ratio = count / 전체 합계 * 100"
	if got["expression"] != want {
		t.Errorf("share_of_total expr: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayCalculateShareOfTotalPartitioned(t *testing.T) {
	got := buildStepDisplay(rawStep("calculate", map[string]any{
		"input": "aspect_counts",
		"expressions": []any{
			map[string]any{
				"name":         "ratio",
				"operation":    "share_of_total",
				"value":        "count",
				"partition_by": []any{"sentiment"},
			},
		},
	}))
	want := "ratio = count / sentiment별 합계 * 100"
	if got["expression"] != want {
		t.Errorf("share_of_total partitioned expr: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayCalculatePercentChange(t *testing.T) {
	got := buildStepDisplay(rawStep("calculate", map[string]any{
		"expressions": []any{
			map[string]any{
				"name":      "delta_rate",
				"operation": "percent_change",
				"left":      "last_count",
				"right":     "this_count",
			},
		},
	}))
	if got["label"] != "증감률 계산" {
		t.Errorf("label: %v", got["label"])
	}
	want := "delta_rate = (this_count - last_count) / last_count * 100"
	if got["expression"] != want {
		t.Errorf("percent_change: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayCompare(t *testing.T) {
	got := buildStepDisplay(rawStep("compare", map[string]any{
		"left":        "agg_last",
		"right":       "agg_this",
		"left_label":  "last_year",
		"right_label": "this_year",
		"join_key":    []any{"aspect"},
	}))
	if got["label"] != "두 결과 비교" {
		t.Errorf("label: %v", got["label"])
	}
	want := "COMPARE last_year vs this_year ON aspect"
	if got["expression"] != want {
		t.Errorf("compare: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayPresent(t *testing.T) {
	got := buildStepDisplay(rawStep("present", map[string]any{
		"input":  "sorted",
		"format": "table",
		"title":  "아스펙트별 건수",
		"limit":  100.0,
	}))
	if got["label"] != "결과 표시" {
		t.Errorf("label: %v", got["label"])
	}
	want := "TABLE: 아스펙트별 건수 (LIMIT 100)"
	if got["expression"] != want {
		t.Errorf("present: want %q, got %q", want, got["expression"])
	}
}

func TestDisplayPresentNoTitle(t *testing.T) {
	got := buildStepDisplay(rawStep("present", map[string]any{"format": "table"}))
	if got["expression"] != "TABLE" {
		t.Errorf("present no title: %v", got["expression"])
	}
}

func TestDisplaySummarize(t *testing.T) {
	got := buildStepDisplay(rawStep("summarize", map[string]any{
		"focus": "부정 비율 추이",
	}))
	if got["label"] != "자연어 요약" {
		t.Errorf("label: %v", got["label"])
	}
	if got["expression"] != "SUMMARIZE focus=부정 비율 추이" {
		t.Errorf("summarize: %v", got["expression"])
	}
}

// ===== integration via projectAnalyzePlan =====

func TestProjectAnalyzePlanAttachesDisplayToEachStep(t *testing.T) {
	plan := map[string]any{
		"plan_version": "v2",
		"steps": []any{
			rawStep("filter", map[string]any{"column": "genuineness", "operator": "neq", "value": "non_review"}),
			rawStep("aggregate", map[string]any{
				"group_by": []any{"aspect"},
				"metrics":  []any{map[string]any{"name": "count", "function": "count"}},
			}),
		},
	}
	got := projectAnalyzePlan(plan)
	steps := got["steps"].([]map[string]any)
	if len(steps) != 2 {
		t.Fatalf("step count: %d", len(steps))
	}
	for i, step := range steps {
		display, ok := step["display"].(map[string]any)
		if !ok {
			t.Fatalf("step %d: display missing", i)
		}
		if display["label"] == "" || display["expression"] == "" {
			t.Errorf("step %d: empty display fields %v", i, display)
		}
		// raw params 유지 — display 추가 후에도 params 그대로.
		if _, ok := step["params"]; !ok {
			t.Errorf("step %d: params dropped", i)
		}
	}
}

func TestProjectAnalyzePlanOmitsDisplayForUnknownSkill(t *testing.T) {
	plan := map[string]any{
		"plan_version": "v2",
		"steps": []any{
			rawStep("nonexistent_skill", map[string]any{"foo": "bar"}),
		},
	}
	got := projectAnalyzePlan(plan)
	steps := got["steps"].([]map[string]any)
	if len(steps) != 1 {
		t.Fatalf("step count: %d", len(steps))
	}
	if _, hasDisplay := steps[0]["display"]; hasDisplay {
		t.Errorf("unknown skill must not have display, got %v", steps[0]["display"])
	}
	// raw params는 fallback용으로 유지.
	if !reflect.DeepEqual(steps[0]["params"], map[string]any{"foo": "bar"}) {
		t.Errorf("raw params dropped on unknown skill")
	}
}

func TestProjectAnalyzePlanDisplaySurvivesJSONRoundTrip(t *testing.T) {
	// 실제 응답이 JSON encode/decode를 거쳐도 display.label / display.expression
	// 가 string으로 유지되는지.
	plan := map[string]any{
		"plan_version": "v2",
		"steps": []any{
			rawStep("filter", map[string]any{"column": "c", "operator": "eq", "value": "x"}),
		},
	}
	projected := projectAnalyzePlan(plan)
	bytes, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	steps, _ := decoded["steps"].([]any)
	if len(steps) != 1 {
		t.Fatalf("step count after roundtrip: %d", len(steps))
	}
	display := steps[0].(map[string]any)["display"].(map[string]any)
	if display["label"] != "조건 필터" {
		t.Errorf("label after roundtrip: %v", display["label"])
	}
	if display["expression"] != "WHERE c = 'x'" {
		t.Errorf("expression after roundtrip: %v", display["expression"])
	}
}

// extractPlanFromResultJSON도 같은 빌더를 사용하는지 확인 (thread detail 경로).
func TestExtractPlanFromResultJSONIncludesStepDisplay(t *testing.T) {
	raw := json.RawMessage(`{
		"plan": {
			"plan_version": "v2",
			"steps": [
				{"id": "s1", "skill": "sort", "params": {"by": ["count"], "order": "desc"}}
			]
		}
	}`)
	got := extractPlanFromResultJSON(raw)
	steps := got["steps"].([]map[string]any)
	if len(steps) != 1 {
		t.Fatalf("step count: %d", len(steps))
	}
	display, ok := steps[0]["display"].(map[string]any)
	if !ok {
		t.Fatalf("display missing in extracted plan")
	}
	if display["expression"] != "ORDER BY count DESC" {
		t.Errorf("expression: %v", display["expression"])
	}
}
