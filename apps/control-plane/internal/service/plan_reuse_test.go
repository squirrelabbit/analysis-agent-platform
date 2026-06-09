package service

import (
	"context"
	"encoding/json"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-26 (plan reuse POC-1) — classifier + patch builder 단위
// test. 모든 reject signal과 happy path를 잠근다.

func TestClassifyReuseAction_HappyPathLimit(t *testing.T) {
	cases := []struct {
		q      string
		wantN  int
	}{
		{"상위 2개만 보여줘", 2},
		{"상위 10개", 10},
		{"top 5", 5},
		{"3개만 보여줘", 3},
	}
	for _, c := range cases {
		t.Run(c.q, func(t *testing.T) {
			action, params, ok := classifyReuseAction(c.q)
			if !ok {
				t.Fatalf("expected reuse classification, got no_match")
			}
			if action != "add_limit" {
				t.Fatalf("expected add_limit, got %s", action)
			}
			if params["n"].(int) != c.wantN {
				t.Fatalf("expected n=%d, got %v", c.wantN, params["n"])
			}
		})
	}
}

func TestClassifyReuseAction_HappyPathFormat(t *testing.T) {
	cases := []struct {
		q      string
		format string
	}{
		{"표로 보여줘", "table"},
		{"차트로 보여줘", "chart"},
		{"json으로 보여줘", "json"},
	}
	for _, c := range cases {
		t.Run(c.q, func(t *testing.T) {
			action, params, ok := classifyReuseAction(c.q)
			if !ok {
				t.Fatalf("expected reuse classification, got no_match")
			}
			if action != "change_present_format" {
				t.Fatalf("expected change_present_format, got %s", action)
			}
			if params["format"].(string) != c.format {
				t.Fatalf("expected format=%s, got %v", c.format, params["format"])
			}
		})
	}
}

func TestClassifyReuseAction_RejectSignals(t *testing.T) {
	// 보수적 정책: reject 신호가 보이면 false negative > false positive.
	cases := []string{
		"왜 부정 평가가 많을까?",
		"이유가 뭐야?",
		"요약해줘",
		"정리해줘",
		"aspect별 평균 점수를 계산해줘",
		"작년 대비 증감",
		"월별로 보여줘",
		"compare last with this",
		"새로운 group by 적용",
		"",
		"전혀 관련 없는 새 분석",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			_, _, ok := classifyReuseAction(q)
			if ok {
				t.Fatalf("expected no_match for %q, got reuse classification", q)
			}
		})
	}
}

func TestTryReusePlan_DisabledByDefaultShortCircuits(t *testing.T) {
	// silverone 2026-06-08 (context hijack 완화) — planReuseEnabled=false면
	// classifier/store/deps를 건드리지 않고 즉시 fallback(reuse 비활성)이어야 한다.
	// store/deps가 nil이어도 게이트가 먼저 반환하므로 panic하지 않는다.
	svc := &AnalysisThreadService{planReuseEnabled: false}
	decision, _, ok := svc.tryReusePlan(
		context.Background(),
		"p1", "d1",
		domain.AnalysisThread{ThreadID: "t1"},
		"부정 후기가 가장 많은 aspect TOP 5를 보여줘",
		nil,
	)
	if ok {
		t.Fatalf("reuse 비활성 시 reuse 흐름을 타면 안 됨")
	}
	if decision.Reused {
		t.Fatalf("decision.Reused는 false여야 함")
	}
	if decision.FallbackReason != "reuse_disabled" {
		t.Fatalf("fallback_reason=reuse_disabled 기대, got %q", decision.FallbackReason)
	}
}

func _aspectPlanWithSort() map[string]any {
	return map[string]any{
		"plan_version": "v2",
		"steps": []any{
			map[string]any{
				"id":    "agg",
				"skill": "aggregate",
				"params": map[string]any{
					"input":    "clauses",
					"group_by": []any{"aspect"},
					"metrics": []any{
						map[string]any{"name": "count", "function": "count", "column": "*"},
					},
				},
			},
			map[string]any{
				"id":    "ranked",
				"skill": "sort",
				"params": map[string]any{
					"input": "agg",
					"by":    []any{"count"},
					"order": "desc",
				},
			},
			map[string]any{
				"id":    "present_top",
				"skill": "present",
				"params": map[string]any{
					"input":  "ranked",
					"format": "table",
					"title":  "aspect별 카운트",
				},
			},
		},
	}
}

func _aspectPlanNoSort() map[string]any {
	return map[string]any{
		"plan_version": "v2",
		"steps": []any{
			map[string]any{
				"id":    "agg",
				"skill": "aggregate",
				"params": map[string]any{
					"input":    "clauses",
					"group_by": []any{"aspect"},
					"metrics":  []any{map[string]any{"name": "count", "function": "count", "column": "*"}},
				},
			},
			map[string]any{
				"id":    "present_top",
				"skill": "present",
				"params": map[string]any{
					"input":  "agg",
					"format": "table",
				},
			},
		},
	}
}

func TestPatchPlanForReuse_ChangePresentFormat(t *testing.T) {
	source := _aspectPlanWithSort()
	patched, err := patchPlanForReuse(source, "change_present_format", map[string]any{"format": "chart"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	steps := patched["steps"].([]any)
	last := steps[len(steps)-1].(map[string]any)
	params := last["params"].(map[string]any)
	if params["format"].(string) != "chart" {
		t.Fatalf("expected format=chart, got %v", params["format"])
	}
	// 원본 plan은 변경되지 않아야 함 (deep clone)
	srcSteps := source["steps"].([]any)
	srcLast := srcSteps[len(srcSteps)-1].(map[string]any)
	srcParams := srcLast["params"].(map[string]any)
	if srcParams["format"].(string) != "table" {
		t.Fatalf("source plan was mutated: %v", srcParams["format"])
	}
}

func TestPatchPlanForReuse_AddLimitOnSortStep(t *testing.T) {
	source := _aspectPlanWithSort()
	patched, err := patchPlanForReuse(source, "add_limit", map[string]any{"n": 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	steps := patched["steps"].([]any)
	// sort step의 params.limit이 2여야 함
	var sortStep map[string]any
	for _, step := range steps {
		stepMap := step.(map[string]any)
		if stepMap["skill"] == "sort" {
			sortStep = stepMap
		}
	}
	if sortStep == nil {
		t.Fatalf("sort step missing in patched plan")
	}
	params := sortStep["params"].(map[string]any)
	if params["limit"].(int) != 2 {
		t.Fatalf("expected limit=2, got %v", params["limit"])
	}
}

func TestPatchPlanForReuse_AddLimitRejectedWithoutSortBeforePresent(t *testing.T) {
	// POC-1는 present.input이 sort step일 때만 limit 적용. aggregate 직후
	// present인 plan은 reuse 거부 → patch error.
	source := _aspectPlanNoSort()
	_, err := patchPlanForReuse(source, "add_limit", map[string]any{"n": 5})
	if err == nil {
		t.Fatalf("expected error when present.input is not a sort step")
	}
}

func TestPatchPlanForReuse_BadFormatParam(t *testing.T) {
	source := _aspectPlanWithSort()
	_, err := patchPlanForReuse(source, "change_present_format", map[string]any{"format": ""})
	if err == nil {
		t.Fatalf("expected error for empty format")
	}
}

func TestInjectReuseMetadata_HappyPath(t *testing.T) {
	result := json.RawMessage(`{"plan": {"plan_version":"v2"}, "present": {"format":"table"}}`)
	decision := ReuseDecision{
		Reused:       true,
		Action:       "add_limit",
		ActionParams: map[string]any{"n": 2},
		SourceRunID:  "run-abc",
	}
	out, err := injectReuseMetadata(result, decision)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unexpected json error: %v", err)
	}
	reuse, ok := doc["reuse"].(map[string]any)
	if !ok {
		t.Fatalf("reuse key missing")
	}
	if reuse["applied"] != true {
		t.Fatalf("expected applied=true, got %v", reuse["applied"])
	}
	if reuse["action"] != "add_limit" {
		t.Fatalf("expected action=add_limit, got %v", reuse["action"])
	}
	if reuse["source_run_id"] != "run-abc" {
		t.Fatalf("expected source_run_id=run-abc, got %v", reuse["source_run_id"])
	}
}

func TestInjectReuseMetadata_FallbackReasonExposed(t *testing.T) {
	result := json.RawMessage(`{"plan_version":"v2"}`)
	decision := ReuseDecision{
		Reused:         false,
		FallbackReason: "classifier_no_match",
	}
	out, err := injectReuseMetadata(result, decision)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var doc map[string]any
	_ = json.Unmarshal(out, &doc)
	reuse := doc["reuse"].(map[string]any)
	if reuse["applied"] != false {
		t.Fatalf("expected applied=false, got %v", reuse["applied"])
	}
	if reuse["fallback_reason"] != "classifier_no_match" {
		t.Fatalf("expected fallback_reason=classifier_no_match, got %v", reuse["fallback_reason"])
	}
}
