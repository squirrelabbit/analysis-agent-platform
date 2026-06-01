package service

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-06-01 — thread detail messages[].display lightweight projection.
// helper level (extractDisplayFromResultJSON) + integration level
// (GetAnalysisThread → assistant.Display 채워짐) 잠금.

func TestExtractDisplayFromResultJSONHappyPath(t *testing.T) {
	raw := json.RawMessage(`{
		"plan": {"plan_version": "v2", "steps": []},
		"composer": {
			"assistant_content": "분석 결과",
			"display": {
				"type": "table",
				"title": "아스펙트 분포",
				"columns": [{"key": "aspect"}, {"key": "count"}],
				"rows": [{"aspect": "food", "count": 3}],
				"total_rows": 3,
				"returned_rows": 3,
				"max_rows": 1000,
				"truncated": false,
				"warnings": []
			}
		}
	}`)
	got := extractDisplayFromResultJSON(raw)
	if got == nil {
		t.Fatalf("display must not be nil")
	}
	wantKeys := []string{"type", "title", "columns", "rows", "total_rows", "returned_rows", "max_rows", "truncated", "warnings"}
	for _, k := range wantKeys {
		if _, ok := got[k]; !ok {
			t.Errorf("display.%s missing", k)
		}
	}
	if got["type"] != "table" {
		t.Errorf("type: want table, got %v", got["type"])
	}
	if got["title"] != "아스펙트 분포" {
		t.Errorf("title mismatch: %v", got["title"])
	}
}

func TestExtractDisplayFromResultJSONDropsNonKeepFields(t *testing.T) {
	// display 안에 keep set 외 필드가 있으면 모두 drop되는지.
	raw := json.RawMessage(`{
		"composer": {
			"display": {
				"type": "table",
				"rows": [],
				"sample_rows": [{"secret": "drop"}],
				"sql": "SELECT * FROM secret",
				"internal_id": "xxx"
			}
		}
	}`)
	got := extractDisplayFromResultJSON(raw)
	if got == nil {
		t.Fatalf("display must not be nil")
	}
	for _, banned := range []string{"sample_rows", "sql", "internal_id"} {
		if _, ok := got[banned]; ok {
			t.Errorf("non-keep field %q leaked into display", banned)
		}
	}
}

func TestExtractDisplayFromResultJSONNoComposerReturnsNil(t *testing.T) {
	raw := json.RawMessage(`{"plan": {"plan_version": "v2"}}`)
	if got := extractDisplayFromResultJSON(raw); got != nil {
		t.Errorf("no composer → nil, got %v", got)
	}
}

func TestExtractDisplayFromResultJSONNoDisplayReturnsNil(t *testing.T) {
	raw := json.RawMessage(`{"composer": {"assistant_content": "hi"}}`)
	if got := extractDisplayFromResultJSON(raw); got != nil {
		t.Errorf("composer without display → nil, got %v", got)
	}
}

func TestExtractDisplayFromResultJSONEmptyOrInvalid(t *testing.T) {
	if extractDisplayFromResultJSON(nil) != nil {
		t.Errorf("nil → nil")
	}
	if extractDisplayFromResultJSON(json.RawMessage("")) != nil {
		t.Errorf("empty → nil")
	}
	if extractDisplayFromResultJSON(json.RawMessage("not-json")) != nil {
		t.Errorf("invalid json → nil")
	}
}

// integration — GetAnalysisThread가 assistant message에 display attach하는지.

func TestGetAnalysisThreadAttachesDisplayToAssistantMessage(t *testing.T) {
	svc := newDisplayTestService(t)

	thread := domain.AnalysisThread{
		ThreadID:         "t1",
		ProjectID:        "p1",
		DatasetID:        "d1",
		DatasetVersionID: "v1",
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	if err := svc.store.SaveAnalysisThread(thread); err != nil {
		t.Fatalf("SaveAnalysisThread: %v", err)
	}

	runID := "run-1"
	runResult := json.RawMessage(`{
		"composer": {
			"assistant_content": "결과",
			"display": {
				"type": "table",
				"title": "title-A",
				"rows": [{"a": 1}],
				"total_rows": 1,
				"returned_rows": 1,
				"max_rows": 1000,
				"truncated": false,
				"warnings": ["w1"]
			}
		}
	}`)
	if err := svc.store.SaveAnalysisRun(domain.AnalysisRun{
		RunID: runID, ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "completed",
		ResultJSON: runResult, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
	// user message — Display 없어야.
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m1", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		Role: "user", Content: "질문", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage user: %v", err)
	}
	// assistant message — RunID 연결.
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m2", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		Role: "assistant", Content: "답변", RunID: &runID,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage assistant: %v", err)
	}

	detail, err := svc.GetAnalysisThread("p1", "d1", "t1")
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if len(detail.Messages) != 2 {
		t.Fatalf("messages: want 2, got %d", len(detail.Messages))
	}

	user := detail.Messages[0]
	if user.Role != "user" {
		t.Fatalf("expected user message first, got %s", user.Role)
	}
	if user.Display != nil {
		t.Errorf("user message must not have display, got %v", user.Display)
	}

	assistant := detail.Messages[1]
	if assistant.Role != "assistant" {
		t.Fatalf("expected assistant message second, got %s", assistant.Role)
	}
	if assistant.Display == nil {
		t.Fatalf("assistant.Display must be attached")
	}
	if assistant.Display["type"] != "table" {
		t.Errorf("display.type: %v", assistant.Display["type"])
	}
	if assistant.Display["title"] != "title-A" {
		t.Errorf("display.title: %v", assistant.Display["title"])
	}
	wantKeys := []string{"type", "title", "rows", "total_rows", "returned_rows", "max_rows", "truncated", "warnings"}
	for _, k := range wantKeys {
		if _, ok := assistant.Display[k]; !ok {
			t.Errorf("assistant.display.%s missing", k)
		}
	}
	// JSON serialize 시 display가 그대로 노출되는지 (omitempty 동작).
	bytes, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var decoded map[string]any
	_ = json.Unmarshal(bytes, &decoded)
	msgs, _ := decoded["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("decoded messages count mismatch")
	}
	userDecoded := msgs[0].(map[string]any)
	if _, ok := userDecoded["display"]; ok {
		t.Errorf("user message json must omit display key (omitempty)")
	}
	assistantDecoded := msgs[1].(map[string]any)
	if _, ok := assistantDecoded["display"]; !ok {
		t.Errorf("assistant message json must include display key")
	}
}

func TestGetAnalysisThreadOmitsDisplayWhenRunMissing(t *testing.T) {
	svc := newDisplayTestService(t)
	if err := svc.store.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t2", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1",
		CreatedAt:        time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisThread: %v", err)
	}
	missingRunID := "run-ghost"
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m1", ThreadID: "t2", ProjectID: "p1", DatasetID: "d1",
		Role: "assistant", Content: "답변", RunID: &missingRunID,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage: %v", err)
	}

	detail, err := svc.GetAnalysisThread("p1", "d1", "t2")
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if len(detail.Messages) != 1 {
		t.Fatalf("messages: want 1, got %d", len(detail.Messages))
	}
	if detail.Messages[0].Display != nil {
		t.Errorf("missing run → display must be nil, got %v", detail.Messages[0].Display)
	}
}

func TestGetAnalysisThreadOmitsDisplayWhenResultJSONLacksDisplay(t *testing.T) {
	svc := newDisplayTestService(t)
	if err := svc.store.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t3", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1",
		CreatedAt:        time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisThread: %v", err)
	}
	runID := "run-no-display"
	if err := svc.store.SaveAnalysisRun(domain.AnalysisRun{
		RunID: runID, ThreadID: "t3", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "completed",
		ResultJSON: json.RawMessage(`{"composer": {"assistant_content": "텍스트만"}}`),
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m1", ThreadID: "t3", ProjectID: "p1", DatasetID: "d1",
		Role: "assistant", Content: "답변", RunID: &runID,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage: %v", err)
	}
	detail, err := svc.GetAnalysisThread("p1", "d1", "t3")
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if detail.Messages[0].Display != nil {
		t.Errorf("result_json without display → message.Display must be nil")
	}
}

// helper level lock — display projection이 keep set만 노출.
func TestProjectComposerDisplayOnlyKeepSet(t *testing.T) {
	got := projectComposerDisplay(map[string]any{
		"type":       "table",
		"title":      "t",
		"rows":       []any{},
		"sql":        "secret",
		"sample":     []any{},
		"context":    map[string]any{"leak": true},
		"warnings":   []any{"w"},
		"max_rows":   1000,
		"truncated":  false,
		"total_rows": 0,
	})
	want := map[string]any{
		"type":       "table",
		"title":      "t",
		"rows":       []any{},
		"warnings":   []any{"w"},
		"max_rows":   1000,
		"truncated":  false,
		"total_rows": 0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("keep set mismatch:\n got %v\nwant %v", got, want)
	}
}

// silverone 2026-06-01 — thread detail messages[].plan lightweight projection.
// extractPlanFromResultJSON helper + GetAnalysisThread integration 잠금.

func TestExtractPlanFromResultJSONHappyPath(t *testing.T) {
	raw := json.RawMessage(`{
		"plan": {
			"plan_version": "v2",
			"steps": [
				{"id": "s1", "skill": "aggregate", "params": {"group_by": "aspect"}, "internal_only": "drop"}
			],
			"non_keep_field": "drop"
		},
		"composer": {"assistant_content": "ok"}
	}`)
	got := extractPlanFromResultJSON(raw)
	if got == nil {
		t.Fatalf("plan must not be nil")
	}
	if got["plan_version"] != "v2" {
		t.Errorf("plan_version: %v", got["plan_version"])
	}
	steps, ok := got["steps"].([]map[string]any)
	if !ok || len(steps) != 1 {
		t.Fatalf("steps shape mismatch: %T %v", got["steps"], got["steps"])
	}
	step := steps[0]
	if step["id"] != "s1" || step["skill"] != "aggregate" {
		t.Errorf("step id/skill mismatch: %v", step)
	}
	if _, ok := step["params"].(map[string]any); !ok {
		t.Errorf("step.params must be map: %T", step["params"])
	}
	// keep set 외 필드는 drop.
	if _, leaked := step["internal_only"]; leaked {
		t.Errorf("step internal_only leaked")
	}
	if _, leaked := got["non_keep_field"]; leaked {
		t.Errorf("plan non_keep_field leaked")
	}
}

func TestExtractPlanFromResultJSONReturnsNilWhenAbsent(t *testing.T) {
	cases := []json.RawMessage{
		nil,
		json.RawMessage(""),
		json.RawMessage("not-json"),
		json.RawMessage(`{"composer": {}}`),
		json.RawMessage(`{"plan": null}`),
	}
	for i, raw := range cases {
		if got := extractPlanFromResultJSON(raw); got != nil {
			t.Errorf("case %d: want nil, got %v", i, got)
		}
	}
}

func TestGetAnalysisThreadAttachesPlanToAssistantMessage(t *testing.T) {
	svc := newDisplayTestService(t)
	if err := svc.store.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t-plan", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1",
		CreatedAt:        time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisThread: %v", err)
	}
	runID := "run-plan"
	if err := svc.store.SaveAnalysisRun(domain.AnalysisRun{
		RunID: runID, ThreadID: "t-plan", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "completed",
		ResultJSON: json.RawMessage(`{
			"plan": {
				"plan_version": "v2",
				"steps": [
					{"id": "agg1", "skill": "aggregate", "params": {"group_by": "aspect"}},
					{"id": "out", "skill": "present", "params": {}}
				]
			},
			"composer": {"assistant_content": "결과"}
		}`),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
	// MessageID로 보조 정렬되므로 user="m1" / assistant="m2"로 user를 먼저 둔다.
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m1", ThreadID: "t-plan", ProjectID: "p1", DatasetID: "d1",
		Role: "user", Content: "질문", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage user: %v", err)
	}
	if err := svc.store.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m2", ThreadID: "t-plan", ProjectID: "p1", DatasetID: "d1",
		Role: "assistant", Content: "답변", RunID: &runID,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisMessage assistant: %v", err)
	}

	detail, err := svc.GetAnalysisThread("p1", "d1", "t-plan")
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if detail.Messages[0].Plan != nil {
		t.Errorf("user message must not have plan, got %v", detail.Messages[0].Plan)
	}
	assistant := detail.Messages[1]
	if assistant.Plan == nil {
		t.Fatalf("assistant.Plan must be attached")
	}
	if assistant.Plan["plan_version"] != "v2" {
		t.Errorf("plan_version: %v", assistant.Plan["plan_version"])
	}
	steps, ok := assistant.Plan["steps"].([]map[string]any)
	if !ok || len(steps) != 2 {
		t.Fatalf("steps shape: %T len=%d", assistant.Plan["steps"], len(steps))
	}
	if steps[0]["id"] != "agg1" || steps[1]["skill"] != "present" {
		t.Errorf("steps content mismatch: %v", steps)
	}

	// JSON marshal — user는 omit, assistant는 plan 키 포함.
	bytes, _ := json.Marshal(detail)
	var decoded map[string]any
	_ = json.Unmarshal(bytes, &decoded)
	msgs, _ := decoded["messages"].([]any)
	if _, ok := msgs[0].(map[string]any)["plan"]; ok {
		t.Errorf("user message json must omit plan (omitempty)")
	}
	if _, ok := msgs[1].(map[string]any)["plan"]; !ok {
		t.Errorf("assistant message json must include plan key")
	}
}

// newDisplayTestService — memory store만 가진 가벼운 DatasetService.
// GetAnalysisThread는 store만 의존하므로 worker URL / clients 등 외부 의존성
// 없이 호출 가능.
func newDisplayTestService(t *testing.T) *DatasetService {
	t.Helper()
	memStore := store.NewMemoryStore()
	return &DatasetService{store: memStore}
}
