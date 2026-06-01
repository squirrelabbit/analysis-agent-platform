package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-28 — frontend-safe analyze response projection 잠금.
// stateful path (`/datasets/{did}/analyze`, `/analysis_threads/{tid}/messages`)
// 응답에서 operator/debug 필드가 stripping되는지, DB는 full로 보존되는지 검증.

// fullAnalyzeWorkerResponse — worker가 반환할 수 있는 full result. operator/debug
// 필드(steps / planner / artifact_paths / composer.context_summary)를 모두 포함.
const fullAnalyzeWorkerResponse = `{
  "plan_version": "v2",
  "plan": {
    "plan_version": "v2",
    "steps": [
      {"id": "real_reviews", "skill": "filter", "params": {"input": "docs", "where": [{"column": "x", "op": "is_not_null"}]}, "inputs": "{\"input\":\"docs\"}"}
    ]
  },
  "steps": [
    {"step_id": "real_reviews", "skill": "filter", "row_count": 10, "sample_rows": [{"x": 1}], "extra": {}}
  ],
  "planner": {
    "prompt_version": "planner-v2-anthropic-v1",
    "attempts": [{"raw": "INTERNAL DRAFT TEXT"}],
    "usage": {"input_tokens": 1234, "output_tokens": 5678}
  },
  "artifact_paths": {
    "docs": "/workspace/data/internal/docs.parquet",
    "clauses": "/workspace/data/internal/clauses.jsonl",
    "genuineness": "/workspace/data/internal/genuineness.jsonl"
  },
  "taxonomy_check": {"status": "ok", "taxonomy_id": "festival-v2"},
  "composer": {
    "assistant_content": "전체 5건 중 3건을 표시했습니다.",
    "metadata": {"mode": "deterministic", "template": "table_normal", "fallback_reason": null},
    "display": {
      "type": "table",
      "title": "aspect별 카운트",
      "columns": ["aspect", "n"],
      "rows": [{"aspect": "food", "n": 2}],
      "total_rows": 5,
      "returned_rows": 3,
      "max_rows": 1000,
      "truncated": true,
      "warnings": ["전체 5건 중 3건만 표시했습니다."]
    },
    "context_summary": {"present_title": "aspect별 카운트", "answer_summary": "x", "question": "y"}
  }
}`

func TestProjectFrontendAnalyzeResult_KeepFieldsOnly(t *testing.T) {
	projected := projectFrontendAnalyzeResult(json.RawMessage(fullAnalyzeWorkerResponse))
	var got map[string]any
	if err := json.Unmarshal(projected, &got); err != nil {
		t.Fatalf("Unmarshal projected: %v", err)
	}

	// keep — 사용자 결정 2026-05-28
	for _, key := range []string{"plan", "composer", "taxonomy_check"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected projected result to keep %q, got keys %+v", key, mapKeys(got))
		}
	}

	// drop — operator/debug 필드
	for _, key := range []string{"steps", "planner", "artifact_paths"} {
		if _, ok := got[key]; ok {
			t.Fatalf("expected projected result to drop %q, but it was present", key)
		}
	}

	// plan: plan_version + steps[].id / .skill / .params 만
	plan, _ := got["plan"].(map[string]any)
	if plan["plan_version"] != "v2" {
		t.Fatalf("plan.plan_version: %v", plan["plan_version"])
	}
	steps, _ := plan["steps"].([]any)
	if len(steps) != 1 {
		t.Fatalf("plan.steps length: %d", len(steps))
	}
	step, _ := steps[0].(map[string]any)
	stepKeys := mapKeys(step)
	expectStepKeys := map[string]bool{"id": true, "skill": true, "params": true}
	for _, key := range stepKeys {
		if !expectStepKeys[key] {
			t.Fatalf("plan.steps[0] should not contain %q (only id/skill/params), got %+v", key, stepKeys)
		}
	}
	for key := range expectStepKeys {
		if _, ok := step[key]; !ok {
			t.Fatalf("plan.steps[0] missing %q: %+v", key, step)
		}
	}

	// composer: assistant_content / metadata.{mode,template,fallback_reason} /
	// display.{type,title,columns,rows,total_rows,returned_rows,max_rows,truncated,warnings}
	composer, _ := got["composer"].(map[string]any)
	if composer["assistant_content"] != "전체 5건 중 3건을 표시했습니다." {
		t.Fatalf("composer.assistant_content: %v", composer["assistant_content"])
	}
	if _, ok := composer["context_summary"]; ok {
		t.Fatalf("composer.context_summary must be stripped from frontend view")
	}
	meta, _ := composer["metadata"].(map[string]any)
	for _, key := range []string{"mode", "template", "fallback_reason"} {
		if _, ok := meta[key]; !ok {
			t.Fatalf("composer.metadata missing %q: %+v", key, meta)
		}
	}
	for _, extra := range mapKeys(meta) {
		switch extra {
		case "mode", "template", "fallback_reason":
		default:
			t.Fatalf("composer.metadata should only contain mode/template/fallback_reason, got extra %q", extra)
		}
	}
	display, _ := composer["display"].(map[string]any)
	for _, key := range []string{
		"type", "title", "columns", "rows",
		"total_rows", "returned_rows", "max_rows", "truncated", "warnings",
	} {
		if _, ok := display[key]; !ok {
			t.Fatalf("composer.display missing %q: %+v", key, display)
		}
	}

	// taxonomy_check 전체 객체 유지
	taxonomy, _ := got["taxonomy_check"].(map[string]any)
	if taxonomy["status"] != "ok" {
		t.Fatalf("taxonomy_check.status: %v", taxonomy["status"])
	}
}

func TestProjectFrontendAnalyzeResult_EmptyInput(t *testing.T) {
	if out := projectFrontendAnalyzeResult(nil); out != nil {
		t.Fatalf("nil input should pass through, got %q", string(out))
	}
	if out := projectFrontendAnalyzeResult(json.RawMessage(`{}`)); string(out) != `{}` {
		t.Fatalf("empty object should project to {}, got %q", string(out))
	}
}

// TestPostAnalysisThreadMessage_ResponseUsesFrontendView — 응답 view에 context_summary
// 등 운영자/debug 필드가 없고 DB에는 full로 보존된다는 잠금 (silverone 2026-05-28).
func TestPostAnalysisThreadMessage_ResponseUsesFrontendView(t *testing.T) {
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fullAnalyzeWorkerResponse))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()
	if err := fx.service.store.SaveDataset(domain.Dataset{
		ProjectID:              fx.projectID,
		DatasetID:              fx.datasetID,
		Name:                   "d",
		DataType:               "unstructured",
		ActiveDatasetVersionID: ptrString(fx.versionID),
	}); err != nil {
		t.Fatalf("save active dataset: %v", err)
	}

	resp, err := fx.service.AnalyzeDatasetAsNewThread(
		context.Background(),
		fx.projectID,
		fx.datasetID,
		AnalyzeRequest{UserQuestion: "aspect별 카운트 알려줘"},
	)
	if err != nil {
		t.Fatalf("AnalyzeDatasetAsNewThread: %v", err)
	}

	// 응답 result는 frontend-safe — plan / composer / taxonomy_check 외 필드는 없음.
	var resultObj map[string]any
	if err := json.Unmarshal(resp.Result, &resultObj); err != nil {
		t.Fatalf("Unmarshal resp.Result: %v", err)
	}
	for _, key := range []string{"steps", "planner", "artifact_paths"} {
		if _, ok := resultObj[key]; ok {
			t.Fatalf("response result should not include %q (operator/debug)", key)
		}
	}
	composer, _ := resultObj["composer"].(map[string]any)
	if _, ok := composer["context_summary"]; ok {
		t.Fatalf("response result.composer.context_summary should be stripped")
	}

	// run view에는 request_json / result_json 등이 없다 (struct 자체에서 제외).
	respJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(respJSON, &raw); err != nil {
		t.Fatalf("unmarshal response json: %v", err)
	}
	runObj, _ := raw["run"].(map[string]any)
	for _, key := range []string{"request_json", "result_json", "project_id", "dataset_id"} {
		if _, ok := runObj[key]; ok {
			t.Fatalf("run view should not contain %q, got %+v", key, mapKeys(runObj))
		}
	}
	for _, key := range []string{"run_id", "thread_id", "dataset_version_id", "user_message_id", "status", "created_at"} {
		if _, ok := runObj[key]; !ok {
			t.Fatalf("run view missing required key %q: %+v", key, mapKeys(runObj))
		}
	}
	assistantObj, _ := raw["assistant_message"].(map[string]any)
	if _, ok := assistantObj["context_summary"]; ok {
		t.Fatalf("assistant_message view should not contain context_summary")
	}

	// DB는 full 보존 — analysis_run.result_json + assistant_message.context_summary 둘 다.
	savedRun, err := fx.service.store.GetAnalysisRun(fx.projectID, resp.Run.RunID)
	if err != nil {
		t.Fatalf("GetAnalysisRun: %v", err)
	}
	if len(savedRun.ResultJSON) == 0 {
		t.Fatalf("DB run.result_json should preserve full result")
	}
	var savedResult map[string]any
	if err := json.Unmarshal(savedRun.ResultJSON, &savedResult); err != nil {
		t.Fatalf("unmarshal DB result_json: %v", err)
	}
	for _, key := range []string{"steps", "planner", "artifact_paths"} {
		if _, ok := savedResult[key]; !ok {
			t.Fatalf("DB result_json should preserve full %q (operator/debug)", key)
		}
	}
	messages, err := fx.service.store.ListAnalysisMessages(fx.projectID, resp.ThreadID)
	if err != nil {
		t.Fatalf("ListAnalysisMessages: %v", err)
	}
	for _, msg := range messages {
		if msg.Role == "assistant" && len(msg.ContextSummary) == 0 {
			t.Fatalf("DB assistant message should preserve context_summary, got empty")
		}
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
