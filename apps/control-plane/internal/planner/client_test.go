package planner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/config"
)

func TestNewReturnsNilForStub(t *testing.T) {
	instance, err := New(config.Config{PlannerBackend: "stub"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance != nil {
		t.Fatalf("expected nil planner, got %#v", instance)
	}
}

func TestPythonAIPlannerGeneratePlan(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks/planner" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"plan": {
				"steps": [
					{
						"skill_name": "unstructured_issue_summary",
						"dataset_name": "issues.csv",
						"inputs": {"text_column": "text"}
					}
				],
				"notes": "planned by python"
			},
			"planner_type": "python-ai",
			"planner_model": "rule-based-v1",
			"planner_prompt_version": "planner-http-v1"
		}`))
	}))
	defer server.Close()

	model := "rule-based-v1"
	promptVersion := "planner-http-v1"
	client := PythonAIPlanner{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}

	result, err := client.GeneratePlan(context.Background(), testAnalysisInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.PlannerType != "python-ai" {
		t.Fatalf("unexpected planner type: %s", result.PlannerType)
	}
	if result.PlannerModel == nil || *result.PlannerModel != model {
		t.Fatalf("unexpected planner model: %+v", result.PlannerModel)
	}
	if result.PlannerPromptVersion == nil || *result.PlannerPromptVersion != promptVersion {
		t.Fatalf("unexpected prompt version: %+v", result.PlannerPromptVersion)
	}
	if len(result.Plan.Steps) != 1 {
		t.Fatalf("unexpected plan: %+v", result.Plan)
	}
	if result.Plan.Steps[0].SkillName != "unstructured_issue_summary" {
		t.Fatalf("unexpected skill: %s", result.Plan.Steps[0].SkillName)
	}
	if !strings.Contains(*result.Plan.Notes, "planned by python") {
		t.Fatalf("unexpected notes: %+v", result.Plan.Notes)
	}
}
