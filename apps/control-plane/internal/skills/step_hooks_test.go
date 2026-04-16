package skills

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestExecutionProgressHookPersistsStepEventsAndArtifacts(t *testing.T) {
	repository := store.NewMemoryStore()
	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID: "exec-1",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "running",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	hook := ExecutionProgressHook{
		Repo: repository,
		Now: func() time.Time {
			return time.Date(2026, 4, 9, 9, 0, 0, 0, time.UTC)
		},
	}
	step := execution.Plan.Steps[0]

	if _, err := hook.BeforeStep(context.Background(), execution, step); err != nil {
		t.Fatalf("unexpected before step error: %v", err)
	}
	if _, err := hook.AfterStep(context.Background(), execution, step, StepHookOutcome{
		Status:         "completed",
		ArtifactBytes:  32,
		ArtifactRef:    "/tmp/artifact.json",
		StoredArtifact: `{"skill_name":"issue_evidence_summary","summary":"partial"}`,
	}); err != nil {
		t.Fatalf("unexpected after step error: %v", err)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}

	if len(current.Events) != 2 {
		t.Fatalf("unexpected event count: %+v", current.Events)
	}
	if current.Events[0].EventType != "STEP_STARTED" || current.Events[1].EventType != "STEP_COMPLETED" {
		t.Fatalf("unexpected step events: %+v", current.Events)
	}
	if _, ok := current.Artifacts["step:step-1:issue_evidence_summary"]; !ok {
		t.Fatalf("expected partial artifact to be stored: %+v", current.Artifacts)
	}
}

func TestExecutionProgressHookCompactsLargeArtifactForStorage(t *testing.T) {
	repository := store.NewMemoryStore()
	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID: "exec-compact",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "running",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "embedding_cluster", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	rawArtifact := `{
		"skill_name":"embedding_cluster",
		"summary":{"cluster_count":2,"clustered_document_count":24},
		"cluster_ref":"/tmp/issues.clusters.json",
		"cluster_execution_mode":"materialized_full_dataset",
		"clusters":[
			{"cluster_id":"cluster-1","label":"결제 오류","document_count":12,"members":[{"chunk_id":"c1"},{"chunk_id":"c2"}],"sample_documents":[{"text":"결제 오류가 반복 발생했습니다"}]},
			{"cluster_id":"cluster-2","label":"로그인 실패","document_count":8,"members":[{"chunk_id":"c3"}],"sample_documents":[{"text":"로그인이 자주 실패합니다"}]}
		]
	}`

	hook := ExecutionProgressHook{
		Repo: repository,
		Now:  func() time.Time { return time.Date(2026, 4, 9, 9, 0, 0, 0, time.UTC) },
	}
	step := execution.Plan.Steps[0]
	if _, err := hook.AfterStep(context.Background(), execution, step, StepHookOutcome{
		Status:         "completed",
		StoredArtifact: rawArtifact,
	}); err != nil {
		t.Fatalf("unexpected after step error: %v", err)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}

	stored := current.Artifacts["step:step-1:embedding_cluster"]
	var decoded map[string]any
	if err := json.Unmarshal([]byte(stored), &decoded); err != nil {
		t.Fatalf("unexpected artifact decode error: %v", err)
	}
	if decoded["cluster_execution_mode"] != "materialized_full_dataset" {
		t.Fatalf("unexpected compacted artifact mode: %+v", decoded)
	}
	clusters, ok := decoded["clusters"].([]any)
	if !ok || len(clusters) != 2 {
		t.Fatalf("unexpected compacted clusters: %+v", decoded)
	}
	first, ok := clusters[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected first cluster payload: %+v", clusters[0])
	}
	if _, exists := first["members"]; exists {
		t.Fatalf("expected members to be removed from compacted artifact: %+v", first)
	}
}
