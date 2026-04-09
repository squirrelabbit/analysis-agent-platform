package skills

import (
	"context"
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
