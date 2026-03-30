package service

import (
	"context"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/planner"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

func TestSubmitAnalysisUsesPlannerWhenConfigured(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, fakePlanner{
		result: planner.PlanGenerationResult{
			Plan: domain.SkillPlan{
				Steps: []domain.SkillPlanStep{
					{
						SkillName:   "unstructured_issue_summary",
						DatasetName: "issues.csv",
						Inputs: map[string]any{
							"text_column": "text",
						},
					},
				},
			},
			PlannerType: "python-ai",
		},
	})

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		EmbeddingStatus:  "not_requested",
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	dataType := "unstructured"
	datasetVersionID := version.DatasetVersionID
	response, err := service.SubmitAnalysis(project.ProjectID, domain.AnalysisSubmitRequest{
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "이슈를 요약해줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if response.Plan.PlannerType == nil || *response.Plan.PlannerType != "python-ai" {
		t.Fatalf("unexpected planner type: %+v", response.Plan.PlannerType)
	}
	if len(response.Plan.Plan.Steps) != 1 {
		t.Fatalf("unexpected plan steps: %+v", response.Plan.Plan.Steps)
	}
	if response.Plan.Plan.Steps[0].SkillName != "unstructured_issue_summary" {
		t.Fatalf("unexpected plan: %+v", response.Plan.Plan.Steps[0])
	}
}

func TestSubmitAnalysisBuildsDefaultUnstructuredPlanWithIssueEvidenceSummary(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		EmbeddingStatus:  "queued",
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	dataType := "unstructured"
	datasetVersionID := version.DatasetVersionID
	response, err := service.SubmitAnalysis(project.ProjectID, domain.AnalysisSubmitRequest{
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "VOC 이슈를 요약해줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if len(response.Plan.Plan.Steps) != 2 {
		t.Fatalf("unexpected plan steps: %+v", response.Plan.Plan.Steps)
	}
	if response.Plan.Plan.Steps[0].SkillName != "unstructured_issue_summary" {
		t.Fatalf("unexpected first step: %+v", response.Plan.Plan.Steps[0])
	}
	if response.Plan.Plan.Steps[1].SkillName != "issue_evidence_summary" {
		t.Fatalf("unexpected second step: %+v", response.Plan.Plan.Steps[1])
	}
	if response.Plan.Plan.Steps[1].Inputs["query"] != "VOC 이슈를 요약해줘" {
		t.Fatalf("unexpected evidence inputs: %+v", response.Plan.Plan.Steps[1].Inputs)
	}
}

func TestResumeExecutionTransitionsWaitingToQueued(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	if err := repository.SaveExecution(domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr("version-1"),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	reason := "embeddings ready"
	resumed, err := service.ResumeExecution(project.ProjectID, "exec-1", domain.ExecutionResumeRequest{Reason: &reason})
	if err != nil {
		t.Fatalf("unexpected resume error: %v", err)
	}
	if resumed.Status != "queued" {
		t.Fatalf("unexpected resumed status: %s", resumed.Status)
	}
	if len(resumed.Events) != 1 || resumed.Events[0].EventType != "RESUME_ENQUEUED" {
		t.Fatalf("unexpected resume events: %+v", resumed.Events)
	}
	if resumed.Events[0].Payload["workflow_id"] == "" {
		t.Fatalf("expected workflow_id in payload: %+v", resumed.Events[0].Payload)
	}
}

func stringPtr(value string) *string {
	return &value
}

type fakePlanner struct {
	result planner.PlanGenerationResult
	err    error
}

func (f fakePlanner) GeneratePlan(_ context.Context, _ domain.AnalysisSubmitRequest) (planner.PlanGenerationResult, error) {
	if f.err != nil {
		return planner.PlanGenerationResult{}, f.err
	}
	return f.result, nil
}
