package service

import (
	"context"
	"testing"
	"time"

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

func TestSubmitAnalysisEnrichesEmbeddingClusterInputs(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, fakePlanner{
		result: planner.PlanGenerationResult{
			Plan: domain.SkillPlan{
				Steps: []domain.SkillPlanStep{
					{
						SkillName:   "embedding_cluster",
						DatasetName: "dataset_from_version",
						Inputs:      map[string]any{},
					},
					{
						SkillName:   "issue_cluster_summary",
						DatasetName: "dataset_from_version",
						Inputs:      map[string]any{},
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
		Metadata: map[string]any{
			"raw_text_column": "text",
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("issues.prepared.parquet"),
		EmbeddingStatus: "ready",
		EmbeddingURI:    stringPtr("issues.embeddings.jsonl"),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	dataType := "unstructured"
	datasetVersionID := version.DatasetVersionID
	response, err := service.SubmitAnalysis(project.ProjectID, domain.AnalysisSubmitRequest{
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "주요 이슈 군집을 보여줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if got := response.Plan.Plan.Steps[0].Inputs["embedding_uri"]; got != "issues.embeddings.jsonl" {
		t.Fatalf("unexpected embedding uri: %+v", response.Plan.Plan.Steps[0].Inputs)
	}
	if got := response.Plan.Plan.Steps[0].Inputs["cluster_similarity_threshold"]; got != 0.3 {
		t.Fatalf("unexpected cluster threshold: %+v", response.Plan.Plan.Steps[0].Inputs)
	}
	if got := response.Plan.Plan.Steps[1].Inputs["text_column"]; got != "normalized_text" {
		t.Fatalf("unexpected cluster summary text column: %+v", response.Plan.Plan.Steps[1].Inputs)
	}
	if response.Plan.Plan.Steps[1].DatasetName != "issues.prepared.parquet" {
		t.Fatalf("unexpected cluster summary dataset name: %+v", response.Plan.Plan.Steps[1])
	}
}

func TestSubmitAnalysisEnrichesSemanticSearchChunkInputs(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, fakePlanner{
		result: planner.PlanGenerationResult{
			Plan: domain.SkillPlan{
				Steps: []domain.SkillPlanStep{
					{
						SkillName:   "semantic_search",
						DatasetName: "dataset_from_version",
						Inputs:      map[string]any{},
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
		Metadata: map[string]any{
			"prepared_ref":         "issues.prepared.parquet",
			"prepared_text_column": "normalized_text",
			"embedding_index_ref":  "pgvector://embedding_index_chunks?dataset_version_id=version-1",
			"chunk_ref":            "issues.chunks.parquet",
			"chunk_format":         "parquet",
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("issues.prepared.parquet"),
		EmbeddingStatus: "ready",
		EmbeddingURI:    stringPtr("issues.embeddings.jsonl"),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	dataType := "unstructured"
	datasetVersionID := version.DatasetVersionID
	response, err := service.SubmitAnalysis(project.ProjectID, domain.AnalysisSubmitRequest{
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "결제 오류 관련 근거를 찾아줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if len(response.Plan.Plan.Steps) != 1 {
		t.Fatalf("unexpected plan steps: %+v", response.Plan.Plan.Steps)
	}
	step := response.Plan.Plan.Steps[0]
	if step.DatasetName != "issues.prepared.parquet" {
		t.Fatalf("unexpected semantic search dataset name: %+v", step)
	}
	if got := step.Inputs["chunk_ref"]; got != "issues.chunks.parquet" {
		t.Fatalf("unexpected chunk ref: %+v", step.Inputs)
	}
	if got := step.Inputs["embedding_index_ref"]; got != "pgvector://embedding_index_chunks?dataset_version_id=version-1" {
		t.Fatalf("unexpected embedding index ref: %+v", step.Inputs)
	}
	if got := step.Inputs["chunk_format"]; got != "parquet" {
		t.Fatalf("unexpected chunk format: %+v", step.Inputs)
	}
	if _, ok := step.Inputs["embedding_uri"]; ok {
		t.Fatalf("semantic search should prefer pgvector metadata without embedding_uri fallback: %+v", step.Inputs)
	}
}

func TestBuildExecutionResultIncludesUsageSummary(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID: "exec-usage",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "completed",
		Artifacts: map[string]string{
			"step:step-1:unstructured_issue_summary": `{"skill_name":"unstructured_issue_summary","usage":{"provider":"anthropic","model":"claude-haiku","operation":"unstructured_issue_summary","request_count":1,"input_tokens":100,"output_tokens":20,"total_tokens":120,"cost_estimation_status":"not_configured"}}`,
			"step:step-2:evidence_pack":              `{"skill_name":"evidence_pack","usage":{"provider":"anthropic","model":"claude-haiku","operation":"evidence_pack","request_count":1,"input_tokens":60,"output_tokens":30,"total_tokens":90,"cost_estimation_status":"not_configured"}}`,
		},
		Plan: domain.SkillPlan{
			PlanID: "plan-usage",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "unstructured_issue_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "evidence_pack", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	result, err := service.BuildExecutionResult(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected build execution result error: %v", err)
	}

	usage, ok := result.Contract["usage_summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected usage summary contract: %+v", result.Contract)
	}
	if totalTokens, ok := usage["total_tokens"].(int); !ok || totalTokens != 210 {
		t.Fatalf("unexpected total tokens: %+v", usage)
	}
	if requestCount, ok := usage["request_count"].(int); !ok || requestCount != 2 {
		t.Fatalf("unexpected request count: %+v", usage)
	}
	if provider, ok := usage["provider"].(string); !ok || provider != "anthropic" {
		t.Fatalf("unexpected provider: %+v", usage)
	}
	if operation, ok := usage["operation"].(string); !ok || operation != "mixed" {
		t.Fatalf("unexpected operation: %+v", usage)
	}
	if result.ResultV1.SchemaVersion != "execution-result-v1" {
		t.Fatalf("unexpected result v1 schema version: %+v", result.ResultV1)
	}
	if result.ResultV1.Answer == nil {
		t.Fatalf("expected result v1 answer: %+v", result.ResultV1)
	}
	if result.ResultV1.PrimarySkillName == nil || *result.ResultV1.PrimarySkillName != "evidence_pack" {
		t.Fatalf("unexpected primary skill: %+v", result.ResultV1)
	}
	if result.ResultV1.UsageSummary["total_tokens"] != 210 {
		t.Fatalf("unexpected result v1 usage summary: %+v", result.ResultV1.UsageSummary)
	}
}

func TestBuildExecutionResultIncludesWaitingState(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	execution := domain.ExecutionSummary{
		ExecutionID: "exec-waiting",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "waiting",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-waiting",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "semantic_search", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: "exec-waiting",
				TS:          now,
				Level:       "info",
				EventType:   "WORKFLOW_WAITING",
				Message:     "execution is waiting for dependency",
				Payload: map[string]any{
					"waiting_for": "embeddings",
					"reason":      "embedding index is not ready",
				},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	result, err := service.BuildExecutionResult(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected build execution result error: %v", err)
	}

	if result.ResultV1.Waiting == nil {
		t.Fatalf("expected waiting state: %+v", result.ResultV1)
	}
	if result.ResultV1.Waiting.WaitingFor != "embeddings" {
		t.Fatalf("unexpected waiting_for: %+v", result.ResultV1.Waiting)
	}
	if len(result.ResultV1.StepResults) != 2 {
		t.Fatalf("unexpected step results: %+v", result.ResultV1.StepResults)
	}
	if result.ResultV1.StepResults[0].Status != "pending" {
		t.Fatalf("unexpected pending step status: %+v", result.ResultV1.StepResults)
	}
}

func TestSubmitAnalysisEnrichesIssueSentimentSummaryInputs(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, fakePlanner{
		result: planner.PlanGenerationResult{
			Plan: domain.SkillPlan{
				Steps: []domain.SkillPlanStep{
					{
						SkillName:   "issue_sentiment_summary",
						DatasetName: "dataset_from_version",
						Inputs:      map[string]any{},
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
		Metadata: map[string]any{
			"prepared_ref":           "issues.prepared.parquet",
			"prepared_text_column":   "normalized_text",
			"sentiment_label_column": "sentiment_label",
			"row_id_column":          "row_id",
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("issues.prepared.parquet"),
		SentimentStatus: "ready",
		SentimentURI:    stringPtr("issues.sentiment.parquet"),
		EmbeddingStatus: "not_requested",
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	dataType := "unstructured"
	datasetVersionID := version.DatasetVersionID
	response, err := service.SubmitAnalysis(project.ProjectID, domain.AnalysisSubmitRequest{
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "감성 분포를 보여줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if len(response.Plan.Plan.Steps) != 1 {
		t.Fatalf("unexpected plan steps: %+v", response.Plan.Plan.Steps)
	}
	step := response.Plan.Plan.Steps[0]
	if step.DatasetName != "issues.sentiment.parquet" {
		t.Fatalf("unexpected sentiment dataset name: %+v", step)
	}
	if got := step.Inputs["prepared_dataset_name"]; got != "issues.prepared.parquet" {
		t.Fatalf("unexpected prepared dataset input: %+v", step.Inputs)
	}
	if got := step.Inputs["text_column"]; got != "normalized_text" {
		t.Fatalf("unexpected text column: %+v", step.Inputs)
	}
	if got := step.Inputs["sentiment_column"]; got != "sentiment_label" {
		t.Fatalf("unexpected sentiment column: %+v", step.Inputs)
	}
	if got := step.Inputs["row_id_column"]; got != "row_id" {
		t.Fatalf("unexpected row id column: %+v", step.Inputs)
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
