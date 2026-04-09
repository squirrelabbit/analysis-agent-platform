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

type fakeExecutionDependencyBuilder struct {
	repo            store.Repository
	calls           []string
	prepareStatus   string
	sentimentStatus string
	embeddingStatus string
	clusterStatus   string
	clusterRequests []domain.DatasetClusterBuildRequest
}

func (b *fakeExecutionDependencyBuilder) CreatePrepareJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetPrepareRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "prepare")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.PrepareStatus = builderStatusOrDefault(b.prepareStatus)
	if version.PrepareStatus == "ready" {
		uri := "prepared.parquet"
		version.PrepareURI = &uri
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["prepared_text_column"] = "normalized_text"
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-prepare", BuildType: "prepare", Status: version.PrepareStatus}, nil
}

func (b *fakeExecutionDependencyBuilder) CreateSentimentJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetSentimentBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "sentiment")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.SentimentStatus = builderStatusOrDefault(b.sentimentStatus)
	if version.SentimentStatus == "ready" {
		uri := "sentiment.parquet"
		version.SentimentURI = &uri
	}
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-sentiment", BuildType: "sentiment", Status: version.SentimentStatus}, nil
}

func (b *fakeExecutionDependencyBuilder) CreateEmbeddingJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetEmbeddingBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "embedding")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.EmbeddingStatus = builderStatusOrDefault(b.embeddingStatus)
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	if version.EmbeddingStatus == "ready" {
		version.Metadata["embedding_index_source_ref"] = "embeddings.index.parquet"
	}
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-embedding", BuildType: "embedding", Status: version.EmbeddingStatus}, nil
}

func (b *fakeExecutionDependencyBuilder) CreateClusterJob(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "cluster")
	b.clusterRequests = append(b.clusterRequests, input)
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	status := builderStatusOrDefault(b.clusterStatus)
	version.Metadata["cluster_status"] = status
	if status == "ready" {
		version.Metadata["cluster_ref"] = "clusters.json"
		version.Metadata["cluster_format"] = "json"
		if input.SimilarityThreshold != nil {
			version.Metadata["cluster_similarity_threshold"] = *input.SimilarityThreshold
		}
		if input.TopN != nil {
			version.Metadata["cluster_top_n"] = *input.TopN
		}
		if input.SampleN != nil {
			version.Metadata["cluster_sample_n"] = *input.SampleN
		}
	}
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-cluster", BuildType: "cluster", Status: status}, nil
}

type lazyExecutionDependencyBuilder struct {
	repo  store.Repository
	calls []string
}

func builderStatusOrDefault(status string) string {
	if status == "" {
		return "queued"
	}
	return status
}

func (b *lazyExecutionDependencyBuilder) CreatePrepareJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetPrepareRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "prepare")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.PrepareStatus = "queued"
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-prepare", BuildType: "prepare", Status: version.PrepareStatus}, nil
}

func (b *lazyExecutionDependencyBuilder) CreateSentimentJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetSentimentBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "sentiment")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.SentimentStatus = "queued"
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-sentiment", BuildType: "sentiment", Status: version.SentimentStatus}, nil
}

func (b *lazyExecutionDependencyBuilder) CreateEmbeddingJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetEmbeddingBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "embedding")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	version.EmbeddingStatus = "queued"
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-embedding", BuildType: "embedding", Status: version.EmbeddingStatus}, nil
}

func (b *lazyExecutionDependencyBuilder) CreateClusterJob(projectID, datasetID, datasetVersionID string, _ domain.DatasetClusterBuildRequest, _ string) (domain.DatasetBuildJob, error) {
	b.calls = append(b.calls, "cluster")
	version, err := b.repo.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["cluster_status"] = "queued"
	if err := b.repo.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return domain.DatasetBuildJob{JobID: "job-cluster", BuildType: "cluster", Status: "queued"}, nil
}

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

func TestExecutePlanAutoBuildsRequiredDependencies(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo:            repository,
		prepareStatus:   "queued",
		sentimentStatus: "queued",
		embeddingStatus: "queued",
	}
	service.SetDependencyBuilder(builder)

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
		PrepareStatus:    "queued",
		SentimentStatus:  "queued",
		EmbeddingStatus:  "queued",
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}
	plan := domain.PlanRecord{
		PlanID:           "plan-1",
		RequestID:        "request-1",
		ProjectID:        project.ProjectID,
		DatasetName:      "issues.csv",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "garbage_filter", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_sentiment_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-3", SkillName: "semantic_search", DatasetName: "issues.csv", Inputs: map[string]any{"query": "결제 오류"}},
			},
		},
	}
	if err := repository.SavePlan(plan); err != nil {
		t.Fatalf("unexpected save plan error: %v", err)
	}

	response, err := service.ExecutePlan(project.ProjectID, plan.PlanID)
	if err != nil {
		t.Fatalf("unexpected execute plan error: %v", err)
	}

	if len(builder.calls) != 1 {
		t.Fatalf("unexpected dependency calls: %+v", builder.calls)
	}
	if builder.calls[0] != "prepare" {
		t.Fatalf("unexpected dependency order: %+v", builder.calls)
	}
	if response.Execution.Status != "queued" {
		t.Fatalf("unexpected execution status: %s", response.Execution.Status)
	}

	updatedVersion, err := repository.GetDatasetVersion(project.ProjectID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	if updatedVersion.PrepareStatus != "queued" {
		t.Fatalf("unexpected updated version: %+v", updatedVersion)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionQueuesNextDependencyWhenStillMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo:            repository,
		prepareStatus:   "ready",
		sentimentStatus: "queued",
	}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		SentimentStatus:  "queued",
		EmbeddingStatus:  "not_requested",
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "garbage_filter", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_sentiment_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: prepare", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 0 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}

	resumed, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if resumed.Status != "waiting" {
		t.Fatalf("expected waiting execution, got %s", resumed.Status)
	}
	if len(builder.calls) != 1 || builder.calls[0] != "sentiment" {
		t.Fatalf("unexpected dependency calls: %+v", builder.calls)
	}
	if len(resumed.Events) != 0 {
		t.Fatalf("expected no resume event, got %+v", resumed.Events)
	}
	updatedVersion, err := repository.GetDatasetVersion(project.ProjectID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	if updatedVersion.SentimentStatus != "queued" {
		t.Fatalf("expected queued sentiment status, got %+v", updatedVersion)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionAutoResumesReadyExecution(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo:            repository,
		prepareStatus:   "ready",
		sentimentStatus: "ready",
	}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		SentimentStatus:  "ready",
		SentimentURI:     stringPtr("sentiment.parquet"),
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "garbage_filter", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_sentiment_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: prepare", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 1 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if current.Status != "queued" {
		t.Fatalf("expected queued execution, got %s", current.Status)
	}
	if len(current.Events) != 1 || current.Events[0].EventType != "RESUME_ENQUEUED" {
		t.Fatalf("unexpected resume event: %+v", current.Events)
	}
	if current.Events[0].Payload["triggered_by"] != "dataset_build_job" {
		t.Fatalf("unexpected resume payload: %+v", current.Events[0].Payload)
	}
	if len(builder.calls) != 0 {
		t.Fatalf("expected no dependency enqueue, got %+v", builder.calls)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionQueuesClusterWhenMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo:          repository,
		clusterStatus: "queued",
	}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"prepared_text_column":       "normalized_text",
			"embedding_index_source_ref": "embeddings.index.parquet",
			"chunk_ref":                  "issues.chunks.parquet",
			"cluster_status":             "not_requested",
		},
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "embedding_cluster", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_cluster_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: embedding", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 0 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}
	if len(builder.calls) != 1 || builder.calls[0] != "cluster" {
		t.Fatalf("unexpected dependency calls: %+v", builder.calls)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if current.Status != "waiting" {
		t.Fatalf("expected waiting execution, got %s", current.Status)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionQueuesClusterWhenParamsMismatch(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo:          repository,
		clusterStatus: "queued",
	}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"prepared_text_column":         "normalized_text",
			"embedding_index_source_ref":   "embeddings.index.parquet",
			"chunk_ref":                    "issues.chunks.parquet",
			"cluster_status":               "ready",
			"cluster_ref":                  "issues.clusters.json",
			"cluster_format":               "json",
			"cluster_similarity_threshold": 0.3,
			"cluster_top_n":                10,
			"cluster_sample_n":             3,
		},
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{
					StepID:      "step-1",
					SkillName:   "embedding_cluster",
					DatasetName: "issues.csv",
					Inputs: map[string]any{
						"cluster_similarity_threshold": 0.2,
						"top_n":                        3,
						"sample_n":                     2,
					},
				},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: embedding", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 0 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}
	if len(builder.calls) != 1 || builder.calls[0] != "cluster" {
		t.Fatalf("unexpected dependency calls: %+v", builder.calls)
	}
	if len(builder.clusterRequests) != 1 {
		t.Fatalf("expected cluster request capture, got %+v", builder.clusterRequests)
	}
	request := builder.clusterRequests[0]
	if request.SimilarityThreshold == nil || *request.SimilarityThreshold != 0.2 {
		t.Fatalf("unexpected cluster threshold request: %+v", request)
	}
	if request.TopN == nil || *request.TopN != 3 || request.SampleN == nil || *request.SampleN != 2 {
		t.Fatalf("unexpected cluster request shape: %+v", request)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionSkipsClusterMaterializationForSubsetPipeline(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{
		repo: repository,
	}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"prepared_text_column":       "normalized_text",
			"embedding_index_source_ref": "embeddings.index.parquet",
			"embedding_index_ref":        "pgvector://embedding_index_chunks?dataset_version_id=version-1",
			"chunk_ref":                  "issues.chunks.parquet",
			"cluster_status":             "not_requested",
		},
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "document_filter", DatasetName: "issues.csv", Inputs: map[string]any{"query": "결제"}},
				{StepID: "step-2", SkillName: "embedding_cluster", DatasetName: "issues.csv", Inputs: map[string]any{"cluster_similarity_threshold": 0.2}},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: embedding", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 1 {
		t.Fatalf("expected subset pipeline execution to resume without cluster build, got %d", resumedCount)
	}
	if len(builder.clusterRequests) != 0 {
		t.Fatalf("expected no cluster materialization request for subset pipeline, got %+v", builder.clusterRequests)
	}
	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if current.Status != "queued" {
		t.Fatalf("expected queued execution, got %s", current.Status)
	}
}

func TestResumeWaitingExecutionsForDatasetVersionAutoResumesReadyClusterExecution(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)
	builder := &fakeExecutionDependencyBuilder{repo: repository}
	service.SetDependencyBuilder(builder)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"prepared_text_column":       "normalized_text",
			"embedding_index_source_ref": "embeddings.index.parquet",
			"chunk_ref":                  "issues.chunks.parquet",
			"cluster_status":             "ready",
			"cluster_ref":                "issues.clusters.json",
			"cluster_format":             "json",
		},
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "embedding_cluster", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: cluster", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 1 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if current.Status != "queued" {
		t.Fatalf("expected queued execution, got %s", current.Status)
	}
	if len(current.Events) != 1 || current.Events[0].EventType != "RESUME_ENQUEUED" {
		t.Fatalf("unexpected resume event: %+v", current.Events)
	}
	if len(builder.calls) != 0 {
		t.Fatalf("expected no dependency enqueue, got %+v", builder.calls)
	}
}

func TestResumeWaitingExecutionsRefreshesPlanDatasetSources(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured"}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_ref":           "/tmp/issues.prepared.parquet",
			"prepared_text_column":   "normalized_text",
			"sentiment_label_column": "sentiment_label",
			"row_id_column":          "row_id",
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus: "ready",
		SentimentURI:    stringPtr("/tmp/sentiment/sentiment.parquet"),
	}
	_ = repository.SaveDatasetVersion(version)
	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{
					StepID:      "step-1",
					SkillName:   "issue_sentiment_summary",
					DatasetName: "/tmp/issues.prepared.parquet.sentiment.parquet",
					Inputs: map[string]any{
						"prepared_dataset_name": "/tmp/issues.prepared.parquet.stale",
						"text_column":           "text",
					},
				},
			},
		},
	}
	_ = repository.SaveExecution(execution)

	resumedCount, err := service.ResumeWaitingExecutionsForDatasetVersion(project.ProjectID, version.DatasetVersionID, "dataset build completed: sentiment", "dataset_build_job")
	if err != nil {
		t.Fatalf("unexpected auto resume error: %v", err)
	}
	if resumedCount != 1 {
		t.Fatalf("unexpected resumed count: %d", resumedCount)
	}

	current, err := repository.GetExecution(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	step := current.Plan.Steps[0]
	if step.DatasetName != "/tmp/sentiment/sentiment.parquet" {
		t.Fatalf("unexpected sentiment dataset source: %+v", step)
	}
	if got := step.Inputs["prepared_dataset_name"]; got != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected prepared dataset source: %+v", step.Inputs)
	}
	if got := step.Inputs["text_column"]; got != "normalized_text" {
		t.Fatalf("unexpected text column: %+v", step.Inputs)
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
			"raw_text_column":     "text",
			"embedding_index_ref": "pgvector://embedding_index_chunks?dataset_version_id=version-1",
			"chunk_ref":           "issues.chunks.parquet",
			"chunk_format":        "parquet",
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("issues.prepared.parquet"),
		EmbeddingStatus: "ready",
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

	if got := response.Plan.Plan.Steps[0].Inputs["embedding_index_ref"]; got != "pgvector://embedding_index_chunks?dataset_version_id=version-1" {
		t.Fatalf("unexpected embedding index ref: %+v", response.Plan.Plan.Steps[0].Inputs)
	}
	if got := response.Plan.Plan.Steps[0].Inputs["chunk_ref"]; got != "issues.chunks.parquet" {
		t.Fatalf("unexpected chunk ref: %+v", response.Plan.Plan.Steps[0].Inputs)
	}
	if _, ok := response.Plan.Plan.Steps[0].Inputs["embedding_uri"]; ok {
		t.Fatalf("embedding cluster should prefer pgvector metadata without embedding_uri fallback: %+v", response.Plan.Plan.Steps[0].Inputs)
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

func TestSubmitAnalysisEnrichesGarbageFilterFromDatasetProfile(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, fakePlanner{
		result: planner.PlanGenerationResult{
			Plan: domain.SkillPlan{
				Steps: []domain.SkillPlanStep{
					{
						SkillName:   "garbage_filter",
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
		Metadata:         map[string]any{},
		Profile: &domain.DatasetProfile{
			ProfileID:        "festival-default",
			GarbageRuleNames: []string{"ad_marker", "platform_placeholder"},
		},
		PrepareStatus:   "ready",
		PrepareURI:      stringPtr("issues.prepared.parquet"),
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
		Goal:             "광고 문서를 제거해줘",
	})
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	step := response.Plan.Plan.Steps[0]
	rules, ok := step.Inputs["garbage_rule_names"].([]string)
	if ok {
		if len(rules) != 2 || rules[0] != "ad_marker" || rules[1] != "platform_placeholder" {
			t.Fatalf("unexpected garbage rule names: %+v", rules)
		}
		return
	}
	rulesAny, ok := step.Inputs["garbage_rule_names"].([]any)
	if !ok || len(rulesAny) != 2 {
		t.Fatalf("unexpected garbage rule payload: %+v", step.Inputs)
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

func TestBuildExecutionResultClearsStaleWaitingDiagnosticsAfterCompletion(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	execution := domain.ExecutionSummary{
		ExecutionID: "exec-completed",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "completed",
		Artifacts: map[string]string{
			"step:step-1:issue_evidence_summary": `{"skill_name":"issue_evidence_summary","summary":"done"}`,
		},
		Plan: domain.SkillPlan{
			PlanID: "plan-completed",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: "exec-completed",
				TS:          now,
				Level:       "info",
				EventType:   "WORKFLOW_WAITING",
				Message:     "execution is waiting for dependency",
				Payload: map[string]any{
					"waiting_for": "embeddings",
					"reason":      "embedding index is not ready",
				},
			},
			{
				ExecutionID: "exec-completed",
				TS:          now.Add(time.Minute),
				Level:       "info",
				EventType:   "WORKFLOW_COMPLETED",
				Message:     "execution completed",
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

	if result.Diagnostics == nil {
		t.Fatalf("expected diagnostics: %+v", result)
	}
	if result.Diagnostics.Waiting != nil {
		t.Fatalf("expected stale waiting diagnostics to be cleared: %+v", result.Diagnostics)
	}
	if result.ResultV1.Waiting != nil {
		t.Fatalf("expected result_v1 waiting to stay empty for completed execution: %+v", result.ResultV1)
	}
}

func TestBuildExecutionProgressReportsRunningStepAndPreview(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	execution := domain.ExecutionSummary{
		ExecutionID: "exec-progress",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "running",
		Artifacts: map[string]string{
			"step:step-1:issue_evidence_summary": `{"skill_name":"issue_evidence_summary","step_id":"step-1","summary":"partial summary","selection_source":"semantic_search","citation_mode":"chunk","key_findings":["finding-a"]}`,
		},
		Plan: domain.SkillPlan{
			PlanID: "plan-progress",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
				{StepID: "step-2", SkillName: "issue_cluster_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: "exec-progress",
				TS:          now,
				Level:       "info",
				EventType:   "WORKFLOW_STARTED",
				Message:     "execution started",
			},
			{
				ExecutionID: "exec-progress",
				TS:          now.Add(time.Second),
				Level:       "info",
				EventType:   "STEP_STARTED",
				Message:     "step started",
				Payload: map[string]any{
					"step_id":    "step-1",
					"skill_name": "issue_evidence_summary",
				},
			},
			{
				ExecutionID: "exec-progress",
				TS:          now.Add(2 * time.Second),
				Level:       "info",
				EventType:   "STEP_COMPLETED",
				Message:     "step completed",
				Payload: map[string]any{
					"step_id":      "step-1",
					"skill_name":   "issue_evidence_summary",
					"artifact_key": "step:step-1:issue_evidence_summary",
				},
			},
			{
				ExecutionID: "exec-progress",
				TS:          now.Add(3 * time.Second),
				Level:       "info",
				EventType:   "STEP_STARTED",
				Message:     "step started",
				Payload: map[string]any{
					"step_id":    "step-2",
					"skill_name": "issue_cluster_summary",
				},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	progress, err := service.BuildExecutionProgress(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected build execution progress error: %v", err)
	}

	if progress.TotalSteps != 2 || progress.CompletedSteps != 1 || progress.FailedSteps != 0 {
		t.Fatalf("unexpected progress counters: %+v", progress)
	}
	if progress.RunningStep == nil || progress.RunningStep.StepID != "step-2" {
		t.Fatalf("unexpected running step: %+v", progress.RunningStep)
	}
	if progress.RunningStep.StartedAt == nil || !progress.RunningStep.StartedAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("unexpected running step start time: %+v", progress.RunningStep)
	}
	if progress.LastEventAt == nil || !progress.LastEventAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("unexpected last_event_at: %+v", progress)
	}
	if len(progress.Steps) != 2 || progress.Steps[0].Status != "completed" || progress.Steps[1].Status != "running" {
		t.Fatalf("unexpected step progress: %+v", progress.Steps)
	}
	if progress.Steps[0].CompletedAt == nil || !progress.Steps[0].CompletedAt.Equal(now.Add(2*time.Second)) {
		t.Fatalf("unexpected completed step timestamps: %+v", progress.Steps[0])
	}
	if progress.ResultPreview == nil || progress.ResultPreview.Summary != "partial summary" {
		t.Fatalf("unexpected result preview: %+v", progress.ResultPreview)
	}
}

func TestBuildExecutionProgressIncludesBuildDependencies(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       stringPointer("prepared.parquet"),
		EmbeddingStatus:  "queued",
		CreatedAt:        time.Now().UTC(),
		Metadata:         map[string]any{},
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	startedAt := time.Now().UTC()
	buildJob := domain.DatasetBuildJob{
		JobID:            "job-embedding-1",
		ProjectID:        project.ProjectID,
		DatasetID:        dataset.DatasetID,
		DatasetVersionID: version.DatasetVersionID,
		BuildType:        "embedding",
		Status:           "running",
		TriggeredBy:      "analysis_execute",
		CreatedAt:        startedAt.Add(-time.Minute),
		StartedAt:        &startedAt,
		Attempt:          1,
	}
	if err := repository.SaveDatasetBuildJob(buildJob); err != nil {
		t.Fatalf("unexpected save dataset build job error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID:      "exec-waiting-progress",
		ProjectID:        project.ProjectID,
		RequestID:        "request-1",
		Status:           "waiting",
		DatasetVersionID: stringPointer(version.DatasetVersionID),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-waiting-progress",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "semantic_search", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: "exec-waiting-progress",
				TS:          startedAt,
				Level:       "info",
				EventType:   "WORKFLOW_WAITING",
				Message:     "execution is waiting for dependency",
				Payload: map[string]any{
					"waiting_for": "embeddings",
					"reason":      "dataset version embeddings are not ready",
				},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	progress, err := service.BuildExecutionProgress(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected build execution progress error: %v", err)
	}

	if len(progress.BuildDependencies) != 2 {
		t.Fatalf("expected prepare+embedding dependencies, got %+v", progress.BuildDependencies)
	}
	if progress.BuildDependencies[0].BuildType != "prepare" || !progress.BuildDependencies[0].Ready || progress.BuildDependencies[0].Status != "ready" {
		t.Fatalf("unexpected prepare dependency: %+v", progress.BuildDependencies[0])
	}
	if progress.BuildDependencies[1].BuildType != "embedding" || progress.BuildDependencies[1].Ready {
		t.Fatalf("unexpected embedding dependency readiness: %+v", progress.BuildDependencies[1])
	}
	if !progress.BuildDependencies[1].WaitingFor || progress.BuildDependencies[1].LatestJob == nil || progress.BuildDependencies[1].LatestJob.JobID != "job-embedding-1" {
		t.Fatalf("expected waiting embedding job in progress response: %+v", progress.BuildDependencies[1])
	}
}

func TestBuildExecutionEventsReturnsTimeline(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	execution := domain.ExecutionSummary{
		ExecutionID: "exec-events",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "running",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-events",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: "exec-events",
				TS:          now,
				Level:       "info",
				EventType:   "WORKFLOW_STARTED",
				Message:     "execution started",
			},
			{
				ExecutionID: "exec-events",
				TS:          now.Add(time.Second),
				Level:       "info",
				EventType:   "STEP_STARTED",
				Message:     "step started",
				Payload: map[string]any{
					"step_id":    "step-1",
					"skill_name": "issue_evidence_summary",
				},
			},
		},
	}
	if err := repository.SaveExecution(execution); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	response, err := service.BuildExecutionEvents(project.ProjectID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("unexpected build execution events error: %v", err)
	}

	if response.ExecutionID != execution.ExecutionID || response.Status != "running" {
		t.Fatalf("unexpected execution events response: %+v", response)
	}
	if response.EventCount != 2 || len(response.Events) != 2 {
		t.Fatalf("unexpected event count: %+v", response)
	}
	if response.Events[1].EventType != "STEP_STARTED" {
		t.Fatalf("unexpected latest event: %+v", response.Events[1])
	}
}

func TestBuildExecutionResultUsesStoredSnapshotWhenPresent(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID: "exec-snapshot",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "completed",
		Artifacts: map[string]string{
			"step:step-1:issue_evidence_summary": `{"skill_name":"issue_evidence_summary","summary":"ephemeral"}`,
		},
		Plan: domain.SkillPlan{
			PlanID: "plan-snapshot",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		ResultV1Snapshot: &domain.ExecutionResultV1{
			SchemaVersion:    "execution-result-v1",
			Status:           "completed",
			PrimarySkillName: stringPtr("issue_evidence_summary"),
			Answer: &domain.ExecutionResultAnswer{
				Summary: "stored snapshot summary",
			},
			StepResults: []domain.ExecutionStepResultV1{
				{StepID: "step-1", SkillName: "issue_evidence_summary", Status: "completed"},
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

	if result.ResultV1.Answer == nil || result.ResultV1.Answer.Summary != "stored snapshot summary" {
		t.Fatalf("expected stored snapshot to be returned: %+v", result.ResultV1)
	}
}

func TestBuildExecutionResultBuildsFallbackFinalAnswer(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	execution := domain.ExecutionSummary{
		ExecutionID: "exec-final-answer",
		ProjectID:   project.ProjectID,
		RequestID:   "request-1",
		Status:      "completed",
		Artifacts: map[string]string{
			"step:step-1:issue_evidence_summary": `{"skill_name":"issue_evidence_summary","summary":"근거 기반 요약","key_findings":["핵심 포인트"],"evidence":[{"snippet":"근거 snippet"}]}`,
		},
		Plan: domain.SkillPlan{
			PlanID: "plan-final-answer",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
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
	if result.FinalAnswer == nil {
		t.Fatalf("expected final answer fallback: %+v", result)
	}
	if result.FinalAnswer.GenerationMode != "fallback" {
		t.Fatalf("unexpected final answer generation mode: %+v", result.FinalAnswer)
	}
	if result.FinalAnswer.AnswerText != "근거 기반 요약" {
		t.Fatalf("unexpected final answer text: %+v", result.FinalAnswer)
	}
}

func TestExecutePlanCopiesDatasetProfileSnapshot(t *testing.T) {
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
		Profile: &domain.DatasetProfile{
			ProfileID:              "festival-default",
			PreparePromptVersion:   stringPtr("dataset-prepare-anthropic-batch-v2"),
			SentimentPromptVersion: stringPtr("sentiment-anthropic-v2"),
			GarbageRuleNames:       []string{"ad_marker"},
		},
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}
	request := domain.AnalysisRequest{
		RequestID:        "request-1",
		ProjectID:        project.ProjectID,
		DatasetName:      stringPtr("issues.csv"),
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Goal:             "요약해줘",
		Constraints:      []string{},
		Context:          map[string]any{},
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveRequest(request); err != nil {
		t.Fatalf("unexpected save request error: %v", err)
	}
	planRecord := domain.PlanRecord{
		PlanID:           "plan-1",
		RequestID:        request.RequestID,
		ProjectID:        project.ProjectID,
		DatasetName:      "issues.csv",
		DatasetVersionID: stringPtr(version.DatasetVersionID),
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "keyword_frequency", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
		},
		Status:    "draft",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SavePlan(planRecord); err != nil {
		t.Fatalf("unexpected save plan error: %v", err)
	}

	executed, err := service.ExecutePlan(project.ProjectID, planRecord.PlanID)
	if err != nil {
		t.Fatalf("unexpected execute plan error: %v", err)
	}
	if executed.Execution.ProfileSnapshot == nil {
		t.Fatalf("expected profile snapshot on execution")
	}
	if executed.Execution.ProfileSnapshot.ProfileID != "festival-default" {
		t.Fatalf("unexpected execution profile snapshot: %+v", executed.Execution.ProfileSnapshot)
	}
}

func TestListExecutionsBuildsSnapshotPreview(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	executions := []domain.ExecutionSummary{
		{
			ExecutionID: "exec-older",
			ProjectID:   project.ProjectID,
			RequestID:   "request-1",
			Status:      "completed",
			CreatedAt:   now.Add(-time.Hour),
			Plan:        domain.SkillPlan{PlanID: "plan-1"},
			ResultV1Snapshot: &domain.ExecutionResultV1{
				SchemaVersion:    "execution-result-v1",
				Status:           "completed",
				PrimarySkillName: stringPtr("issue_cluster_summary"),
				Answer:           &domain.ExecutionResultAnswer{Summary: "이전 실행 요약"},
				Warnings:         []string{"fallback used"},
			},
			FinalAnswerSnapshot: &domain.ExecutionFinalAnswer{
				SchemaVersion:  "execution-final-answer-v1",
				Status:         "ready",
				GenerationMode: "llm",
				AnswerText:     "이전 실행 최종 답변",
			},
		},
		{
			ExecutionID: "exec-newer",
			ProjectID:   project.ProjectID,
			RequestID:   "request-2",
			Status:      "completed",
			CreatedAt:   now,
			Plan:        domain.SkillPlan{PlanID: "plan-2"},
			ResultV1Snapshot: &domain.ExecutionResultV1{
				SchemaVersion:    "execution-result-v1",
				Status:           "completed",
				PrimarySkillName: stringPtr("issue_evidence_summary"),
				Answer:           &domain.ExecutionResultAnswer{Summary: "최신 실행 요약"},
			},
			FinalAnswerSnapshot: &domain.ExecutionFinalAnswer{
				SchemaVersion:  "execution-final-answer-v1",
				Status:         "ready",
				GenerationMode: "llm",
				AnswerText:     "최신 실행 최종 답변",
			},
		},
	}
	for _, execution := range executions {
		if err := repository.SaveExecution(execution); err != nil {
			t.Fatalf("unexpected save execution error: %v", err)
		}
	}

	response, err := service.ListExecutions(project.ProjectID)
	if err != nil {
		t.Fatalf("unexpected list executions error: %v", err)
	}
	if len(response.Items) != 2 {
		t.Fatalf("unexpected execution items: %+v", response.Items)
	}
	if response.Items[0].ExecutionID != "exec-newer" {
		t.Fatalf("expected newest execution first: %+v", response.Items)
	}
	if response.Items[0].AnswerPreview == nil || *response.Items[0].AnswerPreview != "최신 실행 최종 답변" {
		t.Fatalf("unexpected answer preview: %+v", response.Items[0])
	}
	if response.Items[1].WarningCount != 1 {
		t.Fatalf("unexpected warning count: %+v", response.Items[1])
	}
}

func TestCreateReportDraftBuildsSnapshotSections(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	now := time.Now().UTC()
	executions := []domain.ExecutionSummary{
		{
			ExecutionID: "exec-1",
			ProjectID:   project.ProjectID,
			RequestID:   "request-1",
			Status:      "completed",
			CreatedAt:   now.Add(-time.Minute),
			Plan:        domain.SkillPlan{PlanID: "plan-1"},
			ResultV1Snapshot: &domain.ExecutionResultV1{
				SchemaVersion:    "execution-result-v1",
				Status:           "completed",
				PrimarySkillName: stringPtr("issue_evidence_summary"),
				Answer: &domain.ExecutionResultAnswer{
					Summary:           "결제 오류가 반복 발생했습니다.",
					KeyFindings:       []string{"결제 오류 관련 VOC가 반복된다."},
					Evidence:          []map[string]any{{"snippet": "결제 오류가 반복 발생했습니다"}},
					FollowUpQuestions: []string{"결제 실패 구간을 더 볼까요?"},
				},
				UsageSummary: map[string]any{"request_count": 1, "total_tokens": 10},
			},
			FinalAnswerSnapshot: &domain.ExecutionFinalAnswer{
				SchemaVersion:     "execution-final-answer-v1",
				Status:            "ready",
				GenerationMode:    "llm",
				AnswerText:        "결제 오류 반복 이슈가 핵심입니다.",
				KeyPoints:         []string{"결제 오류 VOC가 반복된다."},
				Evidence:          []map[string]any{{"snippet": "결제 오류가 반복 발생했습니다"}},
				FollowUpQuestions: []string{"결제 실패 구간을 더 볼까요?"},
			},
		},
		{
			ExecutionID: "exec-2",
			ProjectID:   project.ProjectID,
			RequestID:   "request-2",
			Status:      "completed",
			CreatedAt:   now,
			Plan:        domain.SkillPlan{PlanID: "plan-2"},
			ResultV1Snapshot: &domain.ExecutionResultV1{
				SchemaVersion:    "execution-result-v1",
				Status:           "completed",
				PrimarySkillName: stringPtr("issue_breakdown_summary"),
				Answer: &domain.ExecutionResultAnswer{
					Summary:     "앱 채널 비중이 가장 높습니다.",
					KeyFindings: []string{"앱 채널이 최다 그룹이다."},
				},
				UsageSummary: map[string]any{"request_count": 1, "total_tokens": 20},
				Warnings:     []string{"fallback summary used"},
			},
			FinalAnswerSnapshot: &domain.ExecutionFinalAnswer{
				SchemaVersion:  "execution-final-answer-v1",
				Status:         "ready",
				GenerationMode: "llm",
				AnswerText:     "앱 채널 언급 비중이 가장 높습니다.",
				KeyPoints:      []string{"앱 채널이 최다 그룹이다."},
				Caveats:        []string{"fallback summary used"},
			},
		},
	}
	for _, execution := range executions {
		if err := repository.SaveExecution(execution); err != nil {
			t.Fatalf("unexpected save execution error: %v", err)
		}
	}

	title := "주요 VOC 보고서 초안"
	draft, err := service.CreateReportDraft(project.ProjectID, domain.ReportDraftCreateRequest{
		Title:        &title,
		ExecutionIDs: []string{"exec-1", "exec-2"},
	})
	if err != nil {
		t.Fatalf("unexpected create report draft error: %v", err)
	}
	if draft.Content.SchemaVersion != "report-draft-v1" {
		t.Fatalf("unexpected report draft schema: %+v", draft.Content)
	}
	if draft.Content.ExecutionCount != 2 {
		t.Fatalf("unexpected execution count: %+v", draft.Content)
	}
	if len(draft.Content.Sections) != 2 {
		t.Fatalf("unexpected report sections: %+v", draft.Content)
	}
	if draft.Content.Sections[0].ExecutionID != "exec-1" {
		t.Fatalf("expected selected order to be preserved: %+v", draft.Content.Sections)
	}
	if draft.Content.Sections[0].Summary != "결제 오류 반복 이슈가 핵심입니다." {
		t.Fatalf("expected final answer summary to be preferred: %+v", draft.Content.Sections[0])
	}
	if got := draft.Content.UsageSummary["total_tokens"]; got != 30 {
		t.Fatalf("unexpected usage summary: %+v", draft.Content.UsageSummary)
	}
	if len(draft.Content.KeyFindings) < 2 {
		t.Fatalf("expected merged key findings: %+v", draft.Content)
	}
	if len(draft.Content.Warnings) != 1 {
		t.Fatalf("expected merged warnings: %+v", draft.Content)
	}
	loaded, err := service.GetReportDraft(project.ProjectID, draft.DraftID)
	if err != nil {
		t.Fatalf("unexpected get report draft error: %v", err)
	}
	if loaded.Content.Overview == "" {
		t.Fatalf("expected stored overview: %+v", loaded.Content)
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

func TestResolvedTextColumnForSkillTreatsDefaultTextAsPlaceholderWhenRawColumnDiffers(t *testing.T) {
	version := domain.DatasetVersion{
		DataType:      "unstructured",
		PrepareStatus: "ready",
		PrepareURI:    stringPtr("festival.prepared.parquet"),
		Metadata: map[string]any{
			"text_column":          "본문",
			"raw_text_column":      "본문",
			"prepared_text_column": "normalized_text",
		},
	}

	got := resolvedTextColumnForSkill(map[string]any{"text_column": "text"}, version)
	if got != "normalized_text" {
		t.Fatalf("expected normalized_text, got %s", got)
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
