package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

func TestDatasetServiceReconcileStartupBuildJobsRedispatchesQueuedAndRunningJobs(t *testing.T) {
	repository := store.NewMemoryStore()
	starter := &fakeDatasetBuildStarter{}
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())
	service.SetBuildJobStarter(starter)

	project := domain.Project{ProjectID: "project-1", Name: "demo", CreatedAt: time.Now().UTC()}
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured", CreatedAt: time.Now().UTC()}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	runningAt := time.Now().UTC()
	runID := "old-run"
	for _, job := range []domain.DatasetBuildJob{
		{
			JobID:            "job-clean",
			ProjectID:        project.ProjectID,
			DatasetID:        dataset.DatasetID,
			DatasetVersionID: version.DatasetVersionID,
			BuildType:        datasetBuildTypeClean,
			Status:           "queued",
			Request:          map[string]any{"text_columns": []any{"제목", "본문"}},
			CreatedAt:        time.Now().UTC(),
		},
		{
			JobID:            "job-prepare",
			ProjectID:        project.ProjectID,
			DatasetID:        dataset.DatasetID,
			DatasetVersionID: version.DatasetVersionID,
			BuildType:        datasetBuildTypePrepare,
			Status:           "queued",
			Request:          map[string]any{"text_column": "text"},
			CreatedAt:        time.Now().UTC(),
		},
		{
			JobID:            "job-embedding",
			ProjectID:        project.ProjectID,
			DatasetID:        dataset.DatasetID,
			DatasetVersionID: version.DatasetVersionID,
			BuildType:        datasetBuildTypeEmbedding,
			Status:           "running",
			Request:          map[string]any{"text_column": "text"},
			WorkflowRunID:    &runID,
			StartedAt:        &runningAt,
			CreatedAt:        time.Now().UTC(),
		},
	} {
		if err := repository.SaveDatasetBuildJob(job); err != nil {
			t.Fatalf("unexpected save dataset build job error: %v", err)
		}
	}

	requeued, err := service.ReconcileStartupBuildJobs()
	if err != nil {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}

	if requeued != 3 {
		t.Fatalf("expected 3 requeued build jobs, got %d", requeued)
	}
	if len(starter.startCalls) != 3 {
		t.Fatalf("expected 3 dataset build workflow starts, got %d", len(starter.startCalls))
	}
	hasCleanStart := false
	for _, call := range starter.startCalls {
		if call.BuildType == datasetBuildTypeClean {
			hasCleanStart = true
			break
		}
	}
	if !hasCleanStart {
		t.Fatalf("expected clean build to be redispatched, got %+v", starter.startCalls)
	}

	stored, err := repository.GetDatasetBuildJob(project.ProjectID, "job-embedding")
	if err != nil {
		t.Fatalf("unexpected get dataset build job error: %v", err)
	}
	if stored.WorkflowRunID != nil {
		t.Fatalf("expected workflow run id to be cleared before redispatch, got %+v", stored.WorkflowRunID)
	}
}

func TestAnalysisServiceReconcileStartupExecutionsRequeuesInflightAndResumesWaiting(t *testing.T) {
	repository := store.NewMemoryStore()
	starter := &fakeDatasetBuildStarter{}
	service := NewAnalysisService(repository, starter, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo", CreatedAt: time.Now().UTC()}
	dataset := domain.Dataset{DatasetID: "dataset-1", ProjectID: project.ProjectID, Name: "issues", DataType: "unstructured", CreatedAt: time.Now().UTC()}
	prepareURI := "issues.prepared.parquet"
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		PrepareStatus: "ready",
		PrepareURI:    &prepareURI,
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	plan := domain.SkillPlan{
		PlanID: "plan-1",
		Steps: []domain.SkillPlanStep{
			{
				StepID:      "step-1",
				SkillName:   "term_frequency",
				DatasetName: "issues.csv",
				Inputs:      map[string]any{"text_column": "text"},
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	for _, execution := range []domain.ExecutionSummary{
		{
			ExecutionID:      "exec-queued",
			ProjectID:        project.ProjectID,
			RequestID:        "request-1",
			Plan:             plan,
			Status:           "queued",
			CreatedAt:        time.Now().UTC(),
			DatasetVersionID: &version.DatasetVersionID,
			Artifacts:        map[string]string{},
			Events:           []domain.ExecutionEvent{},
		},
		{
			ExecutionID:      "exec-waiting",
			ProjectID:        project.ProjectID,
			RequestID:        "request-2",
			Plan:             plan,
			Status:           "waiting",
			CreatedAt:        time.Now().UTC(),
			DatasetVersionID: &version.DatasetVersionID,
			Artifacts:        map[string]string{},
			Events: []domain.ExecutionEvent{
				{
					ExecutionID: "exec-waiting",
					TS:          time.Now().UTC(),
					Level:       "info",
					EventType:   "WORKFLOW_WAITING",
					Message:     "execution is waiting for dependency",
					Payload: map[string]any{
						"waiting_for": "dataset_prepare",
					},
				},
			},
		},
	} {
		if err := repository.SaveExecution(execution); err != nil {
			t.Fatalf("unexpected save execution error: %v", err)
		}
	}

	summary, err := service.ReconcileStartupExecutions()
	if err != nil {
		t.Fatalf("unexpected execution reconciliation error: %v", err)
	}

	if summary.ExecutionsReenqueued != 1 {
		t.Fatalf("expected 1 re-enqueued execution, got %+v", summary)
	}
	if summary.WaitingExecutionsResumed != 1 {
		t.Fatalf("expected 1 resumed waiting execution, got %+v", summary)
	}
	if len(starter.analysisStartCalls) != 2 {
		t.Fatalf("expected 2 analysis workflow starts, got %d", len(starter.analysisStartCalls))
	}

	requeued, err := repository.GetExecution(project.ProjectID, "exec-queued")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if requeued.Status != "queued" {
		t.Fatalf("expected queued execution to remain queued, got %s", requeued.Status)
	}
	if got := requeued.Events[len(requeued.Events)-1].EventType; got != "STARTUP_REENQUEUED" {
		t.Fatalf("expected STARTUP_REENQUEUED event, got %s", got)
	}

	resumed, err := repository.GetExecution(project.ProjectID, "exec-waiting")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if resumed.Status != "queued" {
		t.Fatalf("expected waiting execution to resume into queued, got %s", resumed.Status)
	}
	if got := resumed.Events[len(resumed.Events)-1].EventType; got != "RESUME_ENQUEUED" {
		t.Fatalf("expected RESUME_ENQUEUED event, got %s", got)
	}
}

func TestAnalysisServiceReconcileStartupExecutionsSkipsNoopStarter(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewAnalysisService(repository, workflows.NoopStarter{}, nil)

	project := domain.Project{ProjectID: "project-1", Name: "demo", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	summary, err := service.ReconcileStartupExecutions()
	if err != nil {
		t.Fatalf("unexpected noop reconciliation error: %v", err)
	}
	if summary.ExecutionsReenqueued != 0 || summary.WaitingExecutionsResumed != 0 {
		t.Fatalf("expected noop reconciliation to skip work, got %+v", summary)
	}
}
