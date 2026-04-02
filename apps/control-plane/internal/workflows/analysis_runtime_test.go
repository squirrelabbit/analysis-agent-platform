package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/skills"
	"analysis-support-platform/control-plane/internal/store"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRegisterAnalysisRuntimeUsesExpectedNames(t *testing.T) {
	registrar := &stubRuntimeRegistrar{}

	RegisterAnalysisRuntime(registrar, NewAnalysisActivities())

	if registrar.workflowName != AnalysisExecutionWorkflowName {
		t.Fatalf("unexpected workflow name: %s", registrar.workflowName)
	}
	if len(registrar.activityNames) != 6 {
		t.Fatalf("unexpected activity count: %d", len(registrar.activityNames))
	}
	expected := []string{
		MarkExecutionRunningActivityName,
		CheckExecutionReadinessActivityName,
		MarkExecutionWaitingActivityName,
		ExecutePlanActivityName,
		MarkExecutionCompletedActivityName,
		MarkExecutionFailedActivityName,
	}
	for index, name := range expected {
		if registrar.activityNames[index] != name {
			t.Fatalf("unexpected activity name at %d: %s", index, registrar.activityNames[index])
		}
	}
}

func TestAnalysisExecutionWorkflowCompletesAndPersistsExecution(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID: "exec-1",
		ProjectID:   "project-1",
		RequestID:   "request-1",
		Status:      "queued",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "structured_kpi_summary", DatasetName: "sales_kpi", Inputs: map[string]any{}},
			},
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo: repo,
		Runner: fakeExecutionRunner{
			result: skills.ExecutionRunResult{
				Engine:         "duckdb",
				ProcessedSteps: 1,
				Artifacts: map[string]string{
					"step:step-1:structured_kpi_summary": `{"summary":{"row_count":3}}`,
				},
				Notes: []string{"structured path completed"},
				StepHooks: []skills.StepHookRecord{
					{Phase: "before", StepID: "step-1", SkillName: "structured_kpi_summary"},
					{Phase: "after", StepID: "step-1", SkillName: "structured_kpi_summary", Payload: map[string]any{"status": "completed"}},
				},
			},
		},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID: "exec-1",
			ProjectID:   "project-1",
			RequestID:   "request-1",
			PlanID:      "plan-1",
			RequestedAt: fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	var result AnalysisWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("unexpected get result error: %v", err)
	}

	if result.ExecutionID != "exec-1" {
		t.Fatalf("unexpected execution id: %s", result.ExecutionID)
	}
	if result.Status != "completed" {
		t.Fatalf("unexpected status: %s", result.Status)
	}
	if result.StartedAt != fixedNow || result.CompletedAt != fixedNow {
		t.Fatalf("unexpected timestamps: %+v", result)
	}
	if got := result.Artifacts["step:step-1:structured_kpi_summary"]; got == "" {
		t.Fatalf("expected structured artifact, got: %+v", result.Artifacts)
	}
	if len(result.Notes) != 1 {
		t.Fatalf("unexpected notes: %+v", result.Notes)
	}

	execution, err := repo.GetExecution("project-1", "exec-1")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "completed" {
		t.Fatalf("unexpected persisted status: %s", execution.Status)
	}
	if execution.EndedAt == nil || !execution.EndedAt.Equal(fixedNow) {
		t.Fatalf("unexpected ended_at: %+v", execution.EndedAt)
	}
	if len(execution.Events) != 2 {
		t.Fatalf("unexpected event count: %d", len(execution.Events))
	}
	if execution.Events[0].EventType != "WORKFLOW_STARTED" || execution.Events[1].EventType != "WORKFLOW_COMPLETED" {
		t.Fatalf("unexpected events: %+v", execution.Events)
	}
	if hooks, ok := execution.Events[1].Payload["step_hooks"].([]skills.StepHookRecord); !ok || len(hooks) != 2 {
		t.Fatalf("unexpected step hooks payload: %+v", execution.Events[1].Payload)
	}
	if execution.ResultV1Snapshot == nil {
		t.Fatalf("expected result v1 snapshot to be persisted: %+v", execution)
	}
	if execution.ResultV1Snapshot.SchemaVersion != "execution-result-v1" {
		t.Fatalf("unexpected result v1 snapshot: %+v", execution.ResultV1Snapshot)
	}
}

type stubRuntimeRegistrar struct {
	workflowName  string
	activityNames []string
}

func (s *stubRuntimeRegistrar) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	s.workflowName = options.Name
}

func (s *stubRuntimeRegistrar) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	s.activityNames = append(s.activityNames, options.Name)
}

func TestAnalysisExecutionWorkflowMarksFailure(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID: "exec-2",
		ProjectID:   "project-1",
		RequestID:   "request-1",
		Status:      "queued",
		Artifacts:   map[string]string{},
		Plan: domain.SkillPlan{
			PlanID:    "plan-2",
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo:   repo,
		Runner: fakeExecutionRunner{err: errors.New("duckdb query failed")},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID: "exec-2",
			ProjectID:   "project-1",
			RequestID:   "request-1",
			PlanID:      "plan-2",
			RequestedAt: fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected workflow error")
	}

	execution, err := repo.GetExecution("project-1", "exec-2")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "failed" {
		t.Fatalf("unexpected persisted status: %s", execution.Status)
	}
	if len(execution.Events) != 2 {
		t.Fatalf("unexpected event count: %d", len(execution.Events))
	}
	if execution.Events[1].EventType != "WORKFLOW_FAILED" {
		t.Fatalf("unexpected failure event: %+v", execution.Events[1])
	}
}

func TestAnalysisExecutionWorkflowWaitsForEmbeddings(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID:      "exec-3",
		ProjectID:        "project-1",
		RequestID:        "request-1",
		Status:           "queued",
		DatasetVersionID: stringPtr("version-1"),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-3",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "semantic_search", DatasetName: "issues.csv", Inputs: map[string]any{"query": "오류"}},
			},
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        "dataset-1",
		ProjectID:        "project-1",
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("issues.csv.prepared.parquet"),
		EmbeddingStatus:  "queued",
		CreatedAt:        fixedNow,
	}); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo: repo,
		Runner: fakeExecutionRunner{
			err: errors.New("runner should not execute while waiting"),
		},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID:      "exec-3",
			ProjectID:        "project-1",
			RequestID:        "request-1",
			PlanID:           "plan-3",
			DatasetVersionID: stringPtr("version-1"),
			RequestedAt:      fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	var result AnalysisWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("unexpected get result error: %v", err)
	}
	if result.Status != "waiting" {
		t.Fatalf("unexpected workflow status: %s", result.Status)
	}

	execution, err := repo.GetExecution("project-1", "exec-3")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "waiting" {
		t.Fatalf("unexpected execution status: %s", execution.Status)
	}
	if len(execution.Events) != 2 {
		t.Fatalf("unexpected waiting events: %+v", execution.Events)
	}
	if execution.Events[1].EventType != "WORKFLOW_WAITING" {
		t.Fatalf("unexpected waiting event: %+v", execution.Events[1])
	}
	if execution.Events[1].Payload["waiting_for"] != "embeddings" {
		t.Fatalf("unexpected waiting payload: %+v", execution.Events[1].Payload)
	}
}

func TestAnalysisExecutionWorkflowWaitsForClusterEmbeddings(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID:      "exec-3b",
		ProjectID:        "project-1",
		RequestID:        "request-1",
		Status:           "queued",
		DatasetVersionID: stringPtr("version-1"),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-3b",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "embedding_cluster", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        "dataset-1",
		ProjectID:        "project-1",
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("issues.csv.prepared.parquet"),
		EmbeddingStatus:  "queued",
		CreatedAt:        fixedNow,
	}); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo: repo,
		Runner: fakeExecutionRunner{
			err: errors.New("runner should not execute while waiting"),
		},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID:      "exec-3b",
			ProjectID:        "project-1",
			RequestID:        "request-1",
			PlanID:           "plan-3b",
			DatasetVersionID: stringPtr("version-1"),
			RequestedAt:      fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	execution, err := repo.GetExecution("project-1", "exec-3b")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "waiting" {
		t.Fatalf("unexpected execution status: %s", execution.Status)
	}
	if execution.Events[1].Payload["waiting_for"] != "embeddings" {
		t.Fatalf("unexpected waiting payload: %+v", execution.Events[1].Payload)
	}
}

func TestAnalysisExecutionWorkflowWaitsForDatasetPrepare(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID:      "exec-4",
		ProjectID:        "project-1",
		RequestID:        "request-1",
		Status:           "queued",
		DatasetVersionID: stringPtr("version-2"),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-4",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "unstructured_issue_summary", DatasetName: "issues.csv", Inputs: map[string]any{}},
			},
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "version-2",
		DatasetID:        "dataset-1",
		ProjectID:        "project-1",
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "queued",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        fixedNow,
	}); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo: repo,
		Runner: fakeExecutionRunner{
			err: errors.New("runner should not execute while waiting"),
		},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID:      "exec-4",
			ProjectID:        "project-1",
			RequestID:        "request-1",
			PlanID:           "plan-4",
			DatasetVersionID: stringPtr("version-2"),
			RequestedAt:      fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	execution, err := repo.GetExecution("project-1", "exec-4")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "waiting" {
		t.Fatalf("unexpected execution status: %s", execution.Status)
	}
	if len(execution.Events) != 2 {
		t.Fatalf("unexpected waiting events: %+v", execution.Events)
	}
	if execution.Events[1].Payload["waiting_for"] != "dataset_prepare" {
		t.Fatalf("unexpected waiting payload: %+v", execution.Events[1].Payload)
	}
}

func TestAnalysisExecutionWorkflowWaitsForSentimentLabels(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	fixedNow := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	repo := store.NewMemoryStore()
	if err := repo.SaveExecution(domain.ExecutionSummary{
		ExecutionID:      "exec-5",
		ProjectID:        "project-1",
		RequestID:        "request-1",
		Status:           "queued",
		DatasetVersionID: stringPtr("version-3"),
		Artifacts:        map[string]string{},
		Plan: domain.SkillPlan{
			PlanID: "plan-5",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_sentiment_summary", DatasetName: "issues.sentiment.parquet", Inputs: map[string]any{}},
			},
			CreatedAt: fixedNow,
		},
		Events: []domain.ExecutionEvent{},
	}); err != nil {
		t.Fatalf("unexpected save execution error: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "version-3",
		DatasetID:        "dataset-1",
		ProjectID:        "project-1",
		StorageURI:       "issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "ready",
		PrepareURI:       stringPtr("issues.csv.prepared.parquet"),
		SentimentStatus:  "queued",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        fixedNow,
	}); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	RegisterAnalysisRuntime(env, AnalysisActivities{
		Repo: repo,
		Runner: fakeExecutionRunner{
			err: errors.New("runner should not execute while waiting"),
		},
		Now: func() time.Time {
			return fixedNow
		},
	})

	env.ExecuteWorkflow(
		AnalysisExecutionWorkflow,
		AnalysisWorkflowInput{
			ExecutionID:      "exec-5",
			ProjectID:        "project-1",
			RequestID:        "request-1",
			PlanID:           "plan-5",
			DatasetVersionID: stringPtr("version-3"),
			RequestedAt:      fixedNow.Add(-time.Minute),
		},
	)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}

	execution, err := repo.GetExecution("project-1", "exec-5")
	if err != nil {
		t.Fatalf("unexpected get execution error: %v", err)
	}
	if execution.Status != "waiting" {
		t.Fatalf("unexpected execution status: %s", execution.Status)
	}
	if len(execution.Events) != 2 {
		t.Fatalf("unexpected waiting events: %+v", execution.Events)
	}
	if execution.Events[1].Payload["waiting_for"] != "sentiment_labels" {
		t.Fatalf("unexpected waiting payload: %+v", execution.Events[1].Payload)
	}
}

func stringPtr(value string) *string {
	return &value
}

type fakeExecutionRunner struct {
	result skills.ExecutionRunResult
	err    error
}

func (f fakeExecutionRunner) Run(_ context.Context, _ domain.ExecutionSummary) (skills.ExecutionRunResult, error) {
	if f.err != nil {
		return skills.ExecutionRunResult{}, f.err
	}
	return f.result, nil
}
