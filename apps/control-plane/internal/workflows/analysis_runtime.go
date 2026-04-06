package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/executionresult"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/skills"
	"analysis-support-platform/control-plane/internal/store"

	"go.temporal.io/sdk/activity"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const MarkExecutionRunningActivityName = "analysis.mark_execution_running.v1"
const CheckExecutionReadinessActivityName = "analysis.check_execution_readiness.v1"
const MarkExecutionWaitingActivityName = "analysis.mark_execution_waiting.v1"
const ExecutePlanActivityName = "analysis.execute_plan.v1"
const MarkExecutionCompletedActivityName = "analysis.mark_execution_completed.v1"
const MarkExecutionFailedActivityName = "analysis.mark_execution_failed.v1"

type AnalysisWorkflowResult struct {
	ExecutionID string            `json:"execution_id"`
	Status      string            `json:"status"`
	Artifacts   map[string]string `json:"artifacts"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Notes       []string          `json:"notes,omitempty"`
}

type ExecutionLifecycleResult struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	EventType string    `json:"event_type"`
}

type ExecutionReadinessResult struct {
	Ready      bool      `json:"ready"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	WaitingFor string    `json:"waiting_for,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

type AnalysisActivities struct {
	Repo   store.Repository
	Runner skills.ExecutionRunner
	Now    func() time.Time
}

type CompleteExecutionInput struct {
	WorkflowInput AnalysisWorkflowInput       `json:"workflow_input"`
	Result        skills.StructuredPlanResult `json:"result"`
}

type FailExecutionInput struct {
	WorkflowInput AnalysisWorkflowInput `json:"workflow_input"`
	ErrorMessage  string                `json:"error_message"`
}

type WaitingExecutionInput struct {
	WorkflowInput AnalysisWorkflowInput    `json:"workflow_input"`
	Readiness     ExecutionReadinessResult `json:"readiness"`
}

type RuntimeRegistrar interface {
	RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
}

func RegisterAnalysisRuntime(registrar RuntimeRegistrar, activities AnalysisActivities) {
	registrar.RegisterWorkflowWithOptions(
		AnalysisExecutionWorkflow,
		workflow.RegisterOptions{Name: AnalysisExecutionWorkflowName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkExecutionRunning,
		activity.RegisterOptions{Name: MarkExecutionRunningActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.CheckExecutionReadiness,
		activity.RegisterOptions{Name: CheckExecutionReadinessActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkExecutionWaiting,
		activity.RegisterOptions{Name: MarkExecutionWaitingActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.ExecutePlan,
		activity.RegisterOptions{Name: ExecutePlanActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkExecutionCompleted,
		activity.RegisterOptions{Name: MarkExecutionCompletedActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkExecutionFailed,
		activity.RegisterOptions{Name: MarkExecutionFailedActivityName},
	)
}

func NewAnalysisActivities() AnalysisActivities {
	return AnalysisActivities{
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func AnalysisExecutionWorkflow(ctx workflow.Context, input AnalysisWorkflowInput) (AnalysisWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"analysis execution workflow started",
		"execution_id", input.ExecutionID,
		"project_id", input.ProjectID,
		"request_id", input.RequestID,
		"plan_id", input.PlanID,
	)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var started ExecutionLifecycleResult
	if err := workflow.ExecuteActivity(ctx, MarkExecutionRunningActivityName, input).Get(ctx, &started); err != nil {
		return AnalysisWorkflowResult{}, err
	}

	var readiness ExecutionReadinessResult
	if err := workflow.ExecuteActivity(ctx, CheckExecutionReadinessActivityName, input).Get(ctx, &readiness); err != nil {
		return AnalysisWorkflowResult{}, err
	}
	if !readiness.Ready {
		var waiting ExecutionLifecycleResult
		if err := workflow.ExecuteActivity(
			ctx,
			MarkExecutionWaitingActivityName,
			WaitingExecutionInput{
				WorkflowInput: input,
				Readiness:     readiness,
			},
		).Get(ctx, &waiting); err != nil {
			return AnalysisWorkflowResult{}, err
		}
		return AnalysisWorkflowResult{
			ExecutionID: input.ExecutionID,
			Status:      waiting.Status,
			Artifacts:   map[string]string{},
			StartedAt:   started.Timestamp,
			CompletedAt: waiting.Timestamp,
			Notes:       []string{readiness.Reason},
		}, nil
	}

	var runResult skills.ExecutionRunResult
	if err := workflow.ExecuteActivity(ctx, ExecutePlanActivityName, input).Get(ctx, &runResult); err != nil {
		var failed ExecutionLifecycleResult
		if markErr := workflow.ExecuteActivity(
			ctx,
			MarkExecutionFailedActivityName,
			FailExecutionInput{
				WorkflowInput: input,
				ErrorMessage:  err.Error(),
			},
		).Get(ctx, &failed); markErr != nil {
			return AnalysisWorkflowResult{}, fmt.Errorf("execute structured plan: %w; mark failed: %v", err, markErr)
		}
		return AnalysisWorkflowResult{}, err
	}

	var completed ExecutionLifecycleResult
	if err := workflow.ExecuteActivity(
		ctx,
		MarkExecutionCompletedActivityName,
		CompleteExecutionInput{
			WorkflowInput: input,
			Result:        runResult,
		},
	).Get(ctx, &completed); err != nil {
		return AnalysisWorkflowResult{}, err
	}

	return AnalysisWorkflowResult{
		ExecutionID: input.ExecutionID,
		Status:      completed.Status,
		Artifacts:   runResult.Artifacts,
		StartedAt:   started.Timestamp,
		CompletedAt: completed.Timestamp,
		Notes:       runResult.Notes,
	}, nil
}

func (a AnalysisActivities) MarkExecutionRunning(ctx context.Context, input AnalysisWorkflowInput) (ExecutionLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	execution, err := repo.GetExecution(input.ProjectID, input.ExecutionID)
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	now := a.now()
	info := sdkactivity.GetInfo(ctx)
	execution.Status = "running"
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: input.ExecutionID,
		TS:          now,
		Level:       "info",
		EventType:   "WORKFLOW_STARTED",
		Message:     "execution picked up by Temporal worker",
		Payload: map[string]any{
			"workflow_id": info.WorkflowExecution.ID,
			"run_id":      info.WorkflowExecution.RunID,
			"task_queue":  info.TaskQueue,
			"attempt":     info.Attempt,
		},
	})

	if err := repo.SaveExecution(execution); err != nil {
		return ExecutionLifecycleResult{}, err
	}

	return ExecutionLifecycleResult{
		Status:    execution.Status,
		Timestamp: now,
		EventType: "WORKFLOW_STARTED",
	}, nil
}

func (a AnalysisActivities) CheckExecutionReadiness(ctx context.Context, input AnalysisWorkflowInput) (ExecutionReadinessResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return ExecutionReadinessResult{}, err
	}
	execution, err := repo.GetExecution(input.ProjectID, input.ExecutionID)
	if err != nil {
		return ExecutionReadinessResult{}, err
	}
	needsPrepare := requiresPrepareReady(execution.Plan)
	needsSentiment := requiresSentimentReady(execution.Plan)
	needsEmbedding := requiresEmbeddingReady(execution.Plan)
	if !needsPrepare && !needsSentiment && !needsEmbedding {
		return ExecutionReadinessResult{
			Ready:     true,
			Status:    execution.Status,
			Timestamp: a.now(),
		}, nil
	}
	if execution.DatasetVersionID == nil || *execution.DatasetVersionID == "" {
		return ExecutionReadinessResult{}, errors.New("dataset_version_id is required for unstructured execution readiness checks")
	}
	version, err := repo.GetDatasetVersion(input.ProjectID, *execution.DatasetVersionID)
	if err != nil {
		return ExecutionReadinessResult{}, err
	}
	if needsPrepare {
		if version.PrepareStatus != "ready" || version.PrepareURI == nil || *version.PrepareURI == "" {
			return ExecutionReadinessResult{
				Ready:      false,
				Status:     "waiting",
				Timestamp:  a.now(),
				WaitingFor: "dataset_prepare",
				Reason:     "dataset version prepare output is not ready",
			}, nil
		}
	}
	if needsSentiment {
		if version.SentimentStatus != "ready" || version.SentimentURI == nil || *version.SentimentURI == "" {
			return ExecutionReadinessResult{
				Ready:      false,
				Status:     "waiting",
				Timestamp:  a.now(),
				WaitingFor: "sentiment_labels",
				Reason:     "dataset version sentiment labels are not ready",
			}, nil
		}
	}
	if needsEmbedding {
		hasEmbeddingArtifact := (version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "") ||
			strings.TrimSpace(fmt.Sprintf("%v", version.Metadata["embedding_index_ref"])) != "" ||
			strings.TrimSpace(fmt.Sprintf("%v", version.Metadata["embedding_index_source_ref"])) != ""
		embeddingReady := version.EmbeddingStatus == "ready" && hasEmbeddingArtifact
		if !embeddingReady {
			return ExecutionReadinessResult{
				Ready:      false,
				Status:     "waiting",
				Timestamp:  a.now(),
				WaitingFor: "embeddings",
				Reason:     "dataset version embeddings are not ready",
			}, nil
		}
	}
	return ExecutionReadinessResult{
		Ready:     true,
		Status:    execution.Status,
		Timestamp: a.now(),
	}, nil
}

func (a AnalysisActivities) MarkExecutionWaiting(ctx context.Context, input WaitingExecutionInput) (ExecutionLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	execution, err := repo.GetExecution(input.WorkflowInput.ProjectID, input.WorkflowInput.ExecutionID)
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	now := a.now()
	execution.Status = "waiting"
	execution.EndedAt = nil
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: input.WorkflowInput.ExecutionID,
		TS:          now,
		Level:       "info",
		EventType:   "WORKFLOW_WAITING",
		Message:     "execution is waiting for dependency",
		Payload: map[string]any{
			"waiting_for": input.Readiness.WaitingFor,
			"reason":      input.Readiness.Reason,
		},
	})

	if err := repo.SaveExecution(execution); err != nil {
		return ExecutionLifecycleResult{}, err
	}

	return ExecutionLifecycleResult{
		Status:    execution.Status,
		Timestamp: now,
		EventType: "WORKFLOW_WAITING",
	}, nil
}

func (a AnalysisActivities) ExecutePlan(ctx context.Context, input AnalysisWorkflowInput) (skills.ExecutionRunResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return skills.ExecutionRunResult{}, err
	}
	if a.Runner == nil {
		return skills.ExecutionRunResult{}, errors.New("execution runner is required")
	}

	execution, err := repo.GetExecution(input.ProjectID, input.ExecutionID)
	if err != nil {
		return skills.ExecutionRunResult{}, err
	}
	if execution.DatasetVersionID != nil && strings.TrimSpace(*execution.DatasetVersionID) != "" {
		version, err := repo.GetDatasetVersion(input.ProjectID, strings.TrimSpace(*execution.DatasetVersionID))
		if err != nil {
			return skills.ExecutionRunResult{}, err
		}
		execution.Plan = refreshWorkflowPlanWithDatasetVersion(execution.Plan, version)
	}

	return a.Runner.Run(ctx, execution)
}

func (a AnalysisActivities) MarkExecutionCompleted(ctx context.Context, input CompleteExecutionInput) (ExecutionLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	execution, err := repo.GetExecution(input.WorkflowInput.ProjectID, input.WorkflowInput.ExecutionID)
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	now := a.now()
	if execution.Artifacts == nil {
		execution.Artifacts = map[string]string{}
	}
	for key, value := range input.Result.Artifacts {
		execution.Artifacts[key] = value
	}
	execution.Status = "completed"
	execution.EndedAt = &now
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: input.WorkflowInput.ExecutionID,
		TS:          now,
		Level:       "info",
		EventType:   "WORKFLOW_COMPLETED",
		Message:     "execution completed through worker runtime",
		Payload: map[string]any{
			"artifact_count":   len(input.Result.Artifacts),
			"processed_steps":  input.Result.ProcessedSteps,
			"runner_engine":    input.Result.Engine,
			"structured_notes": input.Result.Notes,
		},
	})
	if len(input.Result.UsageSummary) > 0 {
		execution.Events[len(execution.Events)-1].Payload["usage_summary"] = input.Result.UsageSummary
	}
	if len(input.Result.StepHooks) > 0 {
		execution.Events[len(execution.Events)-1].Payload["step_hooks"] = input.Result.StepHooks
	}
	snapshot := executionresult.BuildV1(execution)
	execution.ResultV1Snapshot = &snapshot

	if err := repo.SaveExecution(execution); err != nil {
		return ExecutionLifecycleResult{}, err
	}

	return ExecutionLifecycleResult{
		Status:    execution.Status,
		Timestamp: now,
		EventType: "WORKFLOW_COMPLETED",
	}, nil
}

func (a AnalysisActivities) MarkExecutionFailed(ctx context.Context, input FailExecutionInput) (ExecutionLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	execution, err := repo.GetExecution(input.WorkflowInput.ProjectID, input.WorkflowInput.ExecutionID)
	if err != nil {
		return ExecutionLifecycleResult{}, err
	}

	now := a.now()
	info := sdkactivity.GetInfo(ctx)
	execution.Status = "failed"
	execution.EndedAt = &now
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: input.WorkflowInput.ExecutionID,
		TS:          now,
		Level:       "error",
		EventType:   "WORKFLOW_FAILED",
		Message:     "execution failed during worker runtime",
		Payload: map[string]any{
			"workflow_id": info.WorkflowExecution.ID,
			"run_id":      info.WorkflowExecution.RunID,
			"error":       input.ErrorMessage,
		},
	})

	if err := repo.SaveExecution(execution); err != nil {
		return ExecutionLifecycleResult{}, err
	}

	return ExecutionLifecycleResult{
		Status:    execution.Status,
		Timestamp: now,
		EventType: "WORKFLOW_FAILED",
	}, nil
}

func (a AnalysisActivities) requireRepo() (store.Repository, error) {
	if a.Repo == nil {
		return nil, errors.New("repository is required")
	}
	return a.Repo, nil
}

func (a AnalysisActivities) now() time.Time {
	if a.Now != nil {
		return a.Now().UTC()
	}
	return time.Now().UTC()
}

func requiresEmbeddingReady(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresEmbedding {
			return true
		}
	}
	return false
}

func requiresPrepareReady(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresPrepare {
			return true
		}
	}
	return false
}

func requiresSentimentReady(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresSentiment {
			return true
		}
	}
	return false
}

func refreshWorkflowPlanWithDatasetVersion(plan domain.SkillPlan, version domain.DatasetVersion) domain.SkillPlan {
	fallback := strings.TrimSpace(version.StorageURI)
	for index := range plan.Steps {
		definition, ok := registry.Skill(plan.Steps[index].SkillName)
		if !ok {
			continue
		}
		switch definition.DatasetSource {
		case "prepared":
			plan.Steps[index].DatasetName = workflowPreparedDatasetSource(version, fallback)
		case "sentiment":
			plan.Steps[index].DatasetName = workflowSentimentDatasetSource(version)
		}
		if plan.Steps[index].Inputs == nil {
			plan.Steps[index].Inputs = map[string]any{}
		}
		for key, metadataKey := range definition.MetadataDefaults {
			current := plan.Steps[index].Inputs[key]
			plan.Steps[index].Inputs[key] = workflowMetadataValue(version.Metadata, metadataKey, current)
		}
		if _, hasTextColumn := definition.DefaultInputs["text_column"]; hasTextColumn {
			plan.Steps[index].Inputs["text_column"] = workflowResolvedTextColumn(plan.Steps[index].Inputs, version)
		}
		if definition.RequiresEmbedding {
			if value := workflowMetadataString(version.Metadata, "embedding_index_ref", ""); value != "" {
				plan.Steps[index].Inputs["embedding_index_ref"] = value
			}
			if value := workflowMetadataString(version.Metadata, "chunk_ref", ""); value != "" {
				plan.Steps[index].Inputs["chunk_ref"] = value
			}
			if value := workflowMetadataString(version.Metadata, "chunk_format", ""); value != "" {
				plan.Steps[index].Inputs["chunk_format"] = value
			}
			if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
				plan.Steps[index].Inputs["embedding_uri"] = strings.TrimSpace(*version.EmbeddingURI)
			} else {
				delete(plan.Steps[index].Inputs, "embedding_uri")
			}
		}
	}
	return plan
}

func workflowPreparedDatasetSource(version domain.DatasetVersion, fallback string) string {
	if version.PrepareStatus == "ready" && version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != "" {
		return strings.TrimSpace(*version.PrepareURI)
	}
	return fallback
}

func workflowSentimentDatasetSource(version domain.DatasetVersion) string {
	if version.SentimentStatus == "ready" && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	return workflowEmbeddingURIValue(workflowPreparedDatasetSource(version, strings.TrimSpace(version.StorageURI)), ".sentiment.parquet")
}

func workflowResolvedTextColumn(inputs map[string]any, version domain.DatasetVersion) string {
	defaultTextColumn := workflowMetadataString(version.Metadata, "prepared_text_column", workflowMetadataString(version.Metadata, "text_column", "normalized_text"))
	if version.PrepareStatus != "ready" {
		defaultTextColumn = workflowMetadataString(version.Metadata, "text_column", "text")
	}
	if inputs == nil {
		return defaultTextColumn
	}
	value, ok := inputs["text_column"]
	if !ok {
		return defaultTextColumn
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" {
		return defaultTextColumn
	}
	rawTextColumn := workflowMetadataString(version.Metadata, "raw_text_column", workflowMetadataString(version.Metadata, "text_column", "text"))
	if version.PrepareStatus == "ready" && text == rawTextColumn {
		return defaultTextColumn
	}
	return text
}

func workflowEmbeddingURI(version domain.DatasetVersion) string {
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return strings.TrimSpace(*version.EmbeddingURI)
	}
	if value := workflowMetadataString(version.Metadata, "embedding_index_source_ref", ""); value != "" {
		return value
	}
	if value := workflowMetadataString(version.Metadata, "embedding_index_ref", ""); value != "" {
		return value
	}
	return workflowEmbeddingURIValue(workflowPreparedDatasetSource(version, strings.TrimSpace(version.StorageURI)), ".embeddings.jsonl")
}

func workflowEmbeddingURIValue(base string, suffix string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return ""
	}
	return base + suffix
}

func workflowMetadataValue(metadata map[string]any, key string, fallback any) any {
	if metadata == nil {
		return fallback
	}
	value, ok := metadata[key]
	if !ok {
		return fallback
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return fallback
		}
	}
	return value
}

func workflowMetadataString(metadata map[string]any, key string, fallback string) string {
	if metadata == nil {
		return fallback
	}
	value, ok := metadata[key]
	if !ok {
		return fallback
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" {
		return fallback
	}
	return text
}
