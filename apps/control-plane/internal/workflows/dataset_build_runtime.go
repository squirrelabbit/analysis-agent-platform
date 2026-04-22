package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const DatasetBuildWorkflowName = "dataset.build.v1"

const MarkDatasetBuildJobRunningActivityName = "dataset_build.mark_running.v1"
const ExecuteDatasetBuildJobActivityName = "dataset_build.execute.v1"
const MarkDatasetBuildJobCompletedActivityName = "dataset_build.mark_completed.v1"
const MarkDatasetBuildJobFailedActivityName = "dataset_build.mark_failed.v1"
const ResumeWaitingExecutionsActivityName = "dataset_build.resume_waiting_executions.v1"

type DatasetBuildWorkflowInput struct {
	JobID            string    `json:"job_id"`
	ProjectID        string    `json:"project_id"`
	DatasetID        string    `json:"dataset_id"`
	DatasetVersionID string    `json:"dataset_version_id"`
	BuildType        string    `json:"build_type"`
	RequestedAt      time.Time `json:"requested_at"`
}

type DatasetBuildLifecycleResult struct {
	JobID      string    `json:"job_id"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	BuildType  string    `json:"build_type"`
	Executions int       `json:"executions,omitempty"`
}

type DatasetBuildRunner interface {
	BuildPrepare(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest) (domain.DatasetVersion, error)
	BuildSentiment(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentBuildRequest) (domain.DatasetVersion, error)
	BuildEmbeddings(projectID, datasetID, datasetVersionID string, input domain.DatasetEmbeddingBuildRequest) (domain.DatasetVersion, error)
	BuildClusters(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest) (domain.DatasetVersion, error)
}

type WaitingExecutionResumer interface {
	ResumeWaitingExecutionsForDatasetVersion(projectID, datasetVersionID, reason, triggeredBy string) (int, error)
}

type DatasetBuildActivities struct {
	Repo        store.Repository
	Builder     DatasetBuildRunner
	Resumer     WaitingExecutionResumer
	Now         func() time.Time
	Concurrency DatasetBuildConcurrencyLimits

	limiterOnce sync.Once
	limiter     *datasetBuildLimiter
}

type DatasetBuildFailureInput struct {
	WorkflowInput DatasetBuildWorkflowInput `json:"workflow_input"`
	ErrorMessage  string                    `json:"error_message"`
	ErrorType     string                    `json:"error_type,omitempty"`
}

type DatasetBuildConcurrencyLimits struct {
	Prepare   int `json:"prepare"`
	Sentiment int `json:"sentiment"`
	Embedding int `json:"embedding"`
	Cluster   int `json:"cluster"`
}

func RegisterDatasetBuildRuntime(registrar RuntimeRegistrar, activities DatasetBuildActivities) {
	handler := &activities
	registrar.RegisterWorkflowWithOptions(
		DatasetBuildWorkflow,
		workflow.RegisterOptions{Name: DatasetBuildWorkflowName},
	)
	registrar.RegisterActivityWithOptions(
		handler.MarkDatasetBuildJobRunning,
		activity.RegisterOptions{Name: MarkDatasetBuildJobRunningActivityName},
	)
	registrar.RegisterActivityWithOptions(
		handler.ExecuteDatasetBuildJob,
		activity.RegisterOptions{Name: ExecuteDatasetBuildJobActivityName},
	)
	registrar.RegisterActivityWithOptions(
		handler.MarkDatasetBuildJobCompleted,
		activity.RegisterOptions{Name: MarkDatasetBuildJobCompletedActivityName},
	)
	registrar.RegisterActivityWithOptions(
		handler.MarkDatasetBuildJobFailed,
		activity.RegisterOptions{Name: MarkDatasetBuildJobFailedActivityName},
	)
	registrar.RegisterActivityWithOptions(
		handler.ResumeWaitingExecutionsForDatasetVersion,
		activity.RegisterOptions{Name: ResumeWaitingExecutionsActivityName},
	)
}

func DatasetBuildWorkflow(ctx workflow.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"dataset build workflow started",
		"job_id", input.JobID,
		"project_id", input.ProjectID,
		"dataset_version_id", input.DatasetVersionID,
		"build_type", input.BuildType,
	)

	lifecycleCtx := workflow.WithActivityOptions(ctx, datasetBuildLifecycleActivityOptions())
	buildCtx := workflow.WithActivityOptions(ctx, datasetBuildExecuteActivityOptions(input.BuildType))
	resumeCtx := workflow.WithActivityOptions(ctx, datasetBuildResumeActivityOptions())

	var started DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(lifecycleCtx, MarkDatasetBuildJobRunningActivityName, input).Get(ctx, &started); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	if err := workflow.ExecuteActivity(buildCtx, ExecuteDatasetBuildJobActivityName, input).Get(ctx, nil); err != nil {
		var failed DatasetBuildLifecycleResult
		failInput := DatasetBuildFailureInput{
			WorkflowInput: input,
			ErrorMessage:  err.Error(),
			ErrorType:     datasetBuildErrorType(err),
		}
		if markErr := workflow.ExecuteActivity(lifecycleCtx, MarkDatasetBuildJobFailedActivityName, failInput).Get(ctx, &failed); markErr != nil {
			return DatasetBuildLifecycleResult{}, fmt.Errorf("execute dataset build job: %w; mark failed: %v", err, markErr)
		}
		return failed, err
	}

	var completed DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(lifecycleCtx, MarkDatasetBuildJobCompletedActivityName, input).Get(ctx, &completed); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	var resumed DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(resumeCtx, ResumeWaitingExecutionsActivityName, input).Get(ctx, &resumed); err != nil {
		logger.Error("resume waiting executions failed", "job_id", input.JobID, "error", err)
		return completed, nil
	}
	completed.Executions = resumed.Executions
	return completed, nil
}

func (a *DatasetBuildActivities) MarkDatasetBuildJobRunning(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	now := a.now()
	job.Status = "running"
	job.StartedAt = &now
	job.ErrorMessage = nil
	job.LastErrorType = nil
	info := activity.GetInfo(ctx)
	if workflowID := info.WorkflowExecution.ID; workflowID != "" {
		job.WorkflowID = &workflowID
	}
	if workflowRunID := info.WorkflowExecution.RunID; workflowRunID != "" {
		job.WorkflowRunID = &workflowRunID
	}
	if err := repo.SaveDatasetBuildJob(job); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	return DatasetBuildLifecycleResult{
		JobID:     input.JobID,
		Status:    job.Status,
		Timestamp: now,
		BuildType: input.BuildType,
	}, nil
}

func (a *DatasetBuildActivities) ExecuteDatasetBuildJob(ctx context.Context, input DatasetBuildWorkflowInput) error {
	repo, err := a.requireRepo()
	if err != nil {
		return err
	}
	if a.Builder == nil {
		return fmt.Errorf("dataset build runner is not configured")
	}

	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return err
	}
	job.Attempt = int(activity.GetInfo(ctx).Attempt)
	if err := repo.SaveDatasetBuildJob(job); err != nil {
		return err
	}

	release, err := a.acquireBuildSlot(ctx, job.BuildType)
	if err != nil {
		return err
	}
	defer release()

	switch job.BuildType {
	case "prepare":
		request, err := decodeBuildRequest[domain.DatasetPrepareRequest](job.Request)
		if err != nil {
			return temporal.NewNonRetryableApplicationError(err.Error(), "invalid_request", err)
		}
		_, err = a.Builder.BuildPrepare(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return classifyDatasetBuildError(err)
	case "sentiment":
		request, err := decodeBuildRequest[domain.DatasetSentimentBuildRequest](job.Request)
		if err != nil {
			return temporal.NewNonRetryableApplicationError(err.Error(), "invalid_request", err)
		}
		_, err = a.Builder.BuildSentiment(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return classifyDatasetBuildError(err)
	case "embedding":
		request, err := decodeBuildRequest[domain.DatasetEmbeddingBuildRequest](job.Request)
		if err != nil {
			return temporal.NewNonRetryableApplicationError(err.Error(), "invalid_request", err)
		}
		_, err = a.Builder.BuildEmbeddings(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return classifyDatasetBuildError(err)
	case "cluster":
		request, err := decodeBuildRequest[domain.DatasetClusterBuildRequest](job.Request)
		if err != nil {
			return temporal.NewNonRetryableApplicationError(err.Error(), "invalid_request", err)
		}
		_, err = a.Builder.BuildClusters(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return classifyDatasetBuildError(err)
	default:
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unsupported dataset build type: %s", job.BuildType),
			"unsupported_build_type",
			nil,
		)
	}
}

func (a *DatasetBuildActivities) MarkDatasetBuildJobCompleted(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	now := a.now()
	job.Status = "completed"
	job.CompletedAt = &now
	job.ErrorMessage = nil
	job.LastErrorType = nil
	if err := repo.SaveDatasetBuildJob(job); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	return DatasetBuildLifecycleResult{
		JobID:     input.JobID,
		Status:    job.Status,
		Timestamp: now,
		BuildType: input.BuildType,
	}, nil
}

func (a *DatasetBuildActivities) MarkDatasetBuildJobFailed(ctx context.Context, payload DatasetBuildFailureInput) (DatasetBuildLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	input := payload.WorkflowInput
	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	now := a.now()
	message := stringsValue(payload.ErrorMessage)
	job.Status = "failed"
	job.CompletedAt = &now
	if message != "" {
		job.ErrorMessage = &message
	}
	if errorType := stringsValue(payload.ErrorType); errorType != "" {
		job.LastErrorType = &errorType
	}
	if err := repo.SaveDatasetBuildJob(job); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	return DatasetBuildLifecycleResult{
		JobID:     input.JobID,
		Status:    job.Status,
		Timestamp: now,
		BuildType: input.BuildType,
	}, nil
}

func (a *DatasetBuildActivities) ResumeWaitingExecutionsForDatasetVersion(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
	if a.Resumer == nil {
		return DatasetBuildLifecycleResult{
			JobID:     input.JobID,
			Status:    "completed",
			Timestamp: a.now(),
			BuildType: input.BuildType,
		}, nil
	}

	reason := fmt.Sprintf("dataset build job completed: %s", input.BuildType)
	count, err := a.Resumer.ResumeWaitingExecutionsForDatasetVersion(input.ProjectID, input.DatasetVersionID, reason, "dataset_build_job")
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	repo, repoErr := a.requireRepo()
	if repoErr != nil {
		return DatasetBuildLifecycleResult{}, repoErr
	}
	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	job.ResumedExecutionCount = count
	if err := repo.SaveDatasetBuildJob(job); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	return DatasetBuildLifecycleResult{
		JobID:      input.JobID,
		Status:     "completed",
		Timestamp:  a.now(),
		BuildType:  input.BuildType,
		Executions: count,
	}, nil
}

func datasetBuildLifecycleActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
}

func datasetBuildResumeActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
}

func datasetBuildExecuteActivityOptions(buildType string) workflow.ActivityOptions {
	timeout := 20 * time.Minute
	maxAttempts := int32(4)
	switch buildType {
	case "prepare":
		timeout = 75 * time.Minute
	case "sentiment":
		timeout = 45 * time.Minute
	case "embedding":
		timeout = 60 * time.Minute
		maxAttempts = 3
	case "cluster":
		timeout = 60 * time.Minute
		maxAttempts = 3
	}
	return workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    maxAttempts,
			NonRetryableErrorTypes: []string{
				"invalid_argument",
				"not_found",
				"invalid_request",
				"worker_request",
				"worker_timeout",
				"configuration_error",
				"unsupported_build_type",
			},
		},
	}
}

func classifyDatasetBuildError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "not_found", err)
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(message, "requires unstructured or mixed dataset version") ||
		strings.Contains(message, "must be ready before") ||
		strings.Contains(message, "storage_uri is required") ||
		strings.Contains(message, "file is required") {
		return temporal.NewNonRetryableApplicationError(err.Error(), "invalid_argument", err)
	}
	if strings.Contains(message, "worker task") && strings.Contains(message, "returned 4") {
		return temporal.NewNonRetryableApplicationError(err.Error(), "worker_request", err)
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(message, "context deadline exceeded") {
		return temporal.NewNonRetryableApplicationError(err.Error(), "worker_timeout", err)
	}
	if strings.TrimSpace(err.Error()) == "python ai worker url is required" {
		return temporal.NewNonRetryableApplicationError(err.Error(), "configuration_error", err)
	}
	return err
}

func datasetBuildErrorType(err error) string {
	if err == nil {
		return ""
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		if stringsValue(appErr.Type()) != "" {
			return appErr.Type()
		}
		return "application_error"
	}
	return "activity_error"
}

func (a *DatasetBuildActivities) requireRepo() (store.Repository, error) {
	if a.Repo == nil {
		return nil, fmt.Errorf("dataset build activities repository is not configured")
	}
	return a.Repo, nil
}

func (a *DatasetBuildActivities) now() time.Time {
	if a.Now != nil {
		return a.Now().UTC()
	}
	return time.Now().UTC()
}

func decodeBuildRequest[T any](value any) (T, error) {
	var zero T
	raw, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	var decoded T
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return zero, err
	}
	return decoded, nil
}

func stringsValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func (a *DatasetBuildActivities) acquireBuildSlot(ctx context.Context, buildType string) (func(), error) {
	limiter := a.getLimiter()
	return limiter.acquire(ctx, buildType)
}

func (a *DatasetBuildActivities) getLimiter() *datasetBuildLimiter {
	a.limiterOnce.Do(func() {
		a.limiter = newDatasetBuildLimiter(a.Concurrency)
	})
	return a.limiter
}

type datasetBuildLimiter struct {
	prepare   chan struct{}
	sentiment chan struct{}
	embedding chan struct{}
	cluster   chan struct{}
}

func newDatasetBuildLimiter(limits DatasetBuildConcurrencyLimits) *datasetBuildLimiter {
	return &datasetBuildLimiter{
		prepare:   makeSemaphore(limits.Prepare),
		sentiment: makeSemaphore(limits.Sentiment),
		embedding: makeSemaphore(limits.Embedding),
		cluster:   makeSemaphore(limits.Cluster),
	}
}

func (l *datasetBuildLimiter) acquire(ctx context.Context, buildType string) (func(), error) {
	semaphore := l.semaphore(buildType)
	if semaphore == nil {
		return func() {}, nil
	}
	select {
	case semaphore <- struct{}{}:
		return func() {
			<-semaphore
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *datasetBuildLimiter) semaphore(buildType string) chan struct{} {
	switch buildType {
	case "prepare":
		return l.prepare
	case "sentiment":
		return l.sentiment
	case "embedding":
		return l.embedding
	case "cluster":
		return l.cluster
	default:
		return nil
	}
}

func makeSemaphore(size int) chan struct{} {
	if size <= 0 {
		return nil
	}
	return make(chan struct{}, size)
}
