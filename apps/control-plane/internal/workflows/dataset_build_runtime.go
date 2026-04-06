package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"

	"go.temporal.io/sdk/activity"
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
}

type WaitingExecutionResumer interface {
	ResumeWaitingExecutionsForDatasetVersion(projectID, datasetVersionID, reason, triggeredBy string) error
}

type DatasetBuildActivities struct {
	Repo    store.Repository
	Builder DatasetBuildRunner
	Resumer WaitingExecutionResumer
	Now     func() time.Time
}

func RegisterDatasetBuildRuntime(registrar RuntimeRegistrar, activities DatasetBuildActivities) {
	registrar.RegisterWorkflowWithOptions(
		DatasetBuildWorkflow,
		workflow.RegisterOptions{Name: DatasetBuildWorkflowName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkDatasetBuildJobRunning,
		activity.RegisterOptions{Name: MarkDatasetBuildJobRunningActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.ExecuteDatasetBuildJob,
		activity.RegisterOptions{Name: ExecuteDatasetBuildJobActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkDatasetBuildJobCompleted,
		activity.RegisterOptions{Name: MarkDatasetBuildJobCompletedActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.MarkDatasetBuildJobFailed,
		activity.RegisterOptions{Name: MarkDatasetBuildJobFailedActivityName},
	)
	registrar.RegisterActivityWithOptions(
		activities.ResumeWaitingExecutionsForDatasetVersion,
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

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
	})

	var started DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(ctx, MarkDatasetBuildJobRunningActivityName, input).Get(ctx, &started); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, ExecuteDatasetBuildJobActivityName, input).Get(ctx, nil); err != nil {
		var failed DatasetBuildLifecycleResult
		failInput := map[string]any{
			"workflow_input": input,
			"error_message":  err.Error(),
		}
		if markErr := workflow.ExecuteActivity(ctx, MarkDatasetBuildJobFailedActivityName, failInput).Get(ctx, &failed); markErr != nil {
			return DatasetBuildLifecycleResult{}, fmt.Errorf("execute dataset build job: %w; mark failed: %v", err, markErr)
		}
		return failed, err
	}

	var completed DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(ctx, MarkDatasetBuildJobCompletedActivityName, input).Get(ctx, &completed); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	var resumed DatasetBuildLifecycleResult
	if err := workflow.ExecuteActivity(ctx, ResumeWaitingExecutionsActivityName, input).Get(ctx, &resumed); err != nil {
		logger.Error("resume waiting executions failed", "job_id", input.JobID, "error", err)
		return completed, nil
	}
	completed.Executions = resumed.Executions
	return completed, nil
}

func (a DatasetBuildActivities) MarkDatasetBuildJobRunning(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
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

func (a DatasetBuildActivities) ExecuteDatasetBuildJob(ctx context.Context, input DatasetBuildWorkflowInput) error {
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

	switch job.BuildType {
	case "prepare":
		request, err := decodeBuildRequest[domain.DatasetPrepareRequest](job.Request)
		if err != nil {
			return err
		}
		_, err = a.Builder.BuildPrepare(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return err
	case "sentiment":
		request, err := decodeBuildRequest[domain.DatasetSentimentBuildRequest](job.Request)
		if err != nil {
			return err
		}
		_, err = a.Builder.BuildSentiment(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return err
	case "embedding":
		request, err := decodeBuildRequest[domain.DatasetEmbeddingBuildRequest](job.Request)
		if err != nil {
			return err
		}
		_, err = a.Builder.BuildEmbeddings(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
		return err
	default:
		return fmt.Errorf("unsupported dataset build type: %s", job.BuildType)
	}
}

func (a DatasetBuildActivities) MarkDatasetBuildJobCompleted(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
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

func (a DatasetBuildActivities) MarkDatasetBuildJobFailed(ctx context.Context, payload map[string]any) (DatasetBuildLifecycleResult, error) {
	repo, err := a.requireRepo()
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	input, err := decodeBuildRequest[DatasetBuildWorkflowInput](payload["workflow_input"])
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	job, err := repo.GetDatasetBuildJob(input.ProjectID, input.JobID)
	if err != nil {
		return DatasetBuildLifecycleResult{}, err
	}

	now := a.now()
	message := stringsValue(payload["error_message"])
	job.Status = "failed"
	job.CompletedAt = &now
	if message != "" {
		job.ErrorMessage = &message
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

func (a DatasetBuildActivities) ResumeWaitingExecutionsForDatasetVersion(ctx context.Context, input DatasetBuildWorkflowInput) (DatasetBuildLifecycleResult, error) {
	if a.Resumer == nil {
		return DatasetBuildLifecycleResult{
			JobID:     input.JobID,
			Status:    "completed",
			Timestamp: a.now(),
			BuildType: input.BuildType,
		}, nil
	}

	reason := fmt.Sprintf("dataset build job completed: %s", input.BuildType)
	if err := a.Resumer.ResumeWaitingExecutionsForDatasetVersion(input.ProjectID, input.DatasetVersionID, reason, "dataset_build_job"); err != nil {
		return DatasetBuildLifecycleResult{}, err
	}
	return DatasetBuildLifecycleResult{
		JobID:     input.JobID,
		Status:    "completed",
		Timestamp: a.now(),
		BuildType: input.BuildType,
	}, nil
}

func (a DatasetBuildActivities) requireRepo() (store.Repository, error) {
	if a.Repo == nil {
		return nil, fmt.Errorf("dataset build activities repository is not configured")
	}
	return a.Repo, nil
}

func (a DatasetBuildActivities) now() time.Time {
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
