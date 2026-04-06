package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/config"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// Analysis workflow는 control plane이 시작하는 workflow 계약을 정의한다.
//
// 현재는 starter와 worker skeleton만 구현되어 있고, 실제 skill fan-out과
// waiting / retry / resume 세부 로직은 후속 단계에서 workflow/activity로 확장한다.

type Starter interface {
	StartAnalysisWorkflow(input StartAnalysisInput) (string, error)
	StartDatasetBuildWorkflow(input StartDatasetBuildInput) (string, error)
	EngineName() string
}

type StartAnalysisInput struct {
	ExecutionID      string
	ProjectID        string
	RequestID        string
	PlanID           string
	DatasetVersionID *string
}

type StartDatasetBuildInput struct {
	JobID            string
	ProjectID        string
	DatasetID        string
	DatasetVersionID string
	BuildType        string
}

func NewStarter(cfg config.Config) (Starter, error) {
	switch cfg.WorkflowEngine {
	case "", "noop":
		return NoopStarter{}, nil
	case "temporal":
		return TemporalStarter{
			Address:               cfg.TemporalAddress,
			Namespace:             cfg.TemporalNamespace,
			TaskQueue:             cfg.TemporalTaskQueue,
			DatasetBuildTaskQueue: cfg.TemporalBuildTaskQueue,
		}, nil
	default:
		return nil, errors.New("unsupported workflow engine: " + cfg.WorkflowEngine)
	}
}

type NoopStarter struct{}

func (NoopStarter) StartAnalysisWorkflow(input StartAnalysisInput) (string, error) {
	return buildWorkflowID(input.ExecutionID), nil
}

func (NoopStarter) StartDatasetBuildWorkflow(input StartDatasetBuildInput) (string, error) {
	return buildDatasetBuildWorkflowID(input.JobID), nil
}

func (NoopStarter) EngineName() string {
	return "noop"
}

type TemporalStarter struct {
	Address               string
	Namespace             string
	TaskQueue             string
	DatasetBuildTaskQueue string
	WorkflowName          string
	DialTimeout           time.Duration
	ClientFactory         TemporalClientFactory
}

func (s TemporalStarter) StartAnalysisWorkflow(input StartAnalysisInput) (string, error) {
	workflowName := s.WorkflowName
	if workflowName == "" {
		workflowName = AnalysisExecutionWorkflowName
	}
	return s.startWorkflow(
		buildWorkflowID(input.ExecutionID),
		strings.TrimSpace(s.TaskQueue),
		workflowName,
		AnalysisWorkflowInput{
			ExecutionID:      input.ExecutionID,
			ProjectID:        input.ProjectID,
			RequestID:        input.RequestID,
			PlanID:           input.PlanID,
			DatasetVersionID: input.DatasetVersionID,
			RequestedAt:      time.Now().UTC(),
		},
	)
}

func (s TemporalStarter) StartDatasetBuildWorkflow(input StartDatasetBuildInput) (string, error) {
	taskQueue := strings.TrimSpace(s.DatasetBuildTaskQueue)
	if taskQueue == "" {
		taskQueue = strings.TrimSpace(s.TaskQueue)
	}
	return s.startWorkflow(
		buildDatasetBuildWorkflowID(input.JobID),
		taskQueue,
		DatasetBuildWorkflowName,
		DatasetBuildWorkflowInput{
			JobID:            input.JobID,
			ProjectID:        input.ProjectID,
			DatasetID:        input.DatasetID,
			DatasetVersionID: input.DatasetVersionID,
			BuildType:        input.BuildType,
			RequestedAt:      time.Now().UTC(),
		},
	)
}

func (s TemporalStarter) startWorkflow(workflowID string, taskQueue string, workflowName string, payload any) (string, error) {
	if s.ClientFactory == nil {
		s.ClientFactory = func(ctx context.Context, options client.Options) (TemporalClient, error) {
			return client.DialContext(ctx, options)
		}
	}
	if s.DialTimeout <= 0 {
		s.DialTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.DialTimeout)
	defer cancel()

	c, err := s.ClientFactory(ctx, client.Options{
		HostPort:  s.Address,
		Namespace: s.Namespace,
	})
	if err != nil {
		return "", err
	}
	defer c.Close()
	if strings.TrimSpace(taskQueue) == "" {
		taskQueue = strings.TrimSpace(s.TaskQueue)
	}

	run, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:                                       workflowID,
			TaskQueue:                                taskQueue,
			WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
		},
		workflowName,
		payload,
	)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return workflowID, nil
		}
		return "", err
	}
	return run.GetID(), nil
}

func (s TemporalStarter) EngineName() string {
	return "temporal"
}

const AnalysisExecutionWorkflowName = "analysis.execution.v1"

type AnalysisWorkflowInput struct {
	ExecutionID      string    `json:"execution_id"`
	ProjectID        string    `json:"project_id"`
	RequestID        string    `json:"request_id"`
	PlanID           string    `json:"plan_id"`
	DatasetVersionID *string   `json:"dataset_version_id,omitempty"`
	RequestedAt      time.Time `json:"requested_at"`
}

type TemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	Close()
}

type TemporalClientFactory func(ctx context.Context, options client.Options) (TemporalClient, error)

func buildWorkflowID(executionID string) string {
	return fmt.Sprintf("analysis-execution-%s", executionID)
}

func buildDatasetBuildWorkflowID(jobID string) string {
	return fmt.Sprintf("dataset-build-%s", jobID)
}
