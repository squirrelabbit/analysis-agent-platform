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

// silverone 2026-05-21 δ-2: 옛 analysis_workflow.go에 있던 Starter +
// TemporalStarter 본문에서 analysis (plan v1) 흐름은 제거하고 dataset_build만
// 남긴다. 후속 단계에서 analyze를 Temporal로 옮길 때 같은 starter에
// StartAnalyzeWorkflow를 추가하면 된다.

type Starter interface {
	StartDatasetBuildWorkflow(input StartDatasetBuildInput) (string, error)
	EngineName() string
}

type StartDatasetBuildInput struct {
	JobID            string
	ProjectID        string
	DatasetID        string
	DatasetVersionID string
	BuildType        string
	RequestID        string
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
	DialTimeout           time.Duration
	ClientFactory         TemporalClientFactory
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
			RequestID:        input.RequestID,
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

type TemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	Close()
}

type TemporalClientFactory func(ctx context.Context, options client.Options) (TemporalClient, error)

func buildDatasetBuildWorkflowID(jobID string) string {
	return fmt.Sprintf("dataset-build-%s", jobID)
}
