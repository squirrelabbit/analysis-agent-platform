package workflows

import (
	"context"
	"errors"
	"fmt"
	"time"

	"analysis-support-platform/control-plane/internal/config"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Analysis workflow는 control plane이 시작하는 workflow 계약을 정의한다.
//
// 현재는 starter와 worker skeleton만 구현되어 있고, 실제 skill fan-out과
// waiting / retry / resume 세부 로직은 후속 단계에서 workflow/activity로 확장한다.

type Starter interface {
	StartAnalysisWorkflow(input StartAnalysisInput) (string, error)
	EngineName() string
}

type StartAnalysisInput struct {
	ExecutionID      string
	ProjectID        string
	RequestID        string
	PlanID           string
	DatasetVersionID *string
}

func NewStarter(cfg config.Config) (Starter, error) {
	switch cfg.WorkflowEngine {
	case "", "noop":
		return NoopStarter{}, nil
	case "temporal":
		return TemporalStarter{
			Address:   cfg.TemporalAddress,
			Namespace: cfg.TemporalNamespace,
			TaskQueue: cfg.TemporalTaskQueue,
		}, nil
	default:
		return nil, errors.New("unsupported workflow engine: " + cfg.WorkflowEngine)
	}
}

type NoopStarter struct{}

func (NoopStarter) StartAnalysisWorkflow(input StartAnalysisInput) (string, error) {
	return buildWorkflowID(input.ExecutionID), nil
}

func (NoopStarter) EngineName() string {
	return "noop"
}

type TemporalStarter struct {
	Address       string
	Namespace     string
	TaskQueue     string
	WorkflowName  string
	DialTimeout   time.Duration
	ClientFactory TemporalClientFactory
}

func (s TemporalStarter) StartAnalysisWorkflow(input StartAnalysisInput) (string, error) {
	if s.ClientFactory == nil {
		s.ClientFactory = func(ctx context.Context, options client.Options) (TemporalClient, error) {
			return client.DialContext(ctx, options)
		}
	}
	if s.WorkflowName == "" {
		s.WorkflowName = AnalysisExecutionWorkflowName
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

	workflowID := buildWorkflowID(input.ExecutionID)
	run, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:                                       workflowID,
			TaskQueue:                                s.TaskQueue,
			WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
		},
		s.WorkflowName,
		AnalysisWorkflowInput{
			ExecutionID:      input.ExecutionID,
			ProjectID:        input.ProjectID,
			RequestID:        input.RequestID,
			PlanID:           input.PlanID,
			DatasetVersionID: input.DatasetVersionID,
			RequestedAt:      time.Now().UTC(),
		},
	)
	if err != nil {
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
