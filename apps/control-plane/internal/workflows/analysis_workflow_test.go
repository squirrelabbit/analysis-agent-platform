package workflows

import (
	"context"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func TestNoopStarterUsesExecutionIDInWorkflowID(t *testing.T) {
	starter := NoopStarter{}
	workflowID, err := starter.StartAnalysisWorkflow(StartAnalysisInput{ExecutionID: "exec-123"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if workflowID != "analysis-execution-exec-123" {
		t.Fatalf("unexpected workflow id: %s", workflowID)
	}
}

func TestTemporalStarterStartsWorkflowWithExpectedOptions(t *testing.T) {
	fakeClient := &stubTemporalClient{}
	var capturedOptions client.Options
	starter := TemporalStarter{
		Address:   "temporal.example:7233",
		Namespace: "analysis",
		TaskQueue: "analysis-support",
		ClientFactory: func(ctx context.Context, options client.Options) (TemporalClient, error) {
			capturedOptions = options
			return fakeClient, nil
		},
	}

	workflowID, err := starter.StartAnalysisWorkflow(StartAnalysisInput{
		ExecutionID: "exec-456",
		ProjectID:   "project-1",
		RequestID:   "request-1",
		PlanID:      "plan-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if workflowID != "analysis-execution-exec-456" {
		t.Fatalf("unexpected workflow id: %s", workflowID)
	}
	if capturedOptions.HostPort != "temporal.example:7233" {
		t.Fatalf("unexpected host port: %s", capturedOptions.HostPort)
	}
	if capturedOptions.Namespace != "analysis" {
		t.Fatalf("unexpected namespace: %s", capturedOptions.Namespace)
	}
	if fakeClient.workflowName != AnalysisExecutionWorkflowName {
		t.Fatalf("unexpected workflow name: %v", fakeClient.workflowName)
	}
	if fakeClient.startOptions.ID != "analysis-execution-exec-456" {
		t.Fatalf("unexpected start workflow id: %s", fakeClient.startOptions.ID)
	}
	if fakeClient.startOptions.TaskQueue != "analysis-support" {
		t.Fatalf("unexpected task queue: %s", fakeClient.startOptions.TaskQueue)
	}
	if fakeClient.startOptions.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		t.Fatalf("unexpected workflow reuse policy: %v", fakeClient.startOptions.WorkflowIDReusePolicy)
	}
	if len(fakeClient.args) != 1 {
		t.Fatalf("unexpected arg count: %d", len(fakeClient.args))
	}
	input, ok := fakeClient.args[0].(AnalysisWorkflowInput)
	if !ok {
		t.Fatalf("unexpected workflow input type: %T", fakeClient.args[0])
	}
	if input.ExecutionID != "exec-456" || input.ProjectID != "project-1" || input.RequestID != "request-1" || input.PlanID != "plan-1" {
		t.Fatalf("unexpected workflow input: %+v", input)
	}
	if !fakeClient.closed {
		t.Fatal("expected temporal client to be closed")
	}
}

type stubTemporalClient struct {
	startOptions client.StartWorkflowOptions
	workflowName interface{}
	args         []interface{}
	closed       bool
}

func (s *stubTemporalClient) ExecuteWorkflow(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	s.startOptions = options
	s.workflowName = workflow
	s.args = args
	return stubWorkflowRun{id: options.ID}, nil
}

func (s *stubTemporalClient) Close() {
	s.closed = true
}

type stubWorkflowRun struct {
	id string
}

func (s stubWorkflowRun) GetID() string {
	return s.id
}

func (s stubWorkflowRun) GetRunID() string {
	return "run-id"
}

func (s stubWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}

func (s stubWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}
