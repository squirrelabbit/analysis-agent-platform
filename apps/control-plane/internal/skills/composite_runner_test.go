package skills

import (
	"context"
	"errors"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

func TestCompositeRunnerRunsStructuredAndUnstructured(t *testing.T) {
	runner := CompositeRunner{
		Structured: stubExecutionRunner{
			result: ExecutionRunResult{
				Artifacts:      map[string]string{"structured": "ok"},
				Notes:          []string{"structured done"},
				ProcessedSteps: 1,
				Engine:         "duckdb",
				StepHooks:      []StepHookRecord{{Phase: "before", StepID: "step-1", SkillName: "structured_kpi_summary"}},
			},
		},
		Unstructured: stubExecutionRunner{
			result: ExecutionRunResult{
				Artifacts: map[string]string{
					"filter":       "ok",
					"keywords":     "ok",
					"breakdown":    "ok",
					"compare":      "ok",
					"sentiment":    "ok",
					"trend":        "ok",
					"unstructured": "ok",
					"semantic":     "ok",
					"evidence":     "ok",
				},
				Notes:          []string{"filter done", "keywords done", "breakdown done", "compare done", "sentiment done", "trend done", "unstructured done", "semantic done", "evidence done"},
				ProcessedSteps: 9,
				Engine:         "python-ai",
				StepHooks:      []StepHookRecord{{Phase: "before", StepID: "step-2", SkillName: "document_filter"}},
			},
		},
	}

	result, err := runner.Run(context.Background(), domain.ExecutionSummary{
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "structured_kpi_summary"},
				{StepID: "step-2", SkillName: "document_filter"},
				{StepID: "step-3", SkillName: "keyword_frequency"},
				{StepID: "step-4", SkillName: "issue_breakdown_summary"},
				{StepID: "step-5", SkillName: "issue_period_compare"},
				{StepID: "step-6", SkillName: "issue_sentiment_summary"},
				{StepID: "step-7", SkillName: "issue_trend_summary"},
				{StepID: "step-8", SkillName: "unstructured_issue_summary"},
				{StepID: "step-9", SkillName: "semantic_search"},
				{StepID: "step-10", SkillName: "issue_evidence_summary"},
				{StepID: "step-11", SkillName: "rank"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Engine != "duckdb+python-ai" {
		t.Fatalf("unexpected engine: %s", result.Engine)
	}
	if result.ProcessedSteps != 10 {
		t.Fatalf("unexpected processed steps: %d", result.ProcessedSteps)
	}
	if len(result.Artifacts) != 10 {
		t.Fatalf("unexpected artifacts: %+v", result.Artifacts)
	}
	if len(result.Notes) != 11 {
		t.Fatalf("unexpected notes: %+v", result.Notes)
	}
	if len(result.StepHooks) != 2 {
		t.Fatalf("unexpected step hooks: %+v", result.StepHooks)
	}
}

func TestCompositeRunnerFailsWhenUnstructuredRunnerIsMissing(t *testing.T) {
	runner := CompositeRunner{
		Structured: stubExecutionRunner{},
	}

	_, err := runner.Run(context.Background(), domain.ExecutionSummary{
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-2", SkillName: "evidence_pack"},
			},
		},
	})
	if err == nil || err.Error() != "unstructured runner is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompositeRunnerReturnsStructuredError(t *testing.T) {
	runner := CompositeRunner{
		Structured: stubExecutionRunner{err: errors.New("duckdb failed")},
	}

	_, err := runner.Run(context.Background(), domain.ExecutionSummary{
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "structured_kpi_summary"},
			},
		},
	})
	if err == nil || err.Error() != "duckdb failed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type stubExecutionRunner struct {
	result ExecutionRunResult
	err    error
}

func (s stubExecutionRunner) Run(_ context.Context, _ domain.ExecutionSummary) (ExecutionRunResult, error) {
	if s.err != nil {
		return ExecutionRunResult{}, s.err
	}
	return s.result, nil
}
