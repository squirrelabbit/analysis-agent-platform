package executionresult

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

func TestBuildV1SuppressesStaleWaitingStateAfterCompletion(t *testing.T) {
	execution := domain.ExecutionSummary{
		ExecutionID: "exec-1",
		Status:      "completed",
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "issue_evidence_summary"},
			},
			CreatedAt: time.Now().UTC(),
		},
		Artifacts: map[string]string{
			"step:step-1:issue_evidence_summary": `{"skill_name":"issue_evidence_summary","summary":"완료된 요약"}`,
		},
		Events: []domain.ExecutionEvent{
			{
				EventType: "WORKFLOW_WAITING",
				Payload: map[string]any{
					"waiting_for": "sentiment_labels",
					"reason":      "dataset version sentiment labels are not ready",
				},
			},
			{
				EventType: "RESUME_ENQUEUED",
				Payload: map[string]any{
					"triggered_by": "dataset_build_job",
				},
			},
			{
				EventType: "WORKFLOW_COMPLETED",
			},
		},
	}

	result := BuildV1(execution)
	if result.Waiting != nil {
		t.Fatalf("expected waiting state to be cleared after completion: %+v", result.Waiting)
	}
	for _, warning := range result.Warnings {
		if warning == "waiting_for=sentiment_labels dataset version sentiment labels are not ready" {
			t.Fatalf("expected stale waiting warning to be removed: %+v", result.Warnings)
		}
	}
}
