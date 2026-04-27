package obs

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

// ActivityLogInfo holds Temporal execution metadata extracted from an activity context.
type ActivityLogInfo struct {
	WorkflowID string
	RunID      string
	Attempt    int32
}

// GetActivityLogInfo extracts Temporal activity metadata from ctx.
// Safe to call outside a Temporal worker (e.g. in unit tests) — returns
// zero-value ActivityLogInfo when the context is not a Temporal activity context.
func GetActivityLogInfo(ctx context.Context) (info ActivityLogInfo) {
	func() {
		defer func() { recover() }() //nolint:errcheck // deliberate: Temporal panics outside worker context
		ai := activity.GetInfo(ctx)
		info = ActivityLogInfo{
			WorkflowID: ai.WorkflowExecution.ID,
			RunID:      ai.WorkflowExecution.RunID,
			Attempt:    ai.Attempt,
		}
	}()
	return info
}

// EnrichActivityContext adds request_id, execution_id, and workflow_id to ctx.
// Pass the ActivityLogInfo returned by GetActivityLogInfo for the workflow_id.
func EnrichActivityContext(ctx context.Context, requestID, executionID string, info ActivityLogInfo) context.Context {
	if requestID != "" {
		ctx = WithRequestID(ctx, requestID)
	}
	if executionID != "" {
		ctx = WithExecutionID(ctx, executionID)
	}
	if info.WorkflowID != "" {
		ctx = WithWorkflowID(ctx, info.WorkflowID)
	}
	return ctx
}

// LogActivityStarted emits workflow.activity.started (and workflow.activity.retried when attempt > 1).
// Returns the start time for passing to LogActivityCompleted or LogActivityFailed.
func LogActivityStarted(ctx context.Context, activityName string, info ActivityLogInfo) time.Time {
	l := FromContext(ctx)
	if info.Attempt > 1 {
		l.Warn("activity retried",
			"event", "workflow.activity.retried",
			"activity_name", activityName,
			"workflow_id", info.WorkflowID,
			"attempt", info.Attempt,
		)
	}
	l.Info("activity started",
		"event", "workflow.activity.started",
		"activity_name", activityName,
		"workflow_id", info.WorkflowID,
		"attempt", info.Attempt,
	)
	return time.Now()
}

// LogActivityCompleted emits workflow.activity.completed.
func LogActivityCompleted(ctx context.Context, activityName string, startedAt time.Time) {
	FromContext(ctx).Info("activity completed",
		"event", "workflow.activity.completed",
		"activity_name", activityName,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
}

// LogActivityFailed emits workflow.activity.failed.
func LogActivityFailed(ctx context.Context, activityName string, startedAt time.Time, err error) {
	FromContext(ctx).Error("activity failed",
		"event", "workflow.activity.failed",
		"activity_name", activityName,
		"duration_ms", time.Since(startedAt).Milliseconds(),
		"error", err.Error(),
	)
}
