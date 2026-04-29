package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/obs"
)

func newTestLogger(buf *bytes.Buffer) {
	obs.Logger = slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func parseLastLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 {
		t.Fatal("no log output")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("unmarshal log: %v\nraw: %s", err, lines[len(lines)-1])
	}
	return entry
}

func parseAllLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal log line: %v\nraw: %s", err, line)
		}
		entries = append(entries, entry)
	}
	return entries
}

// Case 1: normal activity execution (attempt=1).
func TestLogActivityStarted_normalCase(t *testing.T) {
	var buf bytes.Buffer
	newTestLogger(&buf)

	ctx := obs.WithRequestID(context.Background(), "req-abc")
	info := obs.ActivityLogInfo{WorkflowID: "wf-001", RunID: "run-001", Attempt: 1}

	startedAt := obs.LogActivityStarted(ctx, "analysis.mark_execution_running.v1", info)

	if startedAt.IsZero() {
		t.Error("startedAt must not be zero")
	}
	entries := parseAllLines(t, &buf)
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	entry := entries[0]
	if got := entry["event"]; got != "workflow.activity.started" {
		t.Errorf("event: want workflow.activity.started, got %v", got)
	}
	if got := entry["activity_name"]; got != "analysis.mark_execution_running.v1" {
		t.Errorf("activity_name: got %v", got)
	}
	if got := entry["workflow_id"]; got != "wf-001" {
		t.Errorf("workflow_id: got %v", got)
	}
	if got := entry["request_id"]; got != "req-abc" {
		t.Errorf("request_id: got %v", got)
	}
}

// Case 2: activity retry (attempt > 1).
func TestLogActivityStarted_retryCase(t *testing.T) {
	var buf bytes.Buffer
	newTestLogger(&buf)

	ctx := context.Background()
	info := obs.ActivityLogInfo{WorkflowID: "wf-002", Attempt: 3}

	obs.LogActivityStarted(ctx, "analysis.execute_plan.v1", info)

	entries := parseAllLines(t, &buf)
	if len(entries) != 2 {
		t.Fatalf("expected 2 log entries for retry (retried+started), got %d", len(entries))
	}
	events := make([]string, len(entries))
	for i, e := range entries {
		events[i], _ = e["event"].(string)
	}
	if events[0] != "workflow.activity.retried" {
		t.Errorf("first event: want workflow.activity.retried, got %s", events[0])
	}
	if events[1] != "workflow.activity.started" {
		t.Errorf("second event: want workflow.activity.started, got %s", events[1])
	}
	if got := entries[0]["attempt"]; got != float64(3) {
		t.Errorf("attempt in retried event: want 3, got %v", got)
	}
}

// Case 3: activity failure path.
func TestLogActivityFailed_failureCase(t *testing.T) {
	var buf bytes.Buffer
	newTestLogger(&buf)

	ctx := obs.WithExecutionID(context.Background(), "exec-123")
	info := obs.ActivityLogInfo{WorkflowID: "wf-003", Attempt: 1}
	startedAt := obs.LogActivityStarted(ctx, "analysis.execute_plan.v1", info)

	// Simulate some work
	time.Sleep(time.Millisecond)

	obs.LogActivityFailed(ctx, "analysis.execute_plan.v1", startedAt, errors.New("repository unavailable"))

	entries := parseAllLines(t, &buf)
	// started + failed
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}
	failed := entries[len(entries)-1]
	if got := failed["event"]; got != "workflow.activity.failed" {
		t.Errorf("event: want workflow.activity.failed, got %v", got)
	}
	if got := failed["error"]; got != "repository unavailable" {
		t.Errorf("error: got %v", got)
	}
	durationMs, ok := failed["duration_ms"].(float64)
	if !ok || durationMs < 0 {
		t.Errorf("duration_ms should be non-negative number, got %v", failed["duration_ms"])
	}
	if got := failed["execution_id"]; got != "exec-123" {
		t.Errorf("execution_id: got %v", got)
	}
}

func TestLogActivityCompleted(t *testing.T) {
	var buf bytes.Buffer
	newTestLogger(&buf)

	ctx := context.Background()
	startedAt := time.Now().Add(-50 * time.Millisecond)
	obs.LogActivityCompleted(ctx, "dataset_build.execute.v1", startedAt)

	entry := parseLastLine(t, &buf)
	if got := entry["event"]; got != "workflow.activity.completed" {
		t.Errorf("event: want workflow.activity.completed, got %v", got)
	}
	durationMs, ok := entry["duration_ms"].(float64)
	if !ok || durationMs < 0 {
		t.Errorf("duration_ms: got %v", entry["duration_ms"])
	}
}

func TestEnrichActivityContext(t *testing.T) {
	info := obs.ActivityLogInfo{WorkflowID: "wf-enrich", RunID: "run-enrich", Attempt: 1}
	ctx := obs.EnrichActivityContext(context.Background(), "req-enrich", "exec-enrich", info)

	if got := obs.RequestIDFromContext(ctx); got != "req-enrich" {
		t.Errorf("request_id: got %q", got)
	}
}
