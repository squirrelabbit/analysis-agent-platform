package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
)

func TestClassifyDatasetBuildErrorTreatsWorkerTimeoutAsNonRetryable(t *testing.T) {
	err := classifyDatasetBuildError(context.DeadlineExceeded)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected temporal application error: %T", err)
	}
	if appErr.Type() != "worker_timeout" {
		t.Fatalf("unexpected error type: %s", appErr.Type())
	}
}

// silverone 2026-05-28 — LLOA long-running build (doc_genuineness / clause_label)
// 의 timeout + retry race 잠금. 동일 output_path에 두 attempt가 append 하지 않도록
// MaximumAttempts=1 + StartToCloseTimeout 90분.
func TestDatasetBuildExecuteActivityOptionsLLOABuildTypes(t *testing.T) {
	cases := []struct {
		buildType       string
		wantTimeout     time.Duration
		wantMaxAttempts int32
	}{
		{"clean", 20 * time.Minute, 4},
		{"doc_genuineness", 90 * time.Minute, 1},
		{"clause_label", 90 * time.Minute, 1},
		{"unknown_build_type", 20 * time.Minute, 4},
	}
	for _, tc := range cases {
		t.Run(tc.buildType, func(t *testing.T) {
			opts := datasetBuildExecuteActivityOptions(tc.buildType)
			if opts.StartToCloseTimeout != tc.wantTimeout {
				t.Errorf("StartToCloseTimeout: want %v, got %v", tc.wantTimeout, opts.StartToCloseTimeout)
			}
			if opts.RetryPolicy == nil {
				t.Fatalf("RetryPolicy should not be nil")
			}
			if opts.RetryPolicy.MaximumAttempts != tc.wantMaxAttempts {
				t.Errorf("MaximumAttempts: want %d, got %d", tc.wantMaxAttempts, opts.RetryPolicy.MaximumAttempts)
			}
		})
	}
}
