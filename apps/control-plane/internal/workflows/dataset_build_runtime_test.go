package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
)

func TestDatasetBuildLimiterHonorsPerTypeConcurrency(t *testing.T) {
	limiter := newDatasetBuildLimiter(DatasetBuildConcurrencyLimits{
		Prepare:   1,
		Sentiment: 2,
		Embedding: 1,
	})

	releasePrepare, err := limiter.acquire(context.Background(), "prepare")
	if err != nil {
		t.Fatalf("unexpected first prepare acquire error: %v", err)
	}
	defer releasePrepare()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := limiter.acquire(ctx, "prepare"); err == nil {
		t.Fatal("expected second prepare acquire to block until context timeout")
	}

	releaseSentiment1, err := limiter.acquire(context.Background(), "sentiment")
	if err != nil {
		t.Fatalf("unexpected first sentiment acquire error: %v", err)
	}
	defer releaseSentiment1()
	releaseSentiment2, err := limiter.acquire(context.Background(), "sentiment")
	if err != nil {
		t.Fatalf("unexpected second sentiment acquire error: %v", err)
	}
	defer releaseSentiment2()
}

func TestDatasetBuildExecuteActivityOptionsUseBuildSpecificPolicy(t *testing.T) {
	prepareOptions := datasetBuildExecuteActivityOptions("prepare")
	sentimentOptions := datasetBuildExecuteActivityOptions("sentiment")
	embeddingOptions := datasetBuildExecuteActivityOptions("embedding")

	if prepareOptions.StartToCloseTimeout != 75*time.Minute {
		t.Fatalf("unexpected prepare timeout: %s", prepareOptions.StartToCloseTimeout)
	}
	if sentimentOptions.StartToCloseTimeout != 45*time.Minute {
		t.Fatalf("unexpected sentiment timeout: %s", sentimentOptions.StartToCloseTimeout)
	}
	if embeddingOptions.StartToCloseTimeout != 60*time.Minute {
		t.Fatalf("unexpected embedding timeout: %s", embeddingOptions.StartToCloseTimeout)
	}
	if prepareOptions.RetryPolicy == nil || prepareOptions.RetryPolicy.MaximumAttempts != 4 {
		t.Fatalf("unexpected prepare retry policy: %+v", prepareOptions.RetryPolicy)
	}
	if sentimentOptions.RetryPolicy == nil || sentimentOptions.RetryPolicy.MaximumAttempts != 4 {
		t.Fatalf("unexpected sentiment retry policy: %+v", sentimentOptions.RetryPolicy)
	}
	if embeddingOptions.RetryPolicy == nil || embeddingOptions.RetryPolicy.MaximumAttempts != 3 {
		t.Fatalf("unexpected embedding retry policy: %+v", embeddingOptions.RetryPolicy)
	}
}

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
