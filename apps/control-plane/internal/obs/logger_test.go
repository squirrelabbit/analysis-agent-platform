package obs_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"analysis-support-platform/control-plane/internal/obs"
)

func TestInit_defaultLevelIsInfo(t *testing.T) {
	os.Unsetenv("LOG_LEVEL")
	obs.Init("test-svc")

	ctx := context.Background()
	if obs.Logger.Handler().Enabled(ctx, slog.LevelDebug) {
		t.Error("DEBUG should not be enabled at default INFO level")
	}
	if !obs.Logger.Handler().Enabled(ctx, slog.LevelInfo) {
		t.Error("INFO should be enabled by default")
	}
}

func TestInit_debugLevelEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "DEBUG")
	obs.Init("test-svc")

	if !obs.Logger.Handler().Enabled(context.Background(), slog.LevelDebug) {
		t.Error("DEBUG should be enabled when LOG_LEVEL=DEBUG")
	}
}

func TestInit_invalidLevelFallsBackToInfo(t *testing.T) {
	t.Setenv("LOG_LEVEL", "NOTAREAL")
	obs.Init("test-svc")

	ctx := context.Background()
	if !obs.Logger.Handler().Enabled(ctx, slog.LevelInfo) {
		t.Error("INFO should be enabled after invalid LOG_LEVEL")
	}
	if obs.Logger.Handler().Enabled(ctx, slog.LevelDebug) {
		t.Error("DEBUG should not be enabled after invalid LOG_LEVEL")
	}
}

func TestFromContext_emptyContextReturnsLogger(t *testing.T) {
	obs.Init("test-svc")
	l := obs.FromContext(context.Background())
	if l == nil {
		t.Fatal("FromContext returned nil for empty context")
	}
}

func TestFromContext_withAllIDs(t *testing.T) {
	obs.Init("test-svc")
	ctx := obs.WithRequestID(context.Background(), "req-1")
	ctx = obs.WithExecutionID(ctx, "exec-1")
	ctx = obs.WithWorkflowID(ctx, "wf-1")

	l := obs.FromContext(ctx)
	if l == nil {
		t.Fatal("FromContext returned nil")
	}
	// Must not panic
	l.Info("context fields attached", "event", "unit.test")
}

func TestContextHelpers_roundtrip(t *testing.T) {
	obs.Init("test-svc")
	ctx := obs.WithRequestID(context.Background(), "r")
	ctx = obs.WithExecutionID(ctx, "e")
	ctx = obs.WithWorkflowID(ctx, "w")

	// FromContext must return a non-nil, usable logger without panicking.
	l := obs.FromContext(ctx)
	l.Info("roundtrip ok")
}
