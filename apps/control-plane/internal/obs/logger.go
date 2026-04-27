package obs

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey int

const (
	ctxRequestID  ctxKey = iota
	ctxExecutionID
	ctxWorkflowID
)

// Logger is the global structured logger. Call Init before first use.
var Logger *slog.Logger

func init() {
	Logger = slog.Default()
}

// Init configures the global JSON logger with a service label.
// Reads LOG_LEVEL env var (DEBUG|INFO|WARN|ERROR); defaults to INFO.
func Init(service string) {
	level := slog.LevelInfo
	if v := strings.TrimSpace(os.Getenv("LOG_LEVEL")); v != "" {
		var l slog.Level
		if err := l.UnmarshalText([]byte(strings.ToUpper(v))); err == nil {
			level = l
		}
	}
	Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).
		With("service", service)
	slog.SetDefault(Logger)
}

// WithRequestID stores a request ID in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxRequestID, id)
}

// WithExecutionID stores an execution ID in ctx.
func WithExecutionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxExecutionID, id)
}

// WithWorkflowID stores a workflow ID in ctx.
func WithWorkflowID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxWorkflowID, id)
}

// FromContext returns Logger enriched with correlation IDs stored in ctx.
func FromContext(ctx context.Context) *slog.Logger {
	l := Logger
	if id, _ := ctx.Value(ctxRequestID).(string); id != "" {
		l = l.With("request_id", id)
	}
	if id, _ := ctx.Value(ctxExecutionID).(string); id != "" {
		l = l.With("execution_id", id)
	}
	if id, _ := ctx.Value(ctxWorkflowID).(string); id != "" {
		l = l.With("workflow_id", id)
	}
	return l
}

// RequestIDFromContext returns the request ID stored in ctx, or empty string.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxRequestID).(string)
	return id
}
