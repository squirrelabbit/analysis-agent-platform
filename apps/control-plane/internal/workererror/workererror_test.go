package workererror

import (
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
)

func TestRejectionWrapsAsNonRetryableApplicationError(t *testing.T) {
	err := Rejection(400, []byte(`{"error":"bad input"}`))
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected ApplicationError, got %T", err)
	}
	if !appErr.NonRetryable() {
		t.Error("expected NonRetryable=true")
	}
	if appErr.Type() != RejectionErrType {
		t.Errorf("expected type %q, got %q", RejectionErrType, appErr.Type())
	}
	if !strings.Contains(err.Error(), "bad input") {
		t.Errorf("expected message in error: %v", err)
	}
}

func TestRejectionFallsBackToStatusCodeWhenBodyEmpty(t *testing.T) {
	err := Rejection(422, nil)
	if !strings.Contains(err.Error(), "422") {
		t.Errorf("expected status code in fallback message: %v", err)
	}
}

func TestUpstreamReturnsPlainErrorRetryable(t *testing.T) {
	err := Upstream(503, []byte(`{"error":"upstream"}`))
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Errorf("Upstream must NOT be non-retryable")
	}
	if !strings.Contains(err.Error(), "upstream") {
		t.Errorf("expected upstream message: %v", err)
	}
}

func TestParseMessageHandlesAllShapes(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		expected string
	}{
		{"error key", `{"error":"err msg"}`, "err msg"},
		{"detail key", `{"detail":"detail msg"}`, "detail msg"},
		{"message key", `{"message":"message msg"}`, "message msg"},
		{"error wins over detail", `{"error":"e","detail":"d"}`, "e"},
		{"raw text fallback", `not json`, "not json"},
		{"empty body", ``, ""},
		{"empty error string skipped", `{"error":"","detail":"fallback"}`, "fallback"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ParseMessage([]byte(c.body))
			if got != c.expected {
				t.Errorf("expected %q, got %q", c.expected, got)
			}
		})
	}
}
