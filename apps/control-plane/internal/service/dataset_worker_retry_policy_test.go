package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.temporal.io/sdk/temporal"

	"analysis-support-platform/control-plane/internal/workererror"
)

// 5/6 fix lock: dataset build이 Temporal activity 안에서 호출되므로 worker 4xx →
// NonRetryable로 wrap(payload contract 회귀가 build workflow를 무한 retry시키지
// 않게). 이 wrap은 ADR-031 4단계로 PythonBuildClient.RunTask에 통일됐다(2026-06-29).

func newDatasetWorkerTestService(serverURL string) *DatasetService {
	return &DatasetService{
		pythonAIWorkerURL: serverURL,
		httpClient:        &http.Client{},
	}
}

func TestDatasetWorker4xxBecomesNonRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"text_columns is required"}`))
	}))
	defer server.Close()

	s := newDatasetWorkerTestService(server.URL)
	_, err := s.buildClient().RunTask(context.Background(), "/tasks/dataset_clause_label", map[string]any{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("expected NonRetryable, got %T: %v", err, err)
	}
	if appErr.Type() != workererror.RejectionErrType {
		t.Errorf("expected type %q, got %q", workererror.RejectionErrType, appErr.Type())
	}
}

func TestDatasetWorker5xxRemainsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"worker crashed"}`))
	}))
	defer server.Close()

	s := newDatasetWorkerTestService(server.URL)
	_, err := s.buildClient().RunTask(context.Background(), "/tasks/dataset_doc_genuineness", map[string]any{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Errorf("5xx must NOT be non-retryable: %v", err)
	}
}
