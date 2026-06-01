package skills

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
)

// 5/11 (silverone): PythonBuildClient 도입 시 lock된 invariant.
// dataset_build를 plan skill과 분리하면서 첫 마이그레이션 대상이 dataset_clean.
// 이 test가 깨지면 silent regression — dataset_build 호출이 retry storm을
// 일으키거나 (4xx wrap 실패) noise scrub 비호환 path로 들어간다.

func TestPythonBuildClient_RunDatasetClean_HitsExpectedPath(t *testing.T) {
	var receivedPath string
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, bytes.NewReader([]byte(`{"notes":["ok"],"artifact":{"skill_name":"dataset_clean"}}`)))
	}))
	defer server.Close()

	client := NewPythonBuildClient(server.URL, server.Client())
	resp, err := client.RunDatasetClean(context.Background(), map[string]any{"dataset_version_id": "v1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedPath != "/tasks/dataset_clean" {
		t.Fatalf("expected /tasks/dataset_clean, got %s", receivedPath)
	}
	if receivedBody["dataset_version_id"] != "v1" {
		t.Fatalf("payload was not forwarded: %#v", receivedBody)
	}
	if len(resp.Notes) != 1 || resp.Notes[0] != "ok" {
		t.Fatalf("unexpected notes: %#v", resp.Notes)
	}
	if resp.Artifact["skill_name"] != "dataset_clean" {
		t.Fatalf("unexpected artifact: %#v", resp.Artifact)
	}
}

func TestPythonBuildClient_BaseURL_Empty_Returns_Error(t *testing.T) {
	client := NewPythonBuildClient("", &http.Client{})
	_, err := client.RunDatasetClean(context.Background(), map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "python ai worker url is required") {
		t.Fatalf("expected base url error, got: %v", err)
	}
}

func TestPythonBuildClient_4xxBecomesNonRetryableApplicationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"missing dataset_version_id"}`))
	}))
	defer server.Close()

	client := NewPythonBuildClient(server.URL, server.Client())
	_, err := client.RunDatasetClean(context.Background(), map[string]any{})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected temporal.ApplicationError wrap, got %T: %v", err, err)
	}
	if !appErr.NonRetryable() {
		t.Errorf("4xx expected NonRetryable=true")
	}
}

func TestPythonBuildClient_5xxRemainsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"detail":"upstream"}`))
	}))
	defer server.Close()

	client := NewPythonBuildClient(server.URL, server.Client())
	_, err := client.RunDatasetClean(context.Background(), map[string]any{})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Fatalf("5xx must not be wrapped as NonRetryableApplicationError")
	}
}

func TestPythonBuildClient_AllArtifacts_PrefersListOverSingle(t *testing.T) {
	r := PythonBuildTaskResponse{
		Artifact: map[string]any{"k": "single"},
		Artifacts: []map[string]any{
			{"k": "list-0"},
			{"k": "list-1"},
		},
	}
	got := r.AllArtifacts()
	if len(got) != 2 {
		t.Fatalf("expected list of 2, got %d", len(got))
	}
	if got[0]["k"] != "list-0" {
		t.Fatalf("expected list-0 first, got %#v", got[0])
	}
}

func TestPythonBuildClient_AllArtifacts_FallsBackToSingle(t *testing.T) {
	r := PythonBuildTaskResponse{Artifact: map[string]any{"k": "single"}}
	got := r.AllArtifacts()
	if len(got) != 1 || got[0]["k"] != "single" {
		t.Fatalf("expected single wrap, got %#v", got)
	}
}

// dataset_document_cluster_profile은 β2 (5/19) 결정으로 제거됨 — 관련 lock test
// 삭제.
