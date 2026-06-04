package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/metrics"
)

// silverone 2026-06-04 (metrics 1차) — postPythonAITask가 worker 호출 결과를
// control_plane_analysis_worker_call_total{status}에 기록하는지 잠금.
func TestPostPythonAITaskRecordsMetrics(t *testing.T) {
	metrics.ResetForTest()
	t.Cleanup(metrics.ResetForTest)

	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer okServer.Close()
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer errServer.Close()

	okSvc := &DatasetService{pythonAIWorkerURL: okServer.URL}
	if _, err := okSvc.analyze().postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{}); err != nil {
		t.Fatalf("ok call: %v", err)
	}
	errSvc := &DatasetService{pythonAIWorkerURL: errServer.URL}
	if _, err := errSvc.analyze().postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{}); err == nil {
		t.Fatal("expected error from 500 worker")
	}

	out := metrics.Render()
	for _, want := range []string{
		`control_plane_analysis_worker_call_total{status="ok"} 1`,
		`control_plane_analysis_worker_call_total{status="error"} 1`,
		"control_plane_analysis_worker_call_duration_ms_count 2",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q:\n%s", want, out)
		}
	}
}

// silverone 2026-06-04 — postPythonAITask가 하드코딩 120s 대신 주입된
// pythonAITaskTimeout을 실제로 사용하는지 잠금. 회귀 시 분석 요청이 config와
// 무관하게 항상 120s로 도는 부채가 재발한다.
//
// postPythonAITask는 AnalyzeService로 이전됨(facade 분리, 2026-06-04). 테스트는
// DatasetService.SetPythonAITaskTimeout → s.analyze() → AnalyzeService.workerTimeout
// 전 경로가 살아 있는지 facade를 통해 검증한다.

func TestPostPythonAITaskUsesInjectedTimeout(t *testing.T) {
	// 서버는 200ms 지연 후 응답. timeout을 그보다 짧게 주면 client.Do가 실패해야 한다.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	s := &DatasetService{pythonAIWorkerURL: server.URL}
	s.SetPythonAITaskTimeout(20 * time.Millisecond)

	_, err := s.analyze().postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{})
	if err == nil {
		t.Fatalf("expected timeout error with 20ms timeout, got nil")
	}
	if !strings.Contains(err.Error(), "analyze worker call") {
		t.Fatalf("expected worker call error, got: %v", err)
	}
}

func TestPostPythonAITaskInjectedTimeoutAllowsFastResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	// 넉넉한 timeout이면 정상 응답을 받아야 한다.
	s := &DatasetService{pythonAIWorkerURL: server.URL}
	s.SetPythonAITaskTimeout(5 * time.Second)

	raw, err := s.analyze().postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !strings.Contains(string(raw), "ok") {
		t.Fatalf("unexpected body: %s", string(raw))
	}
}

// AnalyzeService가 DatasetService 없이도 worker 호출 경로를 독립 수행할 수 있는지
// (facade 분리의 decoupling) 잠금. versions resolver는 worker 호출에 필요 없으므로 nil.
func TestAnalyzeServiceStandaloneWorkerCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	a := NewAnalyzeService(nil, server.URL, 5*time.Second)
	raw, err := a.postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{})
	if err != nil {
		t.Fatalf("standalone AnalyzeService worker call failed: %v", err)
	}
	if !strings.Contains(string(raw), "ok") {
		t.Fatalf("unexpected body: %s", string(raw))
	}
}

// facade가 DatasetService의 설정값을 AnalyzeService로 전달하는지 잠금.
func TestDatasetServiceAnalyzeCarriesConfig(t *testing.T) {
	s := &DatasetService{pythonAIWorkerURL: "http://worker.example:9000"}
	s.SetPythonAITaskTimeout(7 * time.Second)
	a := s.analyze()
	if a.workerURL != "http://worker.example:9000" {
		t.Fatalf("workerURL not propagated: %q", a.workerURL)
	}
	if a.workerTimeout != 7*time.Second {
		t.Fatalf("workerTimeout not propagated: %v", a.workerTimeout)
	}
}

func TestSetPythonAITaskTimeoutIgnoresNonPositive(t *testing.T) {
	s := &DatasetService{}
	s.SetPythonAITaskTimeout(0)
	if s.pythonAITaskTimeout != 0 {
		t.Fatalf("expected 0 (unset) after non-positive input, got %v", s.pythonAITaskTimeout)
	}
	s.SetPythonAITaskTimeout(-5 * time.Second)
	if s.pythonAITaskTimeout != 0 {
		t.Fatalf("expected timeout to remain unset after negative input, got %v", s.pythonAITaskTimeout)
	}
	// 미설정이면 postPythonAITask가 defaultPythonAITaskTimeout(120s)로 fallback.
	if defaultPythonAITaskTimeout != 120*time.Second {
		t.Fatalf("default fallback drifted: %v", defaultPythonAITaskTimeout)
	}
}
