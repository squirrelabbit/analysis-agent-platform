package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// silverone 2026-06-04 — postPythonAITask가 하드코딩 120s 대신 주입된
// pythonAITaskTimeout을 실제로 사용하는지 잠금. 회귀 시 분석 요청이 config와
// 무관하게 항상 120s로 도는 부채가 재발한다.

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

	_, err := s.postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{})
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

	raw, err := s.postPythonAITask(context.Background(), "/tasks/analyze", map[string]any{})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !strings.Contains(string(raw), "ok") {
		t.Fatalf("unexpected body: %s", string(raw))
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
