package http

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/config"
)

// fake python worker. /tasks/prompt_options에서 task를 보고 200 또는 400을 돌려준다.
func newFakePromptWorker(t *testing.T, called *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called++
		if r.URL.Path != "/tasks/prompt_options" {
			t.Errorf("unexpected worker path: %s", r.URL.Path)
		}
		var payload struct {
			Task string `json:"task"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		w.Header().Set("Content-Type", "application/json")
		switch payload.Task {
		case "doc_genuineness":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"task":"doc_genuineness","default":"v1","versions":[{"version":"v1","label":"v1"}]}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unknown prompt task: ` + payload.Task + `"}`))
		}
	}))
}

func newPromptOptionsServer(workerURL string) *Server {
	return NewServer(config.Config{
		BindAddr:          ":0",
		StoreBackend:      "memory",
		WorkflowEngine:    "noop",
		PythonAIWorkerURL: workerURL,
	})
}

func TestPromptOptions_ValidTaskProxiesWorkerJSON(t *testing.T) {
	called := 0
	worker := newFakePromptWorker(t, &called)
	defer worker.Close()
	handler := newPromptOptionsServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/prompt_options?task=doc_genuineness", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var decoded struct {
		Task     string `json:"task"`
		Default  string `json:"default"`
		Versions []struct {
			Version string `json:"version"`
			Label   string `json:"label"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if decoded.Task != "doc_genuineness" || decoded.Default != "v1" || len(decoded.Versions) != 1 {
		t.Fatalf("unexpected proxied body: %+v", decoded)
	}
	// 응답에 prompt 본문/파일 경로가 새지 않아야 한다.
	if strings.Contains(rec.Body.String(), ".md") {
		t.Errorf("response must not leak file paths: %s", rec.Body.String())
	}
}

func TestPromptOptions_InvalidTaskReturns400(t *testing.T) {
	called := 0
	worker := newFakePromptWorker(t, &called)
	defer worker.Close()
	handler := newPromptOptionsServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/prompt_options?task=nope", nil))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d (%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unknown prompt task") {
		t.Errorf("expected worker error surfaced, got: %s", rec.Body.String())
	}
}

func TestPromptOptions_MissingTaskReturns400WithoutWorkerCall(t *testing.T) {
	called := 0
	worker := newFakePromptWorker(t, &called)
	defer worker.Close()
	handler := newPromptOptionsServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/prompt_options", nil))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d (%s)", rec.Code, rec.Body.String())
	}
	if called != 0 {
		t.Errorf("worker must not be called when task is missing, called=%d", called)
	}
}
