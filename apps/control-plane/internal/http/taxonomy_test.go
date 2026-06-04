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

// fake python worker. /tasks/taxonomy에서 taxonomy_id를 보고 200 또는 400을 돌려준다.
// 빈 taxonomy_id는 worker가 default(festival-v2)로 처리하는 동작을 흉내낸다.
func newFakeTaxonomyWorker(t *testing.T, called *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called++
		if r.URL.Path != "/tasks/taxonomy" {
			t.Errorf("unexpected worker path: %s", r.URL.Path)
		}
		var payload struct {
			TaxonomyID string `json:"taxonomy_id"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		id := payload.TaxonomyID
		if id == "" {
			id = "festival-v2"
		}
		w.Header().Set("Content-Type", "application/json")
		switch id {
		case "festival-v2":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"taxonomy_id":"festival-v2","domain":"festival","aspects":[{"key":"food","label":"음식/음료","description":"맛"}],"sentiments":["positive","negative","neutral"],"fallback_aspect":"etc","taxonomy_hash":"abc"}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"taxonomy config not found: ` + id + `"}`))
		}
	}))
}

func newTaxonomyServer(workerURL string) *Server {
	return NewServer(config.Config{
		BindAddr:          ":0",
		StoreBackend:      "memory",
		WorkflowEngine:    "noop",
		PythonAIWorkerURL: workerURL,
	})
}

func TestTaxonomy_DefaultProxiesWorkerJSON(t *testing.T) {
	called := 0
	worker := newFakeTaxonomyWorker(t, &called)
	defer worker.Close()
	handler := newTaxonomyServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/taxonomy", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var decoded struct {
		TaxonomyID string `json:"taxonomy_id"`
		Domain     string `json:"domain"`
		Aspects    []struct {
			Key   string `json:"key"`
			Label string `json:"label"`
		} `json:"aspects"`
		FallbackAspect string `json:"fallback_aspect"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if decoded.TaxonomyID != "festival-v2" || len(decoded.Aspects) != 1 || decoded.Aspects[0].Label != "음식/음료" {
		t.Fatalf("unexpected proxied body: %+v", decoded)
	}
	if called != 1 {
		t.Errorf("worker should be called once, called=%d", called)
	}
}

func TestTaxonomy_ExplicitIDPassedThrough(t *testing.T) {
	called := 0
	worker := newFakeTaxonomyWorker(t, &called)
	defer worker.Close()
	handler := newTaxonomyServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/taxonomy?taxonomy_id=festival-v2", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestTaxonomy_UnknownIDReturns400(t *testing.T) {
	called := 0
	worker := newFakeTaxonomyWorker(t, &called)
	defer worker.Close()
	handler := newTaxonomyServer(worker.URL).Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/taxonomy?taxonomy_id=nope", nil))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d (%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "taxonomy config not found") {
		t.Errorf("expected worker error surfaced, got: %s", rec.Body.String())
	}
}
