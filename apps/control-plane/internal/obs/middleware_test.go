package obs_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"analysis-support-platform/control-plane/internal/obs"
)

func TestMiddleware_generatesRequestIDWhenAbsent(t *testing.T) {
	obs.Init("test-svc")
	handler := obs.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if id := rec.Header().Get("X-Request-ID"); id == "" {
		t.Error("X-Request-ID response header must be set when absent from request")
	}
}

func TestMiddleware_respectsIncomingRequestID(t *testing.T) {
	obs.Init("test-svc")
	const want = "my-custom-request-id"
	handler := obs.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", want)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != want {
		t.Errorf("expected X-Request-ID=%q, got %q", want, got)
	}
}

func TestMiddleware_contextLoggerDoesNotPanic(t *testing.T) {
	obs.Init("test-svc")
	handler := obs.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// FromContext must return a usable logger enriched with request_id
		obs.FromContext(r.Context()).Info("handler reached", "event", "test.request")
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-ID", "test-id-123")
	handler.ServeHTTP(httptest.NewRecorder(), req)
}

func TestMiddleware_generatedIDsAreUnique(t *testing.T) {
	obs.Init("test-svc")
	seen := make(map[string]struct{})
	handler := obs.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	for i := range 10 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		id := rec.Header().Get("X-Request-ID")
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate request ID on iteration %d: %q", i, id)
		}
		seen[id] = struct{}{}
	}
}
