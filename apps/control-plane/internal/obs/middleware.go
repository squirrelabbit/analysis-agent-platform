package obs

import (
	"crypto/rand"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const requestIDHeader = "X-Request-ID"

// responseRecorder wraps http.ResponseWriter to capture status code and bytes written.
type responseRecorder struct {
	http.ResponseWriter
	status       int
	bytesWritten int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += n
	return n, err
}

// Middleware reads X-Request-ID from the incoming request (or generates a ULID),
// stores it in the request context, echoes it in the response header, and logs
// http.request.started / http.request.completed events.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get(requestIDHeader))
		if id == "" {
			id = newULID()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := WithRequestID(r.Context(), id)

		remoteIP := r.RemoteAddr
		if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
			remoteIP = strings.SplitN(fwd, ",", 2)[0]
		}
		l := FromContext(ctx)
		l.Info("http request started",
			"event", "http.request.started",
			"method", r.Method,
			"path", r.URL.Path,
			"remote_ip", remoteIP,
		)

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		startedAt := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))

		l.Info("http request completed",
			"event", "http.request.completed",
			"status", rec.status,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"response_size", rec.bytesWritten,
		)
	})
}

func newULID() string {
	entropy := ulid.Monotonic(rand.Reader, 0)
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}
