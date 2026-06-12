package http

import (
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"analysis-support-platform/control-plane/internal/config"
)

// Google OAuth 미설정(client_id 없음)이면 start는 깨진 Google 페이지가 아니라
// 로그인 화면(/login?error=config)으로 돌려보내야 한다.
func TestGoogleStartRedirectsToLoginWhenUnconfigured(t *testing.T) {
	s := &Server{cfg: config.Config{}} // AuthGoogleClientID 빈 값
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(stdhttp.MethodGet, "/auth/google/start", nil)

	s.handleAuthGoogleStart(rec, req)

	if rec.Code != stdhttp.StatusFound {
		t.Fatalf("status: got %d want %d", rec.Code, stdhttp.StatusFound)
	}
	if loc := rec.Header().Get("Location"); loc != "/login?error=config" {
		t.Fatalf("location: got %q want %q", loc, "/login?error=config")
	}
}

// callback의 state 불일치(CSRF/만료)는 JSON 에러가 아니라 로그인 화면으로 리다이렉트.
func TestGoogleCallbackStateMismatchRedirectsToLogin(t *testing.T) {
	s := &Server{cfg: config.Config{}}
	rec := httptest.NewRecorder()
	// state 쿠키 없이 콜백 → 불일치.
	req := httptest.NewRequest(stdhttp.MethodGet, "/auth/google/callback?code=c&state=x", nil)

	s.handleAuthGoogleCallback(rec, req)

	if rec.Code != stdhttp.StatusFound {
		t.Fatalf("status: got %d want %d", rec.Code, stdhttp.StatusFound)
	}
	if loc := rec.Header().Get("Location"); loc != "/login?error=session" {
		t.Fatalf("location: got %q want %q", loc, "/login?error=session")
	}
}

func TestProjectScopeRequirement(t *testing.T) {
	cases := []struct {
		name, method, path string
		wantPID, wantRole  string
		wantApplies        bool
	}{
		{"list (no pid)", "GET", "/projects", "", "", false},
		{"create (no pid)", "POST", "/projects", "", "", false},
		{"non-project path", "GET", "/auth/me", "", "", false},
		{"get project → viewer", "GET", "/projects/p1", "p1", "viewer", true},
		{"delete project → owner", "DELETE", "/projects/p1", "p1", "owner", true},
		{"patch project → editor", "PATCH", "/projects/p1", "p1", "editor", true},
		{"members list → owner", "GET", "/projects/p1/members", "p1", "owner", true},
		{"members put → owner", "PUT", "/projects/p1/members/u1", "p1", "owner", true},
		{"sub read → viewer", "GET", "/projects/p1/datasets", "p1", "viewer", true},
		{"sub write → editor", "POST", "/projects/p1/datasets/d1/versions/v1/analyze", "p1", "editor", true},
		{"sub delete → editor", "DELETE", "/projects/p1/datasets/d1", "p1", "editor", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pid, role, applies := projectScopeRequirement(c.method, c.path)
			if applies != c.wantApplies || pid != c.wantPID || role != c.wantRole {
				t.Fatalf("got (%q,%q,%v) want (%q,%q,%v)", pid, role, applies, c.wantPID, c.wantRole, c.wantApplies)
			}
		})
	}
}
