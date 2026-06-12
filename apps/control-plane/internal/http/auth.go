package http

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	stdhttp "net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/service"
)

// 인증/RBAC HTTP 계층 (ADR-025, silverone 2026-06-12).

const (
	sessionCookieName = "asp_session"
	stateCookieName   = "asp_oauth_state"
	// loginPath — 인증 실패를 되돌릴 프론트 로그인 화면 경로(로그인.html).
	loginPath = "/login"
)

// loginErrorRedirect — 인증 실패를 로그인 화면의 에러 배너로 되돌린다.
// 로그인 화면 error 코드: not_allowed(도메인 밖) / wrong_account(미인증 계정) /
// session(state·일반 실패) / config(OAuth 미설정).
func (s *Server) loginErrorRedirect(w stdhttp.ResponseWriter, r *stdhttp.Request, err error) {
	code := "session"
	var unauthorized service.ErrUnauthorized
	var forbidden service.ErrForbidden
	switch {
	case errors.As(err, &forbidden):
		code = "not_allowed"
	case errors.As(err, &unauthorized):
		code = "wrong_account"
	}
	stdhttp.Redirect(w, r, loginPath+"?error="+code, stdhttp.StatusFound)
}

type ctxKey string

const userCtxKey ctxKey = "auth_user"

// userFromContext — 미들웨어가 주입한 인증 user. ok=false면 미인증.
func userFromContext(ctx context.Context) (domain.User, bool) {
	u, ok := ctx.Value(userCtxKey).(domain.User)
	return u, ok
}

// authMiddleware — AuthEnabled일 때만 동작(기본 off = 7/30 사내망 격리 단계 호환).
// public path 외에는 asp_session 쿠키로 인증을 요구하고 user를 context에 주입한다.
func (s *Server) authMiddleware(next stdhttp.Handler) stdhttp.Handler {
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if !s.cfg.AuthEnabled || r.Method == stdhttp.MethodOptions || isPublicAuthPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || strings.TrimSpace(cookie.Value) == "" {
			writeError(w, stdhttp.StatusUnauthorized, "로그인이 필요합니다")
			return
		}
		user, err := s.authService.Authenticate(r.Context(), cookie.Value)
		if err != nil {
			s.writeServiceError(w, err)
			return
		}
		// 프로젝트 스코프 RBAC: /projects/{pid}/... 는 최소 role을 강제한다.
		if pid, required, applies := projectScopeRequirement(r.Method, r.URL.Path); applies {
			if !s.authService.HasProjectRole(user, pid, required) {
				writeError(w, stdhttp.StatusForbidden, "프로젝트 접근 권한이 없습니다")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userCtxKey, user)))
	})
}

// projectScopeRequirement — path/method 기반 최소 프로젝트 role.
//   - /projects (목록) · POST /projects (생성): pid 없음 → applies=false (인증만)
//   - /projects/{pid}/members*       → owner (멤버 관리)
//   - DELETE /projects/{pid}         → owner (프로젝트 삭제)
//   - GET  /projects/{pid}[/...]     → viewer (읽기)
//   - 그 외 mutating (POST/PUT/PATCH/DELETE) → editor (실행/수정)
func projectScopeRequirement(method, path string) (projectID, required string, applies bool) {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	if len(segs) < 2 || segs[0] != "projects" || segs[1] == "" {
		return "", "", false
	}
	pid := segs[1]
	if len(segs) >= 3 && segs[2] == "members" {
		return pid, "owner", true
	}
	if len(segs) == 2 { // /projects/{pid}
		switch method {
		case stdhttp.MethodGet:
			return pid, "viewer", true
		case stdhttp.MethodDelete:
			return pid, "owner", true
		default:
			return pid, "editor", true
		}
	}
	if method == stdhttp.MethodGet {
		return pid, "viewer", true
	}
	return pid, "editor", true
}

// handleAuthConfig — 프론트 부팅용 public config. auth_enabled로 가드 on/off 판단.
func (s *Server) handleAuthConfig(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	writeJSON(w, stdhttp.StatusOK, map[string]any{
		"auth_enabled": s.cfg.AuthEnabled,
		"provider":     "google",
	})
}

// isPublicAuthPath — 세션 없이 접근 가능한 경로. 로그인 진입/문서/헬스.
func isPublicAuthPath(path string) bool {
	switch path {
	case "/health", "/runtime_status", "/metrics",
		"/auth/config", "/auth/google/start", "/auth/google/callback",
		"/openapi.yaml", "/openapi.frontend.yaml":
		return true
	}
	return strings.HasPrefix(path, "/swagger")
}

func (s *Server) handleAuthGoogleStart(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	// Google OAuth 미설정(client_id 없음): 깨진 Google invalid_request 페이지 대신
	// 로그인 화면에 설정 안내를 띄운다.
	if s.cfg.AuthGoogleClientID == "" {
		stdhttp.Redirect(w, r, loginPath+"?error=config", stdhttp.StatusFound)
		return
	}
	state, err := randomState()
	if err != nil {
		writeError(w, stdhttp.StatusInternalServerError, err.Error())
		return
	}
	// CSRF: state를 쿠키에도 심어 callback에서 query state와 대조.
	stdhttp.SetCookie(w, &stdhttp.Cookie{
		Name:     stateCookieName,
		Value:    state,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cfg.AuthCookieSecure,
		SameSite: stdhttp.SameSiteLaxMode,
		MaxAge:   600,
	})
	stdhttp.Redirect(w, r, s.authService.StartURL(state), stdhttp.StatusFound)
}

func (s *Server) handleAuthGoogleCallback(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	// state 대조 (CSRF).
	stateCookie, err := r.Cookie(stateCookieName)
	queryState := r.URL.Query().Get("state")
	if err != nil || stateCookie.Value == "" || queryState == "" || stateCookie.Value != queryState {
		// state 불일치(CSRF/만료) → 로그인 화면으로.
		s.loginErrorRedirect(w, r, nil)
		return
	}
	clearCookie(w, stateCookieName, s.cfg.AuthCookieSecure)

	token, expires, err := s.authService.HandleCallback(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		// 도메인 밖/미인증 계정 등 → 로그인 화면 에러 배너로.
		s.loginErrorRedirect(w, r, err)
		return
	}
	stdhttp.SetCookie(w, &stdhttp.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cfg.AuthCookieSecure,
		SameSite: stdhttp.SameSiteLaxMode,
		Expires:  expires,
	})
	dest := s.cfg.AuthPostLoginURL
	if dest == "" {
		dest = "/"
	}
	stdhttp.Redirect(w, r, dest, stdhttp.StatusFound)
}

func (s *Server) handleAuthMe(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	user, ok := userFromContext(r.Context())
	if !ok {
		// AuthEnabled=false면 미들웨어가 주입을 안 하므로 쿠키로 직접 시도.
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil {
			writeError(w, stdhttp.StatusUnauthorized, "로그인이 필요합니다")
			return
		}
		user, err = s.authService.Authenticate(r.Context(), cookie.Value)
		if err != nil {
			s.writeServiceError(w, err)
			return
		}
	}
	resp, err := s.authService.Me(r.Context(), user)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, resp)
}

func (s *Server) handleAuthLogout(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		_ = s.authService.Logout(r.Context(), cookie.Value)
	}
	clearCookie(w, sessionCookieName, s.cfg.AuthCookieSecure)
	w.WriteHeader(stdhttp.StatusNoContent)
}

// ── 프로젝트 멤버(RBAC) 관리 — admin 또는 해당 프로젝트 owner ──

func (s *Server) handleListProjectMembers(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if !s.requireProjectManage(w, r) {
		return
	}
	members, err := s.authService.ListProjectMembers(r.PathValue("project_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, map[string]any{"items": members})
}

func (s *Server) handleUpsertProjectMember(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if !s.requireProjectManage(w, r) {
		return
	}
	var payload struct {
		Role string `json:"role"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	if err := s.authService.UpsertProjectMember(
		r.PathValue("project_id"), r.PathValue("user_id"), payload.Role,
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) handleDeleteProjectMember(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if !s.requireProjectManage(w, r) {
		return
	}
	if err := s.authService.DeleteProjectMember(
		r.PathValue("project_id"), r.PathValue("user_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

// requireProjectManage — AuthEnabled일 때 admin 또는 프로젝트 owner만 멤버 관리 허용.
// AuthEnabled=false면 통과(현 단계 호환).
func (s *Server) requireProjectManage(w stdhttp.ResponseWriter, r *stdhttp.Request) bool {
	if !s.cfg.AuthEnabled {
		return true
	}
	user, ok := userFromContext(r.Context())
	if !ok {
		writeError(w, stdhttp.StatusUnauthorized, "로그인이 필요합니다")
		return false
	}
	role, has := s.authService.ProjectRole(user, r.PathValue("project_id"))
	if !has || (role != "owner" && user.GlobalRole != "admin") {
		writeError(w, stdhttp.StatusForbidden, "프로젝트 관리 권한이 없습니다")
		return false
	}
	return true
}

func clearCookie(w stdhttp.ResponseWriter, name string, secure bool) {
	stdhttp.SetCookie(w, &stdhttp.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: stdhttp.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func randomState() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
