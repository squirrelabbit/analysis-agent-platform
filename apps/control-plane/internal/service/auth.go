package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

// 인증/RBAC 서비스 (ADR-025, silverone 2026-06-12).
// Google OIDC = 인증, project_members = 권한. 서버가 자체 세션 토큰(asp_session
// 쿠키 값)을 발급하고, 저장은 sha256 해시만(평문 미저장).

const googleAuthURL = "https://accounts.google.com/o/oauth2/v2/auth"

type AuthConfig struct {
	ClientID      string
	RedirectURL   string
	AllowedDomain string   // 회사 도메인(hd/email suffix). 비면 도메인 제한 없음
	AdminEmails   []string // 첫 로그인 시 global_role=admin
	SessionTTL    time.Duration
}

type AuthService struct {
	store       store.Repository
	authn       GoogleAuthenticator
	cfg         AuthConfig
	adminEmails map[string]bool
}

func NewAuthService(repo store.Repository, authn GoogleAuthenticator, cfg AuthConfig) *AuthService {
	admins := map[string]bool{}
	for _, e := range cfg.AdminEmails {
		if e = strings.TrimSpace(strings.ToLower(e)); e != "" {
			admins[e] = true
		}
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 168 * time.Hour
	}
	return &AuthService{store: repo, authn: authn, cfg: cfg, adminEmails: admins}
}

// StartURL — Google authorization 화면 redirect URL. state는 CSRF용(handler가 쿠키와 대조).
func (s *AuthService) StartURL(state string) string {
	q := url.Values{}
	q.Set("client_id", s.cfg.ClientID)
	q.Set("redirect_uri", s.cfg.RedirectURL)
	q.Set("response_type", "code")
	q.Set("scope", "openid email profile")
	q.Set("state", state)
	q.Set("access_type", "online")
	q.Set("prompt", "select_account")
	if s.cfg.AllowedDomain != "" {
		q.Set("hd", s.cfg.AllowedDomain)
	}
	return googleAuthURL + "?" + q.Encode()
}

// HandleCallback — code 교환→검증→도메인 확인→user upsert→세션 발급.
// 반환: 세션 토큰(쿠키 값) + 만료 시각.
func (s *AuthService) HandleCallback(ctx context.Context, code string) (string, time.Time, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return "", time.Time{}, ErrInvalidArgument{Message: "code is required"}
	}
	claims, err := s.authn.Exchange(ctx, code)
	if err != nil {
		return "", time.Time{}, err
	}
	if strings.TrimSpace(claims.Sub) == "" || !claims.EmailVerified {
		return "", time.Time{}, ErrUnauthorized{Message: "google 계정 검증 실패(email_verified)"}
	}
	email := strings.TrimSpace(strings.ToLower(claims.Email))
	if email == "" {
		return "", time.Time{}, ErrUnauthorized{Message: "google email 없음"}
	}
	if s.cfg.AllowedDomain != "" && !s.domainAllowed(claims) {
		return "", time.Time{}, ErrForbidden{Message: "허용되지 않은 도메인입니다"}
	}

	role := "user"
	if s.adminEmails[email] {
		role = "admin"
	}
	now := time.Now().UTC()
	user, err := s.store.UpsertUserByExternal(domain.User{
		UserID:       id.New(),
		Email:        email,
		Name:         strings.TrimSpace(claims.Name),
		AvatarURL:    strings.TrimSpace(claims.Picture),
		AuthProvider: "google",
		ExternalID:   claims.Sub,
		GlobalRole:   role,
		Status:       "active",
	})
	if err != nil {
		return "", time.Time{}, err
	}
	if user.Status != "active" {
		return "", time.Time{}, ErrForbidden{Message: "비활성 계정입니다"}
	}

	token, hash, err := newSessionToken()
	if err != nil {
		return "", time.Time{}, err
	}
	expires := now.Add(s.cfg.SessionTTL)
	if err := s.store.CreateSession(domain.Session{
		SessionID: id.New(),
		UserID:    user.UserID,
		TokenHash: hash,
		ExpiresAt: expires,
		CreatedAt: now,
	}); err != nil {
		return "", time.Time{}, err
	}
	return token, expires, nil
}

// Authenticate — 세션 토큰으로 현재 user 확인. 미들웨어가 매 요청 호출.
func (s *AuthService) Authenticate(ctx context.Context, token string) (domain.User, error) {
	_ = ctx
	token = strings.TrimSpace(token)
	if token == "" {
		return domain.User{}, ErrUnauthorized{Message: "세션 없음"}
	}
	sess, err := s.store.GetSessionByTokenHash(hashToken(token))
	if err != nil {
		return domain.User{}, ErrUnauthorized{Message: "유효하지 않은 세션"}
	}
	if time.Now().UTC().After(sess.ExpiresAt) {
		_ = s.store.DeleteSession(sess.SessionID)
		return domain.User{}, ErrUnauthorized{Message: "세션이 만료되었습니다"}
	}
	user, err := s.store.GetUserByID(sess.UserID)
	if err != nil {
		return domain.User{}, ErrUnauthorized{Message: "사용자 없음"}
	}
	if user.Status != "active" {
		return domain.User{}, ErrForbidden{Message: "비활성 계정입니다"}
	}
	_ = s.store.TouchSession(sess.SessionID, time.Now().UTC())
	return user, nil
}

// Logout — 세션 삭제(idempotent).
func (s *AuthService) Logout(ctx context.Context, token string) error {
	_ = ctx
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	sess, err := s.store.GetSessionByTokenHash(hashToken(token))
	if err != nil {
		return nil
	}
	return s.store.DeleteSession(sess.SessionID)
}

// Me — 현재 user + 프로젝트별 role.
func (s *AuthService) Me(ctx context.Context, user domain.User) (domain.AuthMeResponse, error) {
	_ = ctx
	roles, err := s.store.ListProjectRolesForUser(user.UserID)
	if err != nil {
		return domain.AuthMeResponse{}, err
	}
	if roles == nil {
		roles = map[string]string{}
	}
	return domain.AuthMeResponse{User: user, ProjectRoles: roles}, nil
}

// ProjectRole — 프로젝트 권한. admin은 전 프로젝트 owner로 간주. 권한 없으면 ""+false.
func (s *AuthService) ProjectRole(user domain.User, projectID string) (string, bool) {
	if user.GlobalRole == "admin" {
		return "owner", true
	}
	role, err := s.store.GetProjectRole(projectID, user.UserID)
	if err != nil || role == "" {
		return "", false
	}
	return role, true
}

// ── 프로젝트 멤버(RBAC) 관리 ──

var allowedProjectRoles = map[string]bool{"owner": true, "editor": true, "viewer": true}

func (s *AuthService) ListProjectMembers(projectID string) ([]domain.ProjectMember, error) {
	return s.store.ListProjectMembers(projectID)
}

func (s *AuthService) UpsertProjectMember(projectID, userID, role string) error {
	role = strings.TrimSpace(role)
	if !allowedProjectRoles[role] {
		return ErrInvalidArgument{Message: "role must be one of owner / editor / viewer"}
	}
	if strings.TrimSpace(userID) == "" {
		return ErrInvalidArgument{Message: "user_id is required"}
	}
	if _, err := s.store.GetUserByID(userID); err != nil {
		return ErrNotFound{Resource: "user"}
	}
	return s.store.UpsertProjectMember(domain.ProjectMember{
		ProjectID: projectID, UserID: userID, Role: role,
	})
}

func (s *AuthService) DeleteProjectMember(projectID, userID string) error {
	if err := s.store.DeleteProjectMember(projectID, strings.TrimSpace(userID)); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "project member"}
		}
		return err
	}
	return nil
}

func (s *AuthService) domainAllowed(claims domain.GoogleClaims) bool {
	d := strings.ToLower(s.cfg.AllowedDomain)
	if strings.EqualFold(claims.HostedDomain, d) {
		return true
	}
	at := strings.LastIndex(claims.Email, "@")
	if at < 0 {
		return false
	}
	return strings.EqualFold(claims.Email[at+1:], d)
}

// ── token helpers ──

// newSessionToken — 32바이트 난수 → base64url 토큰 + sha256 hex 해시(저장용).
func newSessionToken() (token, hash string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	token = base64.RawURLEncoding.EncodeToString(buf)
	return token, hashToken(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
