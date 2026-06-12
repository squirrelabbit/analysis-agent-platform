package domain

import "time"

// 인증/RBAC 도메인 (ADR-025, silverone 2026-06-12).
// Google OIDC = 인증, project_members = 권한. Google token은 프론트가 들지 않고
// 우리 서버가 자체 세션 쿠키(asp_session)를 발급한다.

// User — Google OIDC로 인증된 사용자. ExternalID는 Google sub(stable id).
// GlobalRole은 시스템 전역 역할(admin/user). 프로젝트 권한은 ProjectMember가 별도.
type User struct {
	UserID       string     `json:"user_id"`
	Email        string     `json:"email"`
	Name         string     `json:"name,omitempty"`
	AvatarURL    string     `json:"avatar_url,omitempty"`
	AuthProvider string     `json:"auth_provider"` // "google"
	ExternalID   string     `json:"external_id"`   // Google sub
	GlobalRole   string     `json:"global_role"`   // admin | user
	Status       string     `json:"status"`        // active | disabled
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	LastLoginAt  *time.Time `json:"last_login_at,omitempty"`
}

// Session — 서버 세션. TokenHash만 저장(원문 토큰은 쿠키에만, 평문 미저장).
type Session struct {
	SessionID  string    `json:"session_id"`
	UserID     string    `json:"user_id"`
	TokenHash  string    `json:"-"`
	ExpiresAt  time.Time `json:"expires_at"`
	CreatedAt  time.Time `json:"created_at"`
	LastSeenAt time.Time `json:"last_seen_at"`
}

// ProjectMember — 프로젝트 단위 권한(RBAC). role: owner | editor | viewer.
type ProjectMember struct {
	ProjectID string    `json:"project_id"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GoogleClaims — id_token 검증 후 우리가 신뢰하는 최소 claim 집합.
type GoogleClaims struct {
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
	HostedDomain  string `json:"hd"`
}

// AuthMeResponse — GET /auth/me. 현재 user + 프로젝트별 role.
type AuthMeResponse struct {
	User         User             `json:"user"`
	ProjectRoles map[string]string `json:"project_roles"` // project_id → role
}
