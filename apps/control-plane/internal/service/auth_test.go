package service

import (
	"context"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// fakeGoogle — Exchange를 고정 claims로 대체(실 Google 호출 없이 흐름 검증).
type fakeGoogle struct {
	claims domain.GoogleClaims
	err    error
}

func (f fakeGoogle) Exchange(ctx context.Context, code string) (domain.GoogleClaims, error) {
	if f.err != nil {
		return domain.GoogleClaims{}, f.err
	}
	return f.claims, nil
}

func newAuthSvc(t *testing.T, claims domain.GoogleClaims, cfg AuthConfig) (*AuthService, *store.MemoryStore) {
	t.Helper()
	repo := store.NewMemoryStore()
	return NewAuthService(repo, fakeGoogle{claims: claims}, cfg), repo
}

func TestAuthCallbackCreatesUserAndSession(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-1", Email: "a@corp.com", EmailVerified: true, Name: "A", HostedDomain: "corp.com"}
	svc, repo := newAuthSvc(t, claims, AuthConfig{AllowedDomain: "corp.com", AdminEmails: []string{"admin@corp.com"}, SessionTTL: time.Hour})

	token, exp, err := svc.HandleCallback(context.Background(), "code-1")
	if err != nil {
		t.Fatalf("HandleCallback: %v", err)
	}
	if token == "" || !exp.After(time.Now()) {
		t.Fatalf("token/exp invalid: %q %v", token, exp)
	}
	// 토큰으로 인증되고 동일 user가 나와야.
	user, err := svc.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if user.Email != "a@corp.com" || user.ExternalID != "g-1" || user.GlobalRole != "user" {
		t.Fatalf("user unexpected: %+v", user)
	}
	// 평문 토큰은 저장 안 되고 해시만(세션은 해시로 조회).
	if _, err := repo.GetSessionByTokenHash(hashToken(token)); err != nil {
		t.Fatalf("session by hash: %v", err)
	}

	// logout → 인증 실패.
	if err := svc.Logout(context.Background(), token); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, err := svc.Authenticate(context.Background(), token); err == nil {
		t.Fatal("logout 후 인증은 실패해야")
	}
}

func TestAuthAdminEmailGetsAdminRole(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-2", Email: "admin@corp.com", EmailVerified: true, HostedDomain: "corp.com"}
	svc, _ := newAuthSvc(t, claims, AuthConfig{AllowedDomain: "corp.com", AdminEmails: []string{"admin@corp.com"}, SessionTTL: time.Hour})
	token, _, err := svc.HandleCallback(context.Background(), "c")
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	user, _ := svc.Authenticate(context.Background(), token)
	if user.GlobalRole != "admin" {
		t.Fatalf("admin email이면 global_role=admin 기대, got %q", user.GlobalRole)
	}
}

func TestAuthDomainNotAllowedForbidden(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-3", Email: "x@other.com", EmailVerified: true, HostedDomain: "other.com"}
	svc, _ := newAuthSvc(t, claims, AuthConfig{AllowedDomain: "corp.com", SessionTTL: time.Hour})
	if _, _, err := svc.HandleCallback(context.Background(), "c"); err == nil {
		t.Fatal("허용 도메인 밖이면 403")
	} else if _, ok := err.(ErrForbidden); !ok {
		t.Fatalf("want ErrForbidden, got %v", err)
	}
}

func TestAuthEmailNotVerifiedRejected(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-4", Email: "a@corp.com", EmailVerified: false}
	svc, _ := newAuthSvc(t, claims, AuthConfig{SessionTTL: time.Hour})
	if _, _, err := svc.HandleCallback(context.Background(), "c"); err == nil {
		t.Fatal("email_verified=false면 401")
	} else if _, ok := err.(ErrUnauthorized); !ok {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

func TestAuthExpiredSessionRejected(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-5", Email: "a@corp.com", EmailVerified: true}
	svc, repo := newAuthSvc(t, claims, AuthConfig{SessionTTL: time.Hour})
	token, _, err := svc.HandleCallback(context.Background(), "c")
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	// 세션 만료시켜 저장.
	sess, _ := repo.GetSessionByTokenHash(hashToken(token))
	sess.ExpiresAt = time.Now().Add(-time.Minute)
	_ = repo.CreateSession(sess)
	if _, err := svc.Authenticate(context.Background(), token); err == nil {
		t.Fatal("만료 세션은 401")
	} else if _, ok := err.(ErrUnauthorized); !ok {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

func TestProjectRoleAndMembers(t *testing.T) {
	claims := domain.GoogleClaims{Sub: "g-6", Email: "a@corp.com", EmailVerified: true}
	svc, _ := newAuthSvc(t, claims, AuthConfig{SessionTTL: time.Hour})
	token, _, _ := svc.HandleCallback(context.Background(), "c")
	user, _ := svc.Authenticate(context.Background(), token)

	// 멤버 없으면 권한 없음.
	if _, has := svc.ProjectRole(user, "p1"); has {
		t.Fatal("멤버 아니면 권한 없어야")
	}
	// editor 부여 → 권한.
	if err := svc.UpsertProjectMember("p1", user.UserID, "editor"); err != nil {
		t.Fatalf("UpsertProjectMember: %v", err)
	}
	if role, has := svc.ProjectRole(user, "p1"); !has || role != "editor" {
		t.Fatalf("editor 기대, got %q/%v", role, has)
	}
	// 잘못된 role → 400.
	if err := svc.UpsertProjectMember("p1", user.UserID, "boss"); err == nil {
		t.Fatal("잘못된 role은 400")
	}
	// admin은 전 프로젝트 owner.
	admin := domain.User{UserID: "u-admin", GlobalRole: "admin", Status: "active"}
	if role, has := svc.ProjectRole(admin, "any"); !has || role != "owner" {
		t.Fatalf("admin은 owner 기대, got %q/%v", role, has)
	}
}
