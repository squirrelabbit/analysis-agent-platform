package store

import (
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// 인증/RBAC memory 구현 (ADR-025, silverone 2026-06-12). MemoryStore 맵은
// lazy-init한다(NewMemoryStore를 건드리지 않고 nil-safe).

func (s *MemoryStore) ensureAuthMaps() {
	if s.users == nil {
		s.users = map[string]domain.User{}
	}
	if s.sessions == nil {
		s.sessions = map[string]domain.Session{}
	}
	if s.projectMembers == nil {
		s.projectMembers = map[string]domain.ProjectMember{}
	}
}

func projectMemberKey(projectID, userID string) string {
	return projectID + "\x00" + userID
}

func (s *MemoryStore) UpsertUserByExternal(u domain.User) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureAuthMaps()
	now := time.Now().UTC()
	if u.AuthProvider == "" {
		u.AuthProvider = "google"
	}
	if u.GlobalRole == "" {
		u.GlobalRole = "user"
	}
	if u.Status == "" {
		u.Status = "active"
	}
	// (auth_provider, external_id)로 기존 user 탐색.
	for _, existing := range s.users {
		if existing.AuthProvider == u.AuthProvider && existing.ExternalID == u.ExternalID && u.ExternalID != "" {
			existing.Email = u.Email
			existing.Name = u.Name
			existing.AvatarURL = u.AvatarURL
			existing.UpdatedAt = now
			existing.LastLoginAt = &now
			s.users[existing.UserID] = existing
			return existing, nil
		}
	}
	u.CreatedAt = now
	u.UpdatedAt = now
	u.LastLoginAt = &now
	s.users[u.UserID] = u
	return u, nil
}

func (s *MemoryStore) GetUserByID(userID string) (domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[userID]
	if !ok {
		return domain.User{}, ErrNotFound
	}
	return u, nil
}

func (s *MemoryStore) CreateSession(sess domain.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureAuthMaps()
	now := sess.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	sess.CreatedAt = now
	if sess.LastSeenAt.IsZero() {
		sess.LastSeenAt = now
	}
	s.sessions[sess.SessionID] = sess
	return nil
}

func (s *MemoryStore) GetSessionByTokenHash(tokenHash string) (domain.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sess := range s.sessions {
		if sess.TokenHash == tokenHash {
			return sess, nil
		}
	}
	return domain.Session{}, ErrNotFound
}

func (s *MemoryStore) TouchSession(sessionID string, lastSeen time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return ErrNotFound
	}
	sess.LastSeenAt = lastSeen
	s.sessions[sessionID] = sess
	return nil
}

func (s *MemoryStore) DeleteSession(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
	return nil
}

func (s *MemoryStore) ListProjectRolesForUser(userID string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]string{}
	for _, m := range s.projectMembers {
		if m.UserID == userID {
			out[m.ProjectID] = m.Role
		}
	}
	return out, nil
}

func (s *MemoryStore) GetProjectRole(projectID, userID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.projectMembers[projectMemberKey(projectID, userID)]
	if !ok {
		return "", ErrNotFound
	}
	return m.Role, nil
}

func (s *MemoryStore) UpsertProjectMember(m domain.ProjectMember) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureAuthMaps()
	now := time.Now().UTC()
	key := projectMemberKey(m.ProjectID, m.UserID)
	if existing, ok := s.projectMembers[key]; ok {
		m.CreatedAt = existing.CreatedAt
	} else if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	m.UpdatedAt = now
	s.projectMembers[key] = m
	return nil
}

func (s *MemoryStore) DeleteProjectMember(projectID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := projectMemberKey(projectID, userID)
	if _, ok := s.projectMembers[key]; !ok {
		return ErrNotFound
	}
	delete(s.projectMembers, key)
	return nil
}

func (s *MemoryStore) ListProjectMembers(projectID string) ([]domain.ProjectMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.ProjectMember, 0)
	for _, m := range s.projectMembers {
		if m.ProjectID == projectID {
			items = append(items, m)
		}
	}
	return items, nil
}
