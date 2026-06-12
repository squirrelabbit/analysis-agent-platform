package store

import (
	"database/sql"
	"errors"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// 인증/RBAC postgres 구현 (ADR-025, silverone 2026-06-12).

func (s *PostgresStore) UpsertUserByExternal(u domain.User) (domain.User, error) {
	now := time.Now().UTC()
	provider := u.AuthProvider
	if provider == "" {
		provider = "google"
	}
	role := u.GlobalRole
	if role == "" {
		role = "user"
	}
	status := u.Status
	if status == "" {
		status = "active"
	}
	var userID string
	err := s.db.QueryRow(
		`INSERT INTO users (user_id, email, name, avatar_url, auth_provider, external_id,
		                    global_role, status, created_at, updated_at, last_login_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $9)
		 ON CONFLICT (auth_provider, external_id) DO UPDATE
		 SET email = EXCLUDED.email,
		     name = EXCLUDED.name,
		     avatar_url = EXCLUDED.avatar_url,
		     updated_at = EXCLUDED.updated_at,
		     last_login_at = EXCLUDED.last_login_at
		 RETURNING user_id`,
		u.UserID, u.Email, nullableEmptyString(u.Name), nullableEmptyString(u.AvatarURL),
		provider, nullableEmptyString(u.ExternalID), role, status, now,
	).Scan(&userID)
	if err != nil {
		return domain.User{}, err
	}
	return s.GetUserByID(userID)
}

func scanUser(row interface{ Scan(...any) error }) (domain.User, error) {
	var u domain.User
	var name, avatar, external sql.NullString
	var lastLogin sql.NullTime
	if err := row.Scan(
		&u.UserID, &u.Email, &name, &avatar, &u.AuthProvider, &external,
		&u.GlobalRole, &u.Status, &u.CreatedAt, &u.UpdatedAt, &lastLogin,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.User{}, ErrNotFound
		}
		return domain.User{}, err
	}
	u.Name = name.String
	u.AvatarURL = avatar.String
	u.ExternalID = external.String
	if lastLogin.Valid {
		t := lastLogin.Time
		u.LastLoginAt = &t
	}
	return u, nil
}

const userSelectCols = `user_id, email, name, avatar_url, auth_provider, external_id,
	global_role, status, created_at, updated_at, last_login_at`

func (s *PostgresStore) GetUserByID(userID string) (domain.User, error) {
	return scanUser(s.db.QueryRow(
		`SELECT `+userSelectCols+` FROM users WHERE user_id = $1`, userID))
}

func (s *PostgresStore) CreateSession(sess domain.Session) error {
	now := sess.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.Exec(
		`INSERT INTO sessions (session_id, user_id, token_hash, expires_at, created_at, last_seen_at)
		 VALUES ($1, $2, $3, $4, $5, $5)`,
		sess.SessionID, sess.UserID, sess.TokenHash, sess.ExpiresAt, now,
	)
	return err
}

func (s *PostgresStore) GetSessionByTokenHash(tokenHash string) (domain.Session, error) {
	var sess domain.Session
	err := s.db.QueryRow(
		`SELECT session_id, user_id, token_hash, expires_at, created_at, last_seen_at
		 FROM sessions WHERE token_hash = $1`, tokenHash,
	).Scan(&sess.SessionID, &sess.UserID, &sess.TokenHash, &sess.ExpiresAt, &sess.CreatedAt, &sess.LastSeenAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Session{}, ErrNotFound
		}
		return domain.Session{}, err
	}
	return sess, nil
}

func (s *PostgresStore) TouchSession(sessionID string, lastSeen time.Time) error {
	_, err := s.db.Exec(`UPDATE sessions SET last_seen_at = $2 WHERE session_id = $1`, sessionID, lastSeen)
	return err
}

func (s *PostgresStore) DeleteSession(sessionID string) error {
	_, err := s.db.Exec(`DELETE FROM sessions WHERE session_id = $1`, sessionID)
	return err
}

func (s *PostgresStore) ListProjectRolesForUser(userID string) (map[string]string, error) {
	rows, err := s.db.Query(
		`SELECT project_id::text, role FROM project_members WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var pid, role string
		if err := rows.Scan(&pid, &role); err != nil {
			return nil, err
		}
		out[pid] = role
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetProjectRole(projectID, userID string) (string, error) {
	var role string
	err := s.db.QueryRow(
		`SELECT role FROM project_members WHERE project_id = $1::uuid AND user_id = $2`,
		projectID, userID,
	).Scan(&role)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return role, nil
}

func (s *PostgresStore) UpsertProjectMember(m domain.ProjectMember) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(
		`INSERT INTO project_members (project_id, user_id, role, created_at, updated_at)
		 VALUES ($1::uuid, $2, $3, $4, $4)
		 ON CONFLICT (project_id, user_id) DO UPDATE
		 SET role = EXCLUDED.role, updated_at = EXCLUDED.updated_at`,
		m.ProjectID, m.UserID, m.Role, now,
	)
	return err
}

func (s *PostgresStore) DeleteProjectMember(projectID, userID string) error {
	res, err := s.db.Exec(
		`DELETE FROM project_members WHERE project_id = $1::uuid AND user_id = $2`,
		projectID, userID,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) ListProjectMembers(projectID string) ([]domain.ProjectMember, error) {
	rows, err := s.db.Query(
		`SELECT project_id::text, user_id, role, created_at, updated_at
		 FROM project_members WHERE project_id = $1::uuid ORDER BY created_at`,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.ProjectMember, 0)
	for rows.Next() {
		var m domain.ProjectMember
		if err := rows.Scan(&m.ProjectID, &m.UserID, &m.Role, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	return items, rows.Err()
}
