package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db *sql.DB
}

type timestampColumn struct {
	tableName  string
	columnName string
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	if databaseURL == "" {
		return nil, errors.New("DATABASE_URL is required when STORE_BACKEND=postgres")
	}

	db, err := sql.Open("pgx", normalizeDatabaseURL(databaseURL))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	store := &PostgresStore{db: db}
	if err := store.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) SaveProject(project domain.Project) error {
	_, err := s.db.Exec(
		`INSERT INTO projects (project_id, name, description, created_at)
		 VALUES ($1::uuid, $2, $3, $4)
		 ON CONFLICT (project_id) DO UPDATE
		 SET name = EXCLUDED.name,
		     description = EXCLUDED.description,
		     created_at = EXCLUDED.created_at`,
		project.ProjectID,
		project.Name,
		nullableString(project.Description),
		project.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetProject(projectID string) (domain.Project, error) {
	row := s.db.QueryRow(
		`SELECT project_id::text, name, description, created_at
		 FROM projects
		 WHERE project_id = $1::uuid`,
		projectID,
	)

	var project domain.Project
	var description sql.NullString
	if err := row.Scan(&project.ProjectID, &project.Name, &description, &project.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Project{}, ErrNotFound
		}
		return domain.Project{}, err
	}
	if description.Valid {
		project.Description = &description.String
	}
	return project, nil
}

func (s *PostgresStore) ListProjects() ([]domain.Project, error) {
	rows, err := s.db.Query(
		`SELECT project_id::text, name, description, created_at
		 FROM projects
		 ORDER BY created_at ASC, project_id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Project, 0)
	for rows.Next() {
		var project domain.Project
		var description sql.NullString
		if err := rows.Scan(&project.ProjectID, &project.Name, &description, &project.CreatedAt); err != nil {
			return nil, err
		}
		if description.Valid {
			project.Description = &description.String
		}
		items = append(items, project)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) DeleteProject(projectID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = deleteEmbeddingIndexChunksForProject(tx, projectID); err != nil {
		return err
	}
	// 옛 schema (report_drafts / executions / skill_plans / analysis_requests /
	// scenarios)는 5/26 DROP TABLE로 제거 — tableExists 가드 loop도 함께 정리.
	statements := []struct {
		query string
		args  []any
	}{
		{query: `DELETE FROM analysis_messages WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM analysis_runs WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM analysis_threads WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM dataset_build_jobs WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM dataset_versions WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM datasets WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM project_prompt_defaults WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM project_prompts WHERE project_id = $1::uuid`, args: []any{projectID}},
	}
	for _, statement := range statements {
		if _, err = tx.Exec(statement.query, statement.args...); err != nil {
			return err
		}
	}

	result, err := tx.Exec(`DELETE FROM projects WHERE project_id = $1::uuid`, projectID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		err = ErrNotFound
		return err
	}
	err = tx.Commit()
	return err
}

// 5/6 화면기획서 B안 채택: 전역 prompts 테이블 폐기. SavePrompt/GetPrompt/
// GetPromptByVersion/ListPrompts/DeletePrompt 5개 PostgresStore 함수 제거.
// 글로벌 prompt는 .md 코드 계약. project_prompts 흐름만 유지.

func (s *PostgresStore) SaveProjectPrompt(prompt domain.ProjectPrompt) error {
	_, err := s.db.Exec(
		`INSERT INTO project_prompts (
			project_id, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (project_id, version, operation) DO UPDATE
		SET title = EXCLUDED.title,
		    status = EXCLUDED.status,
		    summary = EXCLUDED.summary,
		    content = EXCLUDED.content,
		    content_hash = EXCLUDED.content_hash,
		    updated_at = EXCLUDED.updated_at`,
		prompt.ProjectID,
		prompt.Version,
		prompt.Operation,
		prompt.Title,
		prompt.Status,
		prompt.Summary,
		prompt.Content,
		prompt.ContentHash,
		prompt.CreatedAt,
		prompt.UpdatedAt,
	)
	return err
}

func (s *PostgresStore) GetProjectPrompt(projectID, version, operation string) (domain.ProjectPrompt, error) {
	row := s.db.QueryRow(
		`SELECT project_id::text, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		 FROM project_prompts
		 WHERE project_id = $1::uuid AND version = $2 AND operation = $3`,
		projectID,
		version,
		operation,
	)

	var prompt domain.ProjectPrompt
	if err := row.Scan(
		&prompt.ProjectID,
		&prompt.Version,
		&prompt.Operation,
		&prompt.Title,
		&prompt.Status,
		&prompt.Summary,
		&prompt.Content,
		&prompt.ContentHash,
		&prompt.CreatedAt,
		&prompt.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ProjectPrompt{}, ErrNotFound
		}
		return domain.ProjectPrompt{}, err
	}
	return prompt, nil
}

func (s *PostgresStore) ListProjectPrompts(projectID string) ([]domain.ProjectPrompt, error) {
	rows, err := s.db.Query(
		`SELECT project_id::text, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		 FROM project_prompts
		 WHERE project_id = $1::uuid
		 ORDER BY version ASC, operation ASC, updated_at DESC`,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.ProjectPrompt, 0)
	for rows.Next() {
		var prompt domain.ProjectPrompt
		if err := rows.Scan(
			&prompt.ProjectID,
			&prompt.Version,
			&prompt.Operation,
			&prompt.Title,
			&prompt.Status,
			&prompt.Summary,
			&prompt.Content,
			&prompt.ContentHash,
			&prompt.CreatedAt,
			&prompt.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, prompt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) SaveProjectPromptDefaults(defaults domain.ProjectPromptDefaults) error {
	var updatedAt any
	if defaults.UpdatedAt != nil {
		updatedAt = *defaults.UpdatedAt
	}
	// ADR-015 Phase A5: 6-default schema (prepare/sentiment/planner/
	// planner_meta/issue_evidence_summary/execution_final_answer). New
	// columns are nullable so an existing row that pre-dates the schema
	// migration stays valid; ON CONFLICT updates only the columns that
	// were sent.
	_, err := s.db.Exec(
		`INSERT INTO project_prompt_defaults (
			project_id, prepare_prompt_version, sentiment_prompt_version,
			planner_prompt_version, planner_meta_prompt_version,
			issue_evidence_summary_prompt_version, execution_final_answer_prompt_version,
			updated_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7, $8
		)
		ON CONFLICT (project_id) DO UPDATE
		SET prepare_prompt_version = EXCLUDED.prepare_prompt_version,
		    sentiment_prompt_version = EXCLUDED.sentiment_prompt_version,
		    planner_prompt_version = EXCLUDED.planner_prompt_version,
		    planner_meta_prompt_version = EXCLUDED.planner_meta_prompt_version,
		    issue_evidence_summary_prompt_version = EXCLUDED.issue_evidence_summary_prompt_version,
		    execution_final_answer_prompt_version = EXCLUDED.execution_final_answer_prompt_version,
		    updated_at = EXCLUDED.updated_at`,
		defaults.ProjectID,
		nullableString(defaults.PreparePromptVersion),
		nullableString(defaults.SentimentPromptVersion),
		nullableString(defaults.PlannerPromptVersion),
		nullableString(defaults.PlannerMetaPromptVersion),
		nullableString(defaults.IssueEvidenceSummaryPromptVersion),
		nullableString(defaults.ExecutionFinalAnswerPromptVersion),
		updatedAt,
	)
	return err
}

func (s *PostgresStore) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	row := s.db.QueryRow(
		`SELECT project_id::text, prepare_prompt_version, sentiment_prompt_version,
		        planner_prompt_version, planner_meta_prompt_version,
		        issue_evidence_summary_prompt_version, execution_final_answer_prompt_version,
		        updated_at
		 FROM project_prompt_defaults
		 WHERE project_id = $1::uuid`,
		projectID,
	)

	var defaults domain.ProjectPromptDefaults
	var preparePromptVersion sql.NullString
	var sentimentPromptVersion sql.NullString
	var plannerPromptVersion sql.NullString
	var plannerMetaPromptVersion sql.NullString
	var issueEvidenceSummaryPromptVersion sql.NullString
	var executionFinalAnswerPromptVersion sql.NullString
	var updatedAt time.Time
	if err := row.Scan(
		&defaults.ProjectID,
		&preparePromptVersion,
		&sentimentPromptVersion,
		&plannerPromptVersion,
		&plannerMetaPromptVersion,
		&issueEvidenceSummaryPromptVersion,
		&executionFinalAnswerPromptVersion,
		&updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ProjectPromptDefaults{}, ErrNotFound
		}
		return domain.ProjectPromptDefaults{}, err
	}
	if preparePromptVersion.Valid {
		defaults.PreparePromptVersion = &preparePromptVersion.String
	}
	if sentimentPromptVersion.Valid {
		defaults.SentimentPromptVersion = &sentimentPromptVersion.String
	}
	if plannerPromptVersion.Valid {
		defaults.PlannerPromptVersion = &plannerPromptVersion.String
	}
	if plannerMetaPromptVersion.Valid {
		defaults.PlannerMetaPromptVersion = &plannerMetaPromptVersion.String
	}
	if issueEvidenceSummaryPromptVersion.Valid {
		defaults.IssueEvidenceSummaryPromptVersion = &issueEvidenceSummaryPromptVersion.String
	}
	if executionFinalAnswerPromptVersion.Valid {
		defaults.ExecutionFinalAnswerPromptVersion = &executionFinalAnswerPromptVersion.String
	}
	defaults.UpdatedAt = &updatedAt
	return defaults, nil
}

// AppendProjectPromptChange writes a single ADR-015 §C audit row.
// Append-only by contract — there is no update or delete path.
func (s *PostgresStore) AppendProjectPromptChange(change domain.ProjectPromptChange) error {
	_, err := s.db.Exec(
		`INSERT INTO project_prompt_changes (
			change_id, project_id, version, operation, action, change_reason,
			changed_by, previous_content_hash, new_content_hash, base_version, changed_at
		) VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		change.ChangeID,
		change.ProjectID,
		change.Version,
		change.Operation,
		change.Action,
		change.ChangeReason,
		nullableEmptyString(change.ChangedBy),
		nullableEmptyString(change.PreviousContentHash),
		change.NewContentHash,
		nullableEmptyString(change.BaseVersion),
		change.ChangedAt,
	)
	return err
}

// ListProjectPromptChanges returns audit rows oldest-first, filtered by
// project (and optionally operation). Empty operation returns every
// project change.
func (s *PostgresStore) ListProjectPromptChanges(projectID, operation string) ([]domain.ProjectPromptChange, error) {
	rows, err := s.db.Query(
		`SELECT change_id, project_id::text, version, operation, action, change_reason,
		        COALESCE(changed_by, ''), COALESCE(previous_content_hash, ''),
		        new_content_hash, COALESCE(base_version, ''), changed_at
		 FROM project_prompt_changes
		 WHERE project_id = $1::uuid
		   AND ($2 = '' OR operation = $2)
		 ORDER BY changed_at ASC`,
		projectID,
		operation,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ProjectPromptChange, 0)
	for rows.Next() {
		var change domain.ProjectPromptChange
		if err := rows.Scan(
			&change.ChangeID,
			&change.ProjectID,
			&change.Version,
			&change.Operation,
			&change.Action,
			&change.ChangeReason,
			&change.ChangedBy,
			&change.PreviousContentHash,
			&change.NewContentHash,
			&change.BaseVersion,
			&change.ChangedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, change)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SaveDataset(dataset domain.Dataset) error {
	metadata, err := marshalJSON(defaultMetadataMap(dataset.Metadata))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO datasets (
		     dataset_id, project_id, name, description, data_type,
		     active_dataset_version_id, active_version_updated_at, metadata, created_at
		 ) VALUES (
		     $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::jsonb, $9
		 )
		 ON CONFLICT (dataset_id) DO UPDATE
		 SET project_id = EXCLUDED.project_id,
		     name = EXCLUDED.name,
		     description = EXCLUDED.description,
		     data_type = EXCLUDED.data_type,
		     active_dataset_version_id = EXCLUDED.active_dataset_version_id,
		     active_version_updated_at = EXCLUDED.active_version_updated_at,
		     metadata = EXCLUDED.metadata,
		     created_at = EXCLUDED.created_at`,
		dataset.DatasetID,
		dataset.ProjectID,
		dataset.Name,
		nullableString(dataset.Description),
		dataset.DataType,
		nullableString(dataset.ActiveDatasetVersionID),
		nullableTime(dataset.ActiveVersionUpdatedAt),
		metadata,
		dataset.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetDataset(projectID, datasetID string) (domain.Dataset, error) {
	row := s.db.QueryRow(
		`SELECT dataset_id::text, project_id::text, name, description, data_type,
		        active_dataset_version_id, active_version_updated_at, metadata, created_at
		 FROM datasets
		 WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	)

	var dataset domain.Dataset
	var description sql.NullString
	var activeDatasetVersionID sql.NullString
	var metadataRaw []byte
	if err := row.Scan(
		&dataset.DatasetID,
		&dataset.ProjectID,
		&dataset.Name,
		&description,
		&dataset.DataType,
		&activeDatasetVersionID,
		&dataset.ActiveVersionUpdatedAt,
		&metadataRaw,
		&dataset.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Dataset{}, ErrNotFound
		}
		return domain.Dataset{}, err
	}
	if description.Valid {
		dataset.Description = &description.String
	}
	if activeDatasetVersionID.Valid {
		dataset.ActiveDatasetVersionID = &activeDatasetVersionID.String
	}
	if err := unmarshalJSON(metadataRaw, &dataset.Metadata, map[string]any{}); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func (s *PostgresStore) ListDatasets(projectID string) ([]domain.Dataset, error) {
	rows, err := s.db.Query(
		`SELECT dataset_id::text, project_id::text, name, description, data_type,
		        active_dataset_version_id, active_version_updated_at, metadata, created_at
		 FROM datasets
		 WHERE project_id = $1::uuid
		 ORDER BY created_at ASC, dataset_id ASC`,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Dataset, 0)
	for rows.Next() {
		var dataset domain.Dataset
		var description sql.NullString
		var activeDatasetVersionID sql.NullString
		var metadataRaw []byte
		if err := rows.Scan(
			&dataset.DatasetID,
			&dataset.ProjectID,
			&dataset.Name,
			&description,
			&dataset.DataType,
			&activeDatasetVersionID,
			&dataset.ActiveVersionUpdatedAt,
			&metadataRaw,
			&dataset.CreatedAt,
		); err != nil {
			return nil, err
		}
		if description.Valid {
			dataset.Description = &description.String
		}
		if err := unmarshalJSON(metadataRaw, &dataset.Metadata, map[string]any{}); err != nil {
			return nil, err
		}
		if activeDatasetVersionID.Valid {
			dataset.ActiveDatasetVersionID = &activeDatasetVersionID.String
		}
		items = append(items, dataset)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) DeleteDataset(projectID, datasetID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = deleteEmbeddingIndexChunksForDataset(tx, projectID, datasetID); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM dataset_build_jobs
		  WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM dataset_versions
		  WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	); err != nil {
		return err
	}
	result, err := tx.Exec(
		`DELETE FROM datasets
		  WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return tx.Commit()
}

func (s *PostgresStore) SaveDatasetVersion(version domain.DatasetVersion) error {
	version = normalizeDatasetVersionCleanFields(version)
	metadataJSON, err := marshalJSON(version.Metadata)
	if err != nil {
		return err
	}
	profileJSON, err := marshalJSON(version.Profile)
	if err != nil {
		return err
	}

	// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment/embedding 15
	// 컬럼 INSERT/UPDATE 제거. DB 컬럼은 그대로 (PR5 마이그레이션에서 DROP)
	// 둠 — 미 명시 컬럼은 default 또는 NULL로 채워진다.
	_, err = s.db.Exec(
		`INSERT INTO dataset_versions (
		     dataset_version_id, dataset_id, project_id, storage_uri, data_type, record_count,
		     metadata, profile, clean_status, clean_uri, cleaned_at,
		     created_at, ready_at
		 ) VALUES (
		     $1, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13
		 )
		 ON CONFLICT (dataset_version_id) DO UPDATE
		 SET dataset_id = EXCLUDED.dataset_id,
		     project_id = EXCLUDED.project_id,
		     storage_uri = EXCLUDED.storage_uri,
		     data_type = EXCLUDED.data_type,
		     record_count = EXCLUDED.record_count,
		     metadata = EXCLUDED.metadata,
		     profile = EXCLUDED.profile,
		     clean_status = EXCLUDED.clean_status,
		     clean_uri = EXCLUDED.clean_uri,
		     cleaned_at = EXCLUDED.cleaned_at,
		     created_at = EXCLUDED.created_at,
		     ready_at = EXCLUDED.ready_at`,
		version.DatasetVersionID,
		version.DatasetID,
		version.ProjectID,
		version.StorageURI,
		version.DataType,
		version.RecordCount,
		metadataJSON,
		profileJSON,
		version.CleanStatus,
		nullableString(version.CleanURI),
		nullableTime(version.CleanedAt),
		version.CreatedAt,
		nullableTime(version.ReadyAt),
	)
	if err != nil {
		return err
	}
	return s.syncDatasetVersionArtifacts(version)
}

func (s *PostgresStore) GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error) {
	// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment/embedding 15
	// 컬럼 SELECT/scan 제거. DB 컬럼은 그대로 두지만 Go side에서 안 읽음.
	row := s.db.QueryRow(
		`SELECT dataset_version_id, dataset_id::text, project_id::text, storage_uri, data_type,
		        record_count, metadata, profile, clean_status, clean_uri, cleaned_at,
		        created_at, ready_at
		 FROM dataset_versions
		 WHERE project_id = $1::uuid AND dataset_version_id = $2`,
		projectID,
		datasetVersionID,
	)

	var version domain.DatasetVersion
	var recordCount sql.NullInt64
	var metadataRaw []byte
	var profileRaw []byte
	var cleanURI sql.NullString
	if err := row.Scan(
		&version.DatasetVersionID,
		&version.DatasetID,
		&version.ProjectID,
		&version.StorageURI,
		&version.DataType,
		&recordCount,
		&metadataRaw,
		&profileRaw,
		&version.CleanStatus,
		&cleanURI,
		&version.CleanedAt,
		&version.CreatedAt,
		&version.ReadyAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.DatasetVersion{}, ErrNotFound
		}
		return domain.DatasetVersion{}, err
	}
	if err := unmarshalJSON(metadataRaw, &version.Metadata, map[string]any{}); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := unmarshalJSON(profileRaw, &version.Profile, (*domain.DatasetProfile)(nil)); err != nil {
		return domain.DatasetVersion{}, err
	}
	if recordCount.Valid {
		value := int(recordCount.Int64)
		version.RecordCount = &value
	}
	if cleanURI.Valid {
		version.CleanURI = &cleanURI.String
		version.CleanedRef = &cleanURI.String
	}
	version = normalizeDatasetVersionCleanFields(version)
	artifacts, err := s.ListDatasetVersionArtifacts(projectID, version.DatasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	version.Artifacts = artifacts
	return version, nil
}

func (s *PostgresStore) ListDatasetVersions(projectID, datasetID string) ([]domain.DatasetVersion, error) {
	// silverone 2026-05-28 (β2 cleanup PR2) — GetDatasetVersion과 동일하게
	// prepare/sentiment/embedding 15 컬럼 SELECT/scan 제거.
	rows, err := s.db.Query(
		`SELECT dataset_version_id, dataset_id::text, project_id::text, storage_uri, data_type,
		        record_count, metadata, profile, clean_status, clean_uri, cleaned_at,
		        created_at, ready_at
		 FROM dataset_versions
		 WHERE project_id = $1::uuid AND dataset_id = $2::uuid
		 ORDER BY created_at DESC, dataset_version_id DESC`,
		projectID,
		datasetID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetVersion, 0)
	for rows.Next() {
		var version domain.DatasetVersion
		var recordCount sql.NullInt64
		var metadataRaw []byte
		var profileRaw []byte
		var cleanURI sql.NullString
		if err := rows.Scan(
			&version.DatasetVersionID,
			&version.DatasetID,
			&version.ProjectID,
			&version.StorageURI,
			&version.DataType,
			&recordCount,
			&metadataRaw,
			&profileRaw,
			&version.CleanStatus,
			&cleanURI,
			&version.CleanedAt,
			&version.CreatedAt,
			&version.ReadyAt,
		); err != nil {
			return nil, err
		}
		if recordCount.Valid {
			value := int(recordCount.Int64)
			version.RecordCount = &value
		}
		if err := unmarshalJSON(metadataRaw, &version.Metadata, map[string]any{}); err != nil {
			return nil, err
		}
		if err := unmarshalNullableJSON(profileRaw, &version.Profile); err != nil {
			return nil, err
		}
		if cleanURI.Valid {
			version.CleanURI = &cleanURI.String
			version.CleanedRef = &cleanURI.String
		}
		version = normalizeDatasetVersionCleanFields(version)
		artifacts, err := s.ListDatasetVersionArtifacts(projectID, version.DatasetVersionID)
		if err != nil {
			return nil, err
		}
		version.Artifacts = artifacts
		items = append(items, version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) DeleteDatasetVersion(projectID, datasetID, datasetVersionID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = deleteEmbeddingIndexChunksForVersion(tx, datasetVersionID); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM dataset_build_jobs
		  WHERE project_id = $1::uuid
		    AND dataset_id = $2::uuid
		    AND dataset_version_id = $3`,
		projectID,
		datasetID,
		datasetVersionID,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`UPDATE datasets
		    SET active_dataset_version_id = NULL,
		        active_version_updated_at = NOW()
		  WHERE project_id = $1::uuid
		    AND dataset_id = $2::uuid
		    AND active_dataset_version_id = $3`,
		projectID,
		datasetID,
		datasetVersionID,
	); err != nil {
		return err
	}
	result, err := tx.Exec(
		`DELETE FROM dataset_versions
		  WHERE project_id = $1::uuid
		    AND dataset_id = $2::uuid
		    AND dataset_version_id = $3`,
		projectID,
		datasetID,
		datasetVersionID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return tx.Commit()
}

func deleteEmbeddingIndexChunksForProject(tx *sql.Tx, projectID string) error {
	exists, err := tableExists(tx, "embedding_index_chunks")
	if err != nil || !exists {
		return err
	}
	_, err = tx.Exec(
		`DELETE FROM embedding_index_chunks
		  WHERE dataset_version_id IN (
		      SELECT dataset_version_id
		      FROM dataset_versions
		      WHERE project_id = $1::uuid
		  )`,
		projectID,
	)
	return err
}

func deleteEmbeddingIndexChunksForDataset(tx *sql.Tx, projectID, datasetID string) error {
	exists, err := tableExists(tx, "embedding_index_chunks")
	if err != nil || !exists {
		return err
	}
	_, err = tx.Exec(
		`DELETE FROM embedding_index_chunks
		  WHERE dataset_version_id IN (
		      SELECT dataset_version_id
		      FROM dataset_versions
		      WHERE project_id = $1::uuid AND dataset_id = $2::uuid
		  )`,
		projectID,
		datasetID,
	)
	return err
}

func deleteEmbeddingIndexChunksForVersion(tx *sql.Tx, datasetVersionID string) error {
	exists, err := tableExists(tx, "embedding_index_chunks")
	if err != nil || !exists {
		return err
	}
	_, err = tx.Exec(
		`DELETE FROM embedding_index_chunks
		  WHERE dataset_version_id = $1`,
		datasetVersionID,
	)
	return err
}

func tableExists(tx *sql.Tx, tableName string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, "public."+tableName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *PostgresStore) syncDatasetVersionArtifacts(version domain.DatasetVersion) (err error) {
	artifacts := deriveDatasetVersionArtifacts(version, time.Now().UTC())
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	staleArgs := []any{version.ProjectID, version.DatasetVersionID}
	if len(artifacts) == 0 {
		if _, err = tx.Exec(
			`DELETE FROM dataset_version_artifacts
			  WHERE project_id = $1::uuid AND dataset_version_id = $2`,
			staleArgs...,
		); err != nil {
			return err
		}
		return tx.Commit()
	}

	placeholders := make([]string, 0, len(artifacts))
	for index, artifact := range artifacts {
		summaryJSON, marshalErr := marshalJSON(defaultMetadataMap(artifact.Summary))
		if marshalErr != nil {
			return marshalErr
		}
		metadataJSON, marshalErr := marshalJSON(defaultMetadataMap(artifact.Metadata))
		if marshalErr != nil {
			return marshalErr
		}
		if _, err = tx.Exec(
			`INSERT INTO dataset_version_artifacts (
				artifact_id, project_id, dataset_id, dataset_version_id, artifact_type,
				stage, status, uri, format, model, prompt_version, summary, metadata,
				created_at, updated_at
			) VALUES (
				$1, $2::uuid, $3::uuid, $4, $5,
				$6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb,
				$14, $15
			)
			ON CONFLICT (dataset_version_id, artifact_type) DO UPDATE
			SET artifact_id = EXCLUDED.artifact_id,
			    project_id = EXCLUDED.project_id,
			    dataset_id = EXCLUDED.dataset_id,
			    stage = EXCLUDED.stage,
			    status = EXCLUDED.status,
			    uri = EXCLUDED.uri,
			    format = EXCLUDED.format,
			    model = EXCLUDED.model,
			    prompt_version = EXCLUDED.prompt_version,
			    summary = EXCLUDED.summary,
			    metadata = EXCLUDED.metadata,
			    updated_at = EXCLUDED.updated_at
			-- silverone 2026-05-28 (B1): no-op update 방지. payload field 중 어느
			-- 하나라도 다를 때만 UPDATE 실행 → updated_at 갱신은 *실제 변경* 시점
			-- 으로 한정. GET dataset_version 흐름이 attachDatasetVersionArtifacts
			-- 를 호출해도 값이 같으면 row가 touch되지 않는다.
			WHERE
			   dataset_version_artifacts.stage          IS DISTINCT FROM EXCLUDED.stage
			OR dataset_version_artifacts.status         IS DISTINCT FROM EXCLUDED.status
			OR dataset_version_artifacts.uri            IS DISTINCT FROM EXCLUDED.uri
			OR dataset_version_artifacts.format         IS DISTINCT FROM EXCLUDED.format
			OR dataset_version_artifacts.model          IS DISTINCT FROM EXCLUDED.model
			OR dataset_version_artifacts.prompt_version IS DISTINCT FROM EXCLUDED.prompt_version
			OR dataset_version_artifacts.summary        IS DISTINCT FROM EXCLUDED.summary
			OR dataset_version_artifacts.metadata       IS DISTINCT FROM EXCLUDED.metadata`,
			artifact.ArtifactID,
			artifact.ProjectID,
			artifact.DatasetID,
			artifact.DatasetVersionID,
			artifact.ArtifactType,
			artifact.Stage,
			artifact.Status,
			nullIfEmpty(artifact.URI),
			nullIfEmpty(artifact.Format),
			nullIfEmpty(artifact.Model),
			nullIfEmpty(artifact.PromptVersion),
			summaryJSON,
			metadataJSON,
			artifact.CreatedAt,
			artifact.UpdatedAt,
		); err != nil {
			return err
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", index+3))
		staleArgs = append(staleArgs, artifact.ArtifactType)
	}

	if _, err = tx.Exec(
		fmt.Sprintf(
			`DELETE FROM dataset_version_artifacts
			  WHERE project_id = $1::uuid
			    AND dataset_version_id = $2
			    AND artifact_type NOT IN (%s)`,
			strings.Join(placeholders, ", "),
		),
		staleArgs...,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) ListDatasetVersionArtifacts(projectID, datasetVersionID string) ([]domain.DatasetVersionArtifact, error) {
	rows, err := s.db.Query(
		`SELECT artifact_id, project_id::text, dataset_id::text, dataset_version_id,
		        artifact_type, stage, status, uri, format, model, prompt_version,
		        summary, metadata, created_at, updated_at
		 FROM dataset_version_artifacts
		 WHERE project_id = $1::uuid AND dataset_version_id = $2
		 ORDER BY
		   CASE stage
		     WHEN 'source' THEN 10
		     WHEN 'clean' THEN 20
		     WHEN 'prepare' THEN 30
		     WHEN 'sentiment' THEN 40
		     WHEN 'embedding' THEN 50
		     WHEN 'cluster' THEN 60
		     ELSE 100
		   END,
		   artifact_type ASC`,
		projectID,
		datasetVersionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetVersionArtifact, 0)
	for rows.Next() {
		var artifact domain.DatasetVersionArtifact
		var uri sql.NullString
		var format sql.NullString
		var model sql.NullString
		var promptVersion sql.NullString
		var summaryRaw []byte
		var metadataRaw []byte
		if err := rows.Scan(
			&artifact.ArtifactID,
			&artifact.ProjectID,
			&artifact.DatasetID,
			&artifact.DatasetVersionID,
			&artifact.ArtifactType,
			&artifact.Stage,
			&artifact.Status,
			&uri,
			&format,
			&model,
			&promptVersion,
			&summaryRaw,
			&metadataRaw,
			&artifact.CreatedAt,
			&artifact.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if uri.Valid {
			artifact.URI = uri.String
		}
		if format.Valid {
			artifact.Format = format.String
		}
		if model.Valid {
			artifact.Model = model.String
		}
		if promptVersion.Valid {
			artifact.PromptVersion = promptVersion.String
		}
		if err := unmarshalJSON(summaryRaw, &artifact.Summary, map[string]any{}); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(metadataRaw, &artifact.Metadata, map[string]any{}); err != nil {
			return nil, err
		}
		items = append(items, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) SaveDatasetBuildJob(job domain.DatasetBuildJob) error {
	requestJSON, err := marshalJSON(defaultMetadataMap(job.Request))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO dataset_build_jobs (
			job_id, project_id, dataset_id, dataset_version_id, build_type, status,
			request, triggered_by, workflow_id, workflow_run_id, attempt, error_message, last_error_type,
			created_at, started_at, completed_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
			$7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
		ON CONFLICT (job_id) DO UPDATE
		SET dataset_id = EXCLUDED.dataset_id,
		    dataset_version_id = EXCLUDED.dataset_version_id,
		    build_type = EXCLUDED.build_type,
		    status = EXCLUDED.status,
		    request = EXCLUDED.request,
		    triggered_by = EXCLUDED.triggered_by,
		    workflow_id = EXCLUDED.workflow_id,
		    workflow_run_id = EXCLUDED.workflow_run_id,
		    attempt = EXCLUDED.attempt,
		    last_error_type = EXCLUDED.last_error_type,
		    started_at = EXCLUDED.started_at,
		    completed_at = EXCLUDED.completed_at,
		    error_message = EXCLUDED.error_message`,
		job.JobID,
		job.ProjectID,
		job.DatasetID,
		job.DatasetVersionID,
		job.BuildType,
		job.Status,
		requestJSON,
		nullIfEmpty(job.TriggeredBy),
		nullableString(job.WorkflowID),
		nullableString(job.WorkflowRunID),
		job.Attempt,
		nullableString(job.ErrorMessage),
		nullableString(job.LastErrorType),
		job.CreatedAt,
		nullableTime(job.StartedAt),
		nullableTime(job.CompletedAt),
	)
	return err
}

func (s *PostgresStore) GetDatasetBuildJob(projectID, jobID string) (domain.DatasetBuildJob, error) {
	row := s.db.QueryRow(
		`SELECT job_id::text, project_id::text, dataset_id::text, dataset_version_id, build_type, status,
		        request, triggered_by, workflow_id, workflow_run_id, attempt, error_message, last_error_type,
		        created_at, started_at, completed_at
		 FROM dataset_build_jobs
		 WHERE project_id = $1::uuid AND job_id = $2::uuid`,
		projectID,
		jobID,
	)
	var job domain.DatasetBuildJob
	var requestRaw []byte
	var triggeredBy sql.NullString
	var workflowID sql.NullString
	var workflowRunID sql.NullString
	var errorMessage sql.NullString
	var lastErrorType sql.NullString
	if err := row.Scan(
		&job.JobID,
		&job.ProjectID,
		&job.DatasetID,
		&job.DatasetVersionID,
		&job.BuildType,
		&job.Status,
		&requestRaw,
		&triggeredBy,
		&workflowID,
		&workflowRunID,
		&job.Attempt,
		&errorMessage,
		&lastErrorType,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.DatasetBuildJob{}, ErrNotFound
		}
		return domain.DatasetBuildJob{}, err
	}
	if err := unmarshalJSON(requestRaw, &job.Request, map[string]any{}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if triggeredBy.Valid {
		job.TriggeredBy = triggeredBy.String
	}
	if workflowID.Valid {
		job.WorkflowID = &workflowID.String
	}
	if workflowRunID.Valid {
		job.WorkflowRunID = &workflowRunID.String
	}
	if errorMessage.Valid {
		job.ErrorMessage = &errorMessage.String
	}
	if lastErrorType.Valid {
		job.LastErrorType = &lastErrorType.String
	}
	return job, nil
}

func (s *PostgresStore) ListDatasetBuildJobs(projectID, datasetVersionID string) ([]domain.DatasetBuildJob, error) {
	baseQuery := `SELECT job_id::text, project_id::text, dataset_id::text, dataset_version_id, build_type, status,
	                     request, triggered_by, workflow_id, workflow_run_id, attempt, error_message, last_error_type,
	                     created_at, started_at, completed_at
	              FROM dataset_build_jobs
	              WHERE project_id = $1::uuid`
	args := []any{projectID}
	if strings.TrimSpace(datasetVersionID) != "" {
		baseQuery += ` AND dataset_version_id = $2`
		args = append(args, datasetVersionID)
	}
	baseQuery += ` ORDER BY created_at DESC, job_id DESC`

	rows, err := s.db.Query(baseQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetBuildJob, 0)
	for rows.Next() {
		var job domain.DatasetBuildJob
		var requestRaw []byte
		var triggeredBy sql.NullString
		var workflowID sql.NullString
		var workflowRunID sql.NullString
		var errorMessage sql.NullString
		var lastErrorType sql.NullString
		if err := rows.Scan(
			&job.JobID,
			&job.ProjectID,
			&job.DatasetID,
			&job.DatasetVersionID,
			&job.BuildType,
			&job.Status,
			&requestRaw,
			&triggeredBy,
			&workflowID,
			&workflowRunID,
			&job.Attempt,
			&errorMessage,
			&lastErrorType,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(requestRaw, &job.Request, map[string]any{}); err != nil {
			return nil, err
		}
		if triggeredBy.Valid {
			job.TriggeredBy = triggeredBy.String
		}
		if workflowID.Valid {
			job.WorkflowID = &workflowID.String
		}
		if workflowRunID.Valid {
			job.WorkflowRunID = &workflowRunID.String
		}
		if errorMessage.Valid {
			job.ErrorMessage = &errorMessage.String
		}
		if lastErrorType.Valid {
			job.LastErrorType = &lastErrorType.String
		}
		items = append(items, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// ListInFlightDatasetBuildJobs — silverone 2026-05-27 (Codex adversarial review
// fix-2). status가 queued/running인 모든 row를 가져온다. project_id 필터 없음 —
// startup reconciliation은 system 전체를 본다.
func (s *PostgresStore) ListInFlightDatasetBuildJobs() ([]domain.DatasetBuildJob, error) {
	query := `SELECT job_id::text, project_id::text, dataset_id::text, dataset_version_id, build_type, status,
	                 request, triggered_by, workflow_id, workflow_run_id, attempt, error_message, last_error_type,
	                 created_at, started_at, completed_at
	          FROM dataset_build_jobs
	          WHERE status IN ('queued','running')
	          ORDER BY created_at ASC`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.DatasetBuildJob, 0)
	for rows.Next() {
		var job domain.DatasetBuildJob
		var requestRaw []byte
		var triggeredBy, workflowID, workflowRunID, errorMessage, lastErrorType sql.NullString
		if err := rows.Scan(
			&job.JobID, &job.ProjectID, &job.DatasetID, &job.DatasetVersionID,
			&job.BuildType, &job.Status, &requestRaw,
			&triggeredBy, &workflowID, &workflowRunID,
			&job.Attempt, &errorMessage, &lastErrorType,
			&job.CreatedAt, &job.StartedAt, &job.CompletedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(requestRaw, &job.Request, map[string]any{}); err != nil {
			return nil, err
		}
		if triggeredBy.Valid {
			job.TriggeredBy = triggeredBy.String
		}
		if workflowID.Valid {
			job.WorkflowID = &workflowID.String
		}
		if workflowRunID.Valid {
			job.WorkflowRunID = &workflowRunID.String
		}
		if errorMessage.Valid {
			job.ErrorMessage = &errorMessage.String
		}
		if lastErrorType.Valid {
			job.LastErrorType = &lastErrorType.String
		}
		items = append(items, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) SaveAnalysisThread(thread domain.AnalysisThread) error {
	_, err := s.db.Exec(
		`INSERT INTO analysis_threads (
			thread_id, project_id, dataset_id, dataset_version_id, title, created_at, updated_at
		) VALUES (
			$1, $2::uuid, $3::uuid, $4, $5, $6, $7
		)
		ON CONFLICT (thread_id) DO UPDATE
		SET title = EXCLUDED.title,
		    updated_at = EXCLUDED.updated_at`,
		thread.ThreadID,
		thread.ProjectID,
		thread.DatasetID,
		thread.DatasetVersionID,
		nullIfEmpty(thread.Title),
		thread.CreatedAt,
		thread.UpdatedAt,
	)
	return err
}

func (s *PostgresStore) GetAnalysisThread(projectID, datasetID, threadID string) (domain.AnalysisThread, error) {
	row := s.db.QueryRow(
		`SELECT t.thread_id, t.project_id::text, t.dataset_id::text, t.dataset_version_id,
		        COALESCE(t.title, ''), t.created_at, t.updated_at,
		        COUNT(m.message_id)::int AS message_count,
		        COALESCE((
		          SELECT lm.content
		          FROM analysis_messages lm
		          WHERE lm.thread_id = t.thread_id
		          ORDER BY lm.created_at DESC, lm.message_id DESC
		          LIMIT 1
		        ), '') AS last_message
		 FROM analysis_threads t
		 LEFT JOIN analysis_messages m ON m.thread_id = t.thread_id
		 WHERE t.project_id = $1::uuid AND t.dataset_id = $2::uuid AND t.thread_id = $3
		 GROUP BY t.thread_id`,
		projectID,
		datasetID,
		threadID,
	)
	var thread domain.AnalysisThread
	if err := row.Scan(
		&thread.ThreadID,
		&thread.ProjectID,
		&thread.DatasetID,
		&thread.DatasetVersionID,
		&thread.Title,
		&thread.CreatedAt,
		&thread.UpdatedAt,
		&thread.MessageCount,
		&thread.LastMessage,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AnalysisThread{}, ErrNotFound
		}
		return domain.AnalysisThread{}, err
	}
	thread.LastMessage = truncateAnalysisTitle(thread.LastMessage)
	return thread, nil
}

func (s *PostgresStore) DeleteAnalysisThread(projectID, datasetID, threadID string) error {
	// project_id + dataset_id + thread_id가 모두 일치하는 row만 삭제 → dataset 불일치
	// 시 0 rows → ErrNotFound(404). analysis_messages / analysis_runs /
	// planner_rejection_events는 FK ON DELETE CASCADE로 함께 삭제된다.
	res, err := s.db.Exec(
		`DELETE FROM analysis_threads
		 WHERE thread_id = $1 AND project_id = $2::uuid AND dataset_id = $3::uuid`,
		threadID,
		projectID,
		datasetID,
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

func (s *PostgresStore) ListAnalysisThreads(projectID, datasetID string) ([]domain.AnalysisThread, error) {
	rows, err := s.db.Query(
		`SELECT t.thread_id, t.project_id::text, t.dataset_id::text, t.dataset_version_id,
		        COALESCE(t.title, ''), t.created_at, t.updated_at,
		        COUNT(m.message_id)::int AS message_count,
		        COALESCE((
		          SELECT lm.content
		          FROM analysis_messages lm
		          WHERE lm.thread_id = t.thread_id
		          ORDER BY lm.created_at DESC, lm.message_id DESC
		          LIMIT 1
		        ), '') AS last_message
		 FROM analysis_threads t
		 LEFT JOIN analysis_messages m ON m.thread_id = t.thread_id
		 WHERE t.project_id = $1::uuid AND t.dataset_id = $2::uuid
		 GROUP BY t.thread_id
		 ORDER BY t.updated_at DESC, t.thread_id DESC`,
		projectID,
		datasetID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.AnalysisThread, 0)
	for rows.Next() {
		var thread domain.AnalysisThread
		if err := rows.Scan(
			&thread.ThreadID,
			&thread.ProjectID,
			&thread.DatasetID,
			&thread.DatasetVersionID,
			&thread.Title,
			&thread.CreatedAt,
			&thread.UpdatedAt,
			&thread.MessageCount,
			&thread.LastMessage,
		); err != nil {
			return nil, err
		}
		thread.LastMessage = truncateAnalysisTitle(thread.LastMessage)
		items = append(items, thread)
	}
	return items, rows.Err()
}

// silverone 2026-06-01 — project 사이드바 채팅 count. dataset 단위 합산이
// 아닌 단일 COUNT 쿼리(analysis_threads.project_id 인덱스 활용)로 처리.
// project가 없거나 thread 0건이면 0 반환 (ErrNotFound 아님).
func (s *PostgresStore) CountAnalysisThreadsByProject(projectID string) (int, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*)::int FROM analysis_threads WHERE project_id = $1::uuid`,
		projectID,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PostgresStore) SaveAnalysisMessage(message domain.AnalysisMessage) error {
	contextSummary, err := marshalJSON(defaultMetadataMap(message.ContextSummary))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO analysis_messages (
			message_id, thread_id, project_id, dataset_id, role, content,
			context_summary, run_id, created_at
		) VALUES (
			$1, $2, $3::uuid, $4::uuid, $5, $6, $7::jsonb, $8, $9
		)
		ON CONFLICT (message_id) DO UPDATE
		SET content = EXCLUDED.content,
		    context_summary = EXCLUDED.context_summary,
		    run_id = EXCLUDED.run_id`,
		message.MessageID,
		message.ThreadID,
		message.ProjectID,
		message.DatasetID,
		message.Role,
		message.Content,
		contextSummary,
		nullableString(message.RunID),
		message.CreatedAt,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`UPDATE analysis_threads
		  SET updated_at = GREATEST(updated_at, $1),
		      title = COALESCE(NULLIF(title, ''), $2)
		  WHERE thread_id = $3`,
		message.CreatedAt,
		nullIfEmpty(truncateAnalysisTitle(message.Content)),
		message.ThreadID,
	)
	return err
}

func (s *PostgresStore) ListAnalysisMessages(projectID, threadID string) ([]domain.AnalysisMessage, error) {
	rows, err := s.db.Query(
		`SELECT message_id, thread_id, project_id::text, dataset_id::text, role, content,
		        context_summary, run_id, created_at
		 FROM analysis_messages
		 WHERE project_id = $1::uuid AND thread_id = $2
		 ORDER BY created_at ASC, message_id ASC`,
		projectID,
		threadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.AnalysisMessage, 0)
	for rows.Next() {
		var message domain.AnalysisMessage
		var contextRaw []byte
		var runID sql.NullString
		if err := rows.Scan(
			&message.MessageID,
			&message.ThreadID,
			&message.ProjectID,
			&message.DatasetID,
			&message.Role,
			&message.Content,
			&contextRaw,
			&runID,
			&message.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(contextRaw, &message.ContextSummary, map[string]any{}); err != nil {
			return nil, err
		}
		if runID.Valid {
			message.RunID = &runID.String
		}
		items = append(items, message)
	}
	return items, rows.Err()
}

func (s *PostgresStore) SaveAnalysisRun(run domain.AnalysisRun) error {
	requestJSON, err := marshalJSON(defaultMetadataMap(run.RequestJSON))
	if err != nil {
		return err
	}
	resultJSON := []byte("null")
	if len(run.ResultJSON) > 0 {
		resultJSON = run.ResultJSON
	}
	_, err = s.db.Exec(
		`INSERT INTO analysis_runs (
			run_id, thread_id, project_id, dataset_id, dataset_version_id, user_message_id,
			request_json, result_json, status, error_message, created_at, completed_at
		) VALUES (
			$1, $2, $3::uuid, $4::uuid, $5, $6,
			$7::jsonb, $8::jsonb, $9, $10, $11, $12
		)
		ON CONFLICT (run_id) DO UPDATE
		SET request_json = EXCLUDED.request_json,
		    result_json = EXCLUDED.result_json,
		    status = EXCLUDED.status,
		    error_message = EXCLUDED.error_message,
		    completed_at = EXCLUDED.completed_at`,
		run.RunID,
		run.ThreadID,
		run.ProjectID,
		run.DatasetID,
		run.DatasetVersionID,
		nullIfEmpty(run.UserMessageID),
		requestJSON,
		resultJSON,
		run.Status,
		nullableString(run.ErrorMessage),
		run.CreatedAt,
		nullableTime(run.CompletedAt),
	)
	return err
}

func (s *PostgresStore) SaveRejectionEvent(event domain.PlannerRejectionEvent) error {
	var capabilityGap any
	if len(event.CapabilityGap) > 0 {
		encoded, err := marshalJSON(event.CapabilityGap)
		if err != nil {
			return err
		}
		capabilityGap = encoded
	}
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	// message_id UNIQUE — 같은 거절 응답을 재처리해도 중복 적재하지 않는다 (idempotent).
	_, err := s.db.Exec(
		`INSERT INTO planner_rejection_events (
			event_id, project_id, dataset_id, thread_id, message_id,
			user_question, reason, message, capability_gap, created_at
		) VALUES (
			$1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9::jsonb, $10
		)
		ON CONFLICT (message_id) DO NOTHING`,
		event.EventID,
		event.ProjectID,
		event.DatasetID,
		nullIfEmpty(event.ThreadID),
		event.MessageID,
		event.UserQuestion,
		event.Reason,
		nullableEmptyString(event.Message),
		capabilityGap,
		createdAt,
	)
	return err
}

func (s *PostgresStore) GetAnalysisRun(projectID, runID string) (domain.AnalysisRun, error) {
	row := s.db.QueryRow(
		`SELECT run_id, thread_id, project_id::text, dataset_id::text, dataset_version_id,
		        COALESCE(user_message_id, ''), request_json, result_json, status,
		        error_message, created_at, completed_at
		 FROM analysis_runs
		 WHERE project_id = $1::uuid AND run_id = $2`,
		projectID,
		runID,
	)
	var run domain.AnalysisRun
	var requestRaw []byte
	var resultRaw []byte
	var errorMessage sql.NullString
	if err := row.Scan(
		&run.RunID,
		&run.ThreadID,
		&run.ProjectID,
		&run.DatasetID,
		&run.DatasetVersionID,
		&run.UserMessageID,
		&requestRaw,
		&resultRaw,
		&run.Status,
		&errorMessage,
		&run.CreatedAt,
		&run.CompletedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AnalysisRun{}, ErrNotFound
		}
		return domain.AnalysisRun{}, err
	}
	if err := unmarshalJSON(requestRaw, &run.RequestJSON, map[string]any{}); err != nil {
		return domain.AnalysisRun{}, err
	}
	if string(resultRaw) != "null" {
		run.ResultJSON = append([]byte(nil), resultRaw...)
	}
	if errorMessage.Valid {
		run.ErrorMessage = &errorMessage.String
	}
	return run, nil
}

// GetLastSuccessfulAnalysisRun — silverone 2026-05-26 (plan reuse POC-1).
// thread 안 status='completed' run 중 가장 늦은 created_at 1건을 반환한다.
// 없으면 ErrNotFound. tie-break은 run_id 사전순 (deterministic).
func (s *PostgresStore) GetLastSuccessfulAnalysisRun(projectID, threadID string) (domain.AnalysisRun, error) {
	row := s.db.QueryRow(
		`SELECT run_id, thread_id, project_id::text, dataset_id::text, dataset_version_id,
		        COALESCE(user_message_id, ''), request_json, result_json, status,
		        error_message, created_at, completed_at
		 FROM analysis_runs
		 WHERE project_id = $1::uuid AND thread_id = $2 AND status = 'completed'
		 ORDER BY created_at DESC, run_id DESC
		 LIMIT 1`,
		projectID,
		threadID,
	)
	var run domain.AnalysisRun
	var requestRaw []byte
	var resultRaw []byte
	var errorMessage sql.NullString
	if err := row.Scan(
		&run.RunID,
		&run.ThreadID,
		&run.ProjectID,
		&run.DatasetID,
		&run.DatasetVersionID,
		&run.UserMessageID,
		&requestRaw,
		&resultRaw,
		&run.Status,
		&errorMessage,
		&run.CreatedAt,
		&run.CompletedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AnalysisRun{}, ErrNotFound
		}
		return domain.AnalysisRun{}, err
	}
	if err := unmarshalJSON(requestRaw, &run.RequestJSON, map[string]any{}); err != nil {
		return domain.AnalysisRun{}, err
	}
	if string(resultRaw) != "null" {
		run.ResultJSON = append([]byte(nil), resultRaw...)
	}
	if errorMessage.Valid {
		run.ErrorMessage = &errorMessage.String
	}
	return run, nil
}

// ListInFlightAnalysisRuns — silverone 2026-05-27 (Codex adversarial review
// fix-2). status='running' run 전체를 system 단위로 가져온다. startup
// reconciliation이 모두 failed로 마감한다.
func (s *PostgresStore) ListInFlightAnalysisRuns() ([]domain.AnalysisRun, error) {
	rows, err := s.db.Query(
		`SELECT run_id, thread_id, project_id::text, dataset_id::text, dataset_version_id,
		        COALESCE(user_message_id, ''), request_json, result_json, status,
		        error_message, created_at, completed_at
		 FROM analysis_runs
		 WHERE status = 'running'
		 ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.AnalysisRun, 0)
	for rows.Next() {
		var run domain.AnalysisRun
		var requestRaw, resultRaw []byte
		var errorMessage sql.NullString
		if err := rows.Scan(
			&run.RunID, &run.ThreadID, &run.ProjectID, &run.DatasetID, &run.DatasetVersionID,
			&run.UserMessageID, &requestRaw, &resultRaw, &run.Status,
			&errorMessage, &run.CreatedAt, &run.CompletedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(requestRaw, &run.RequestJSON, map[string]any{}); err != nil {
			return nil, err
		}
		if string(resultRaw) != "null" {
			run.ResultJSON = append([]byte(nil), resultRaw...)
		}
		if errorMessage.Valid {
			run.ErrorMessage = &errorMessage.String
		}
		items = append(items, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) ReplaceEmbeddingChunkIndex(datasetVersionID string, records []domain.EmbeddingIndexChunk) error {
	if strings.TrimSpace(datasetVersionID) == "" {
		return errors.New("datasetVersionID is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM embedding_index_chunks WHERE dataset_version_id = $1`, datasetVersionID); err != nil {
		return err
	}
	for _, record := range records {
		metadataJSON, marshalErr := json.Marshal(defaultMetadataMap(record.Metadata))
		if marshalErr != nil {
			err = marshalErr
			return err
		}
		vectorLiteral := pgvectorLiteral(record.Embedding)
		if _, err = tx.Exec(
			`INSERT INTO embedding_index_chunks (
				chunk_id, dataset_version_id, row_id, source_row_index, chunk_index, chunk_ref,
				embedding_model, vector_dim, embedding, metadata
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9::vector, $10::jsonb
			)
			ON CONFLICT (chunk_id) DO UPDATE
			SET dataset_version_id = EXCLUDED.dataset_version_id,
			    row_id = EXCLUDED.row_id,
			    source_row_index = EXCLUDED.source_row_index,
			    chunk_index = EXCLUDED.chunk_index,
			    chunk_ref = EXCLUDED.chunk_ref,
			    embedding_model = EXCLUDED.embedding_model,
			    vector_dim = EXCLUDED.vector_dim,
			    embedding = EXCLUDED.embedding,
			    metadata = EXCLUDED.metadata,
			    created_at = NOW()`,
			record.ChunkID,
			record.DatasetVersionID,
			nullIfEmpty(record.RowID),
			record.SourceRowIndex,
			record.ChunkIndex,
			nullIfEmpty(record.ChunkRef),
			nullIfEmpty(record.EmbeddingModel),
			record.VectorDim,
			vectorLiteral,
			string(metadataJSON),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) ensureSchema(ctx context.Context) error {
	statements := []string{
		`DO $$
		BEGIN
			CREATE EXTENSION IF NOT EXISTS vector;
		EXCEPTION
			WHEN undefined_file THEN
				RAISE NOTICE 'pgvector extension is not installed on this Postgres instance';
			WHEN insufficient_privilege THEN
				RAISE NOTICE 'pgvector extension could not be created due to insufficient privilege';
		END
		$$`,
		`CREATE TABLE IF NOT EXISTS projects (
			project_id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		// 5/6 화면기획서 B안 채택: 전역 prompts 테이블 폐기 (글로벌 prompt =
		// .md 코드 계약). 기존 운영 DB의 ``prompts`` 테이블은 *DROP은 안 함*
		// — Postgres에 빈 테이블로 잔존해도 영향 0이고, 미래 정책 변경 시
		// 데이터 복구 가능. 새 deploy에선 더 이상 ensure 안 함.
		`CREATE TABLE IF NOT EXISTS project_prompts (
			project_id UUID NOT NULL REFERENCES projects(project_id),
			version TEXT NOT NULL,
			operation TEXT NOT NULL,
			title TEXT NOT NULL,
			status TEXT NOT NULL,
			summary TEXT NOT NULL DEFAULT '',
			content TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (project_id, version, operation)
		)`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS summary TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS content TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`CREATE TABLE IF NOT EXISTS project_prompt_defaults (
			project_id UUID PRIMARY KEY REFERENCES projects(project_id),
			prepare_prompt_version TEXT,
			sentiment_prompt_version TEXT,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS prepare_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS sentiment_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS planner_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS planner_meta_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS issue_evidence_summary_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS execution_final_answer_prompt_version TEXT`,
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		// ADR-015 §C audit log. Append-only; no update or delete path.
		`CREATE TABLE IF NOT EXISTS project_prompt_changes (
			change_id TEXT PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			version TEXT NOT NULL,
			operation TEXT NOT NULL,
			action TEXT NOT NULL,
			change_reason TEXT NOT NULL,
			changed_by TEXT,
			previous_content_hash TEXT,
			new_content_hash TEXT NOT NULL,
			base_version TEXT,
			changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS project_prompt_changes_project_op_idx
			ON project_prompt_changes (project_id, operation, changed_at DESC)`,
		`CREATE TABLE IF NOT EXISTS datasets (
				dataset_id UUID PRIMARY KEY,
				project_id UUID NOT NULL REFERENCES projects(project_id),
				name TEXT NOT NULL,
				description TEXT,
				data_type TEXT NOT NULL,
				active_dataset_version_id TEXT,
				active_version_updated_at TIMESTAMPTZ,
				metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
				created_at TIMESTAMPTZ NOT NULL
			)`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS active_dataset_version_id TEXT`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS active_version_updated_at TIMESTAMPTZ`,
		// silverone 2026-05-22 (옵션 α) — dataset-level metadata 저장 컬럼.
		// 기존 row는 빈 jsonb로 초기화.
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`CREATE TABLE IF NOT EXISTS dataset_versions (
			dataset_version_id TEXT PRIMARY KEY,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id),
			project_id UUID NOT NULL REFERENCES projects(project_id),
			storage_uri TEXT NOT NULL,
			data_type TEXT NOT NULL,
			record_count BIGINT,
			metadata JSONB NOT NULL,
			profile JSONB,
			clean_status TEXT NOT NULL DEFAULT 'not_requested',
			clean_uri TEXT,
			cleaned_at TIMESTAMPTZ,
			-- silverone 2026-06-01 — β2 prepare/sentiment/embedding 15 컬럼은 CREATE TABLE 본문에서도 제거.
			-- fresh DB 깨끗하게 시작. 옛 DB는 scripts/operator/drop_dataset_versions_beta2_columns.sql 명시 실행으로 DROP.
			created_at TIMESTAMPTZ NOT NULL,
			ready_at TIMESTAMPTZ
		)`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS profile JSONB`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS clean_status TEXT NOT NULL DEFAULT 'not_requested'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS clean_uri TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS cleaned_at TIMESTAMPTZ`,
		`UPDATE dataset_versions
		  SET clean_status = COALESCE(NULLIF(metadata->>'clean_status', ''), clean_status),
		      clean_uri = COALESCE(NULLIF(metadata->>'clean_uri', ''), NULLIF(metadata->>'cleaned_ref', ''), clean_uri),
		      cleaned_at = COALESCE(cleaned_at, NULLIF(metadata->>'cleaned_at', '')::timestamptz)
		  WHERE metadata IS NOT NULL
		    AND (metadata ? 'clean_status' OR metadata ? 'clean_uri' OR metadata ? 'cleaned_ref' OR metadata ? 'cleaned_at')`,
		// silverone 2026-06-01 — β2 deprecated 15 컬럼(prepare/sentiment/embedding)
		// 의 ADD COLUMN 12 라인 + embedding_status DEFAULT ALTER 1 라인 제거.
		// 옛 운영 DB가 컬럼을 가지고 있어도 boot-time DROP은 하지 않는다 —
		// operator가 `scripts/operator/drop_dataset_versions_beta2_columns.sql`
		// 을 명시 실행해야 컬럼이 사라진다. 다만 ADD COLUMN을 그대로 두면 DROP
		// 후 control-plane restart 시 컬럼이 다시 살아나서 cleanup이 무효화되므로
		// ADD/ALTER 12+1 라인 제거. CREATE TABLE 본문에서도 15 컬럼 정의 제거
		// (fresh DB는 깨끗하게 시작). PR3/PR4/PR5 분할안은 vault audit 노트
		// `dataset_versions_beta2_columns_audit_2026-05-28.md` 참조.
		`UPDATE datasets d
		  SET active_dataset_version_id = NULL,
		      active_version_updated_at = NOW()
		  WHERE active_dataset_version_id IS NOT NULL
		    AND NOT EXISTS (
		        SELECT 1
		        FROM dataset_versions v
		        WHERE v.dataset_version_id = d.active_dataset_version_id
		          AND v.dataset_id = d.dataset_id
		          AND v.project_id = d.project_id
		    )`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1
				FROM pg_constraint
				WHERE conname = 'datasets_active_dataset_version_fk'
			) THEN
				ALTER TABLE datasets
				ADD CONSTRAINT datasets_active_dataset_version_fk
				FOREIGN KEY (active_dataset_version_id)
				REFERENCES dataset_versions(dataset_version_id)
				ON DELETE SET NULL;
			END IF;
		END
		$$`,
		`CREATE OR REPLACE FUNCTION validate_dataset_active_version()
		RETURNS trigger AS $$
		BEGIN
			IF NEW.active_dataset_version_id IS NULL THEN
				RETURN NEW;
			END IF;
			IF NOT EXISTS (
				SELECT 1
				FROM dataset_versions v
				WHERE v.dataset_version_id = NEW.active_dataset_version_id
				  AND v.dataset_id = NEW.dataset_id
				  AND v.project_id = NEW.project_id
			) THEN
				RAISE EXCEPTION 'active_dataset_version_id must reference a dataset version in the same project and dataset';
			END IF;
			RETURN NEW;
		END
		$$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS datasets_active_version_validate ON datasets`,
		`CREATE TRIGGER datasets_active_version_validate
		  BEFORE INSERT OR UPDATE OF project_id, dataset_id, active_dataset_version_id
		  ON datasets
		  FOR EACH ROW
		  EXECUTE FUNCTION validate_dataset_active_version()`,
		`CREATE INDEX IF NOT EXISTS datasets_active_dataset_version_idx
		  ON datasets(active_dataset_version_id)
		  WHERE active_dataset_version_id IS NOT NULL`,
		`CREATE TABLE IF NOT EXISTS dataset_version_artifacts (
			artifact_id TEXT PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
			dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE CASCADE,
			artifact_type TEXT NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			uri TEXT,
			format TEXT,
			model TEXT,
			prompt_version TEXT,
			summary JSONB NOT NULL DEFAULT '{}'::jsonb,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (dataset_version_id, artifact_type)
		)`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS uri TEXT`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS format TEXT`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS model TEXT`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS prompt_version TEXT`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS summary JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE dataset_version_artifacts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`CREATE INDEX IF NOT EXISTS dataset_version_artifacts_version_idx
		  ON dataset_version_artifacts(project_id, dataset_version_id)`,
		`CREATE INDEX IF NOT EXISTS dataset_version_artifacts_stage_idx
		  ON dataset_version_artifacts(project_id, dataset_version_id, stage, artifact_type)`,
		`CREATE TABLE IF NOT EXISTS dataset_build_jobs (
			job_id UUID PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id),
			dataset_version_id TEXT NOT NULL,
			build_type TEXT NOT NULL,
			status TEXT NOT NULL,
			request JSONB NOT NULL DEFAULT '{}'::jsonb,
			triggered_by TEXT,
			workflow_id TEXT,
			workflow_run_id TEXT,
			attempt INTEGER NOT NULL DEFAULT 0,
			error_message TEXT,
			last_error_type TEXT,
			created_at TIMESTAMPTZ NOT NULL,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ
		)`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS workflow_id TEXT`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS workflow_run_id TEXT`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS last_error_type TEXT`,
		// 2026-05-21 — resumed_execution_count 컬럼 제거 (δ-3 executions drop 잔재).
		// 기존 DB row의 데이터 손실은 없음 — 카운터는 의미 없는 값(항상 0).
		`ALTER TABLE dataset_build_jobs DROP COLUMN IF EXISTS resumed_execution_count`,
		`CREATE INDEX IF NOT EXISTS dataset_build_jobs_project_version_idx ON dataset_build_jobs(project_id, dataset_version_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS analysis_threads (
			thread_id TEXT PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
			dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE CASCADE,
			title TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS analysis_threads_dataset_idx
		  ON analysis_threads(project_id, dataset_id, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS analysis_runs (
			run_id TEXT PRIMARY KEY,
			thread_id TEXT NOT NULL REFERENCES analysis_threads(thread_id) ON DELETE CASCADE,
			project_id UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
			dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE CASCADE,
			user_message_id TEXT,
			request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			result_json JSONB,
			status TEXT NOT NULL,
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS analysis_runs_thread_idx
		  ON analysis_runs(project_id, thread_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS analysis_messages (
			message_id TEXT PRIMARY KEY,
			thread_id TEXT NOT NULL REFERENCES analysis_threads(thread_id) ON DELETE CASCADE,
			project_id UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
			role TEXT NOT NULL,
			content TEXT NOT NULL,
			context_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
			run_id TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS analysis_messages_thread_idx
		  ON analysis_messages(project_id, thread_id, created_at ASC)`,
		// silverone 2026-06-01 (PR2) — planner answerable=false 거절 이벤트 적재.
		// message_id UNIQUE로 중복 무시. out_of_dataset_scope는 service가 저장 안 함
		// (unsupported_skill / missing_data_or_artifact만). skill upgrade backlog 축적.
		`CREATE TABLE IF NOT EXISTS planner_rejection_events (
			event_id TEXT PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
			thread_id TEXT REFERENCES analysis_threads(thread_id) ON DELETE CASCADE,
			message_id TEXT NOT NULL UNIQUE,
			user_question TEXT NOT NULL,
			reason TEXT NOT NULL,
			message TEXT,
			capability_gap JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS planner_rejection_events_reason_idx
		  ON planner_rejection_events(reason, created_at DESC)`,
		// silverone 2026-05-27 (Codex adversarial review fix-1) — 옛 schema
		// (report_drafts / executions / skill_plans / analysis_requests) DROP을
		// boot-time ensureSchema에서 제거. 운영/감사 이력이 들어있을 수 있는
		// 테이블을 서버 기동 시점에 자동 삭제하면 rollback 불가능 + 감사 이력
		// 손실. 삭제가 필요한 경우 별도 operator-run migration으로 분리하고
		// backup / archive / rollback guide / production gate를 둔다.
		// δ-3 (5/21)에서 새 채팅형 저장 흐름(analysis_threads / analysis_messages /
		// analysis_runs)이 들어왔지만, 옛 테이블 자체는 자동으로 지우지 않는다.
		`DO $$
		BEGIN
			IF to_regtype('vector') IS NOT NULL THEN
				EXECUTE 'CREATE TABLE IF NOT EXISTS embedding_index_chunks (
					chunk_id TEXT PRIMARY KEY,
					dataset_version_id TEXT NOT NULL,
					row_id TEXT,
					source_row_index BIGINT,
					chunk_index INTEGER,
					chunk_ref TEXT,
					embedding_model TEXT,
					vector_dim INTEGER,
					embedding vector,
					metadata JSONB NOT NULL DEFAULT ''{}''::jsonb,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)';
				EXECUTE 'CREATE INDEX IF NOT EXISTS embedding_index_chunks_dataset_version_idx ON embedding_index_chunks(dataset_version_id)';
			END IF;
		END
		$$`,
		// β2 (5/19) document_cluster_profile 제거 후 남는 staging 테이블
		// (dataset_version_cluster_profile_builds / dataset_version_cluster_confirmations)
		// 정리는 boot path에서 제거됐다 (silverone 2026-06-04, Codex review #4).
		// 운영 코드는 destructive cleanup을 자동 실행하지 않는다 — 운영 데이터
		// 삭제가 발생할 수 있으므로 필요 시 operator가
		// scripts/migrations/0001_drop_legacy_cluster_tables.sql 을 1회 수동 실행한다.
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := s.promoteTimestampColumnsToTimestamptz(ctx); err != nil {
		return err
	}
	return s.backfillDatasetVersionArtifacts(ctx)
}

func (s *PostgresStore) promoteTimestampColumnsToTimestamptz(ctx context.Context) error {
	columns := []timestampColumn{
		{tableName: "projects", columnName: "created_at"},
		{tableName: "prompts", columnName: "created_at"},
		{tableName: "prompts", columnName: "updated_at"},
		{tableName: "project_prompts", columnName: "created_at"},
		{tableName: "project_prompts", columnName: "updated_at"},
		{tableName: "project_prompt_defaults", columnName: "updated_at"},
		{tableName: "datasets", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "cleaned_at"},
		// silverone 2026-06-01 (β2 cleanup PR3) — prepare/sentiment 계열 timestamp
		// 컬럼은 drop_dataset_versions_beta2_columns.sql DROP 대상. Go read/write
		// path는 이 컬럼을 더 이상 쓰지 않으므로 timestamptz 승격 목록에서도 제거한다.
		// (columnDataType는 컬럼 부재 시 graceful skip이라 깨지진 않았으나 dead ref.)
		{tableName: "dataset_versions", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "ready_at"},
		{tableName: "dataset_version_artifacts", columnName: "created_at"},
		{tableName: "dataset_version_artifacts", columnName: "updated_at"},
		{tableName: "dataset_build_jobs", columnName: "created_at"},
		{tableName: "dataset_build_jobs", columnName: "started_at"},
		{tableName: "dataset_build_jobs", columnName: "completed_at"},
	}
	for _, column := range columns {
		dataType, err := s.columnDataType(ctx, column.tableName, column.columnName)
		if err != nil {
			return err
		}
		if dataType == "" || dataType == "timestamp with time zone" {
			continue
		}
		if dataType != "timestamp without time zone" {
			return fmt.Errorf("unsupported timestamp type for %s.%s: %s", column.tableName, column.columnName, dataType)
		}
		statement := fmt.Sprintf(
			`ALTER TABLE %s ALTER COLUMN %s TYPE TIMESTAMPTZ USING %s AT TIME ZONE 'UTC'`,
			column.tableName,
			column.columnName,
			column.columnName,
		)
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) backfillDatasetVersionArtifacts(ctx context.Context) error {
	projects, err := s.ListProjects()
	if err != nil {
		return err
	}
	for _, project := range projects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		datasets, err := s.ListDatasets(project.ProjectID)
		if err != nil {
			return err
		}
		for _, dataset := range datasets {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			versions, err := s.ListDatasetVersions(project.ProjectID, dataset.DatasetID)
			if err != nil {
				return err
			}
			for _, version := range versions {
				if err := s.syncDatasetVersionArtifacts(version); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *PostgresStore) columnDataType(ctx context.Context, tableName, columnName string) (string, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT data_type
		 FROM information_schema.columns
		 WHERE table_schema = 'public'
		   AND table_name = $1
		   AND column_name = $2`,
		tableName,
		columnName,
	)
	var dataType string
	if err := row.Scan(&dataType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return dataType, nil
}

func normalizeDatabaseURL(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Replace(value, "postgresql+psycopg://", "postgresql://", 1)
	value = strings.Replace(value, "postgres+psycopg://", "postgres://", 1)
	return value
}

func marshalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func unmarshalJSON[T any](raw []byte, target *T, fallback T) error {
	if len(raw) == 0 || string(raw) == "null" {
		*target = fallback
		return nil
	}
	return json.Unmarshal(raw, target)
}

func unmarshalNullableJSON[T any](raw []byte, target **T) error {
	if len(raw) == 0 || string(raw) == "null" {
		*target = nil
		return nil
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*target = &value
	return nil
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

// nullableEmptyString maps an empty string to NULL — useful when the
// caller passes plain “string“ (not pointer) but the column should
// stay NULL when unset, e.g. ADR-015 audit fields like “changed_by“
// before auth lands.
func nullableEmptyString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullIfEmpty(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func normalizeDatasetVersionCleanFields(version domain.DatasetVersion) domain.DatasetVersion {
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	status := metadataString(version.Metadata, "clean_status")
	if status == "" {
		status = strings.TrimSpace(version.CleanStatus)
	}
	if status == "" {
		switch version.DataType {
		case "unstructured", "mixed", "both":
			status = "not_requested"
		default:
			status = "not_applicable"
		}
	}
	version.CleanStatus = status
	version.Metadata["clean_status"] = status

	cleanURI := strings.TrimSpace(derefString(version.CleanURI))
	if cleanURI == "" {
		cleanURI = strings.TrimSpace(derefString(version.CleanedRef))
	}
	if cleanURI == "" {
		cleanURI = metadataString(version.Metadata, "clean_uri")
	}
	if cleanURI == "" {
		cleanURI = metadataString(version.Metadata, "cleaned_ref")
	}
	if cleanURI != "" {
		version.CleanURI = &cleanURI
		version.CleanedRef = &cleanURI
		version.Metadata["clean_uri"] = cleanURI
		version.Metadata["cleaned_ref"] = cleanURI
	}

	if version.CleanedAt == nil {
		if cleanedAt, ok := metadataTime(version.Metadata, "cleaned_at"); ok {
			version.CleanedAt = &cleanedAt
		}
	}
	if version.CleanedAt != nil {
		version.Metadata["cleaned_at"] = *version.CleanedAt
	}
	return version
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func metadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func metadataTime(metadata map[string]any, key string) (time.Time, bool) {
	if len(metadata) == 0 {
		return time.Time{}, false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return time.Time{}, false
	}
	switch typed := value.(type) {
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false
		}
		return typed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return time.Time{}, false
		}
		parsed, err := time.Parse(time.RFC3339Nano, trimmed)
		if err != nil {
			return time.Time{}, false
		}
		return parsed, true
	default:
		return time.Time{}, false
	}
}

func defaultMetadataMap(metadata map[string]any) map[string]any {
	if metadata == nil {
		return map[string]any{}
	}
	return metadata
}

func pgvectorLiteral(values []float32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(float64(value), 'f', -1, 32))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
