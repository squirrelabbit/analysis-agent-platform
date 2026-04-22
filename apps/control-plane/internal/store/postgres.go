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
	statements := []struct {
		query string
		args  []any
	}{
		{query: `DELETE FROM dataset_build_jobs WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM executions WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM report_drafts WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM skill_plans WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM analysis_requests WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM dataset_versions WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM datasets WHERE project_id = $1::uuid`, args: []any{projectID}},
		{query: `DELETE FROM scenarios WHERE project_id = $1::uuid`, args: []any{projectID}},
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

func (s *PostgresStore) SavePrompt(prompt domain.Prompt) error {
	_, err := s.db.Exec(
		`INSERT INTO prompts (
			prompt_id, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (prompt_id) DO UPDATE
		SET version = EXCLUDED.version,
		    operation = EXCLUDED.operation,
		    title = EXCLUDED.title,
		    status = EXCLUDED.status,
		    summary = EXCLUDED.summary,
		    content = EXCLUDED.content,
		    content_hash = EXCLUDED.content_hash,
		    updated_at = EXCLUDED.updated_at`,
		prompt.PromptID,
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

func (s *PostgresStore) GetPrompt(promptID string) (domain.Prompt, error) {
	row := s.db.QueryRow(
		`SELECT prompt_id::text, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		 FROM prompts
		 WHERE prompt_id = $1::uuid`,
		promptID,
	)

	var prompt domain.Prompt
	if err := row.Scan(
		&prompt.PromptID,
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
			return domain.Prompt{}, ErrNotFound
		}
		return domain.Prompt{}, err
	}
	return prompt, nil
}

func (s *PostgresStore) GetPromptByVersion(version, operation string) (domain.Prompt, error) {
	row := s.db.QueryRow(
		`SELECT prompt_id::text, version, operation, title, status, summary, content, content_hash, created_at, updated_at
		 FROM prompts
		 WHERE version = $1 AND operation = $2`,
		version,
		operation,
	)

	var prompt domain.Prompt
	if err := row.Scan(
		&prompt.PromptID,
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
			return domain.Prompt{}, ErrNotFound
		}
		return domain.Prompt{}, err
	}
	return prompt, nil
}

func (s *PostgresStore) ListPrompts(operation string) ([]domain.Prompt, error) {
	query := `SELECT prompt_id::text, version, operation, title, status, summary, content, content_hash, created_at, updated_at
	          FROM prompts`
	args := []any{}
	if strings.TrimSpace(operation) != "" {
		query += ` WHERE operation = $1`
		args = append(args, strings.TrimSpace(operation))
	}
	query += ` ORDER BY operation ASC, version ASC, updated_at DESC`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Prompt, 0)
	for rows.Next() {
		var prompt domain.Prompt
		if err := rows.Scan(
			&prompt.PromptID,
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

func (s *PostgresStore) DeletePrompt(promptID string) error {
	result, err := s.db.Exec(`DELETE FROM prompts WHERE prompt_id = $1::uuid`, promptID)
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
	return nil
}

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
	_, err := s.db.Exec(
		`INSERT INTO project_prompt_defaults (
			project_id, prepare_prompt_version, sentiment_prompt_version, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4
		)
		ON CONFLICT (project_id) DO UPDATE
		SET prepare_prompt_version = EXCLUDED.prepare_prompt_version,
		    sentiment_prompt_version = EXCLUDED.sentiment_prompt_version,
		    updated_at = EXCLUDED.updated_at`,
		defaults.ProjectID,
		nullableString(defaults.PreparePromptVersion),
		nullableString(defaults.SentimentPromptVersion),
		updatedAt,
	)
	return err
}

func (s *PostgresStore) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	row := s.db.QueryRow(
		`SELECT project_id::text, prepare_prompt_version, sentiment_prompt_version, updated_at
		 FROM project_prompt_defaults
		 WHERE project_id = $1::uuid`,
		projectID,
	)

	var defaults domain.ProjectPromptDefaults
	var preparePromptVersion sql.NullString
	var sentimentPromptVersion sql.NullString
	var updatedAt time.Time
	if err := row.Scan(&defaults.ProjectID, &preparePromptVersion, &sentimentPromptVersion, &updatedAt); err != nil {
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
	defaults.UpdatedAt = &updatedAt
	return defaults, nil
}

func (s *PostgresStore) SaveScenario(scenario domain.Scenario) error {
	stepsJSON, err := marshalJSON(scenario.Steps)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO scenarios (
			project_id, scenario_id, planning_mode, user_query, query_type, interpretation, analysis_scope, steps, created_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7, $8::jsonb, $9
		)
		ON CONFLICT (project_id, scenario_id) DO UPDATE
		SET planning_mode = EXCLUDED.planning_mode,
		    user_query = EXCLUDED.user_query,
		    query_type = EXCLUDED.query_type,
		    interpretation = EXCLUDED.interpretation,
		    analysis_scope = EXCLUDED.analysis_scope,
		    steps = EXCLUDED.steps`,
		scenario.ProjectID,
		scenario.ScenarioID,
		scenario.PlanningMode,
		scenario.UserQuery,
		scenario.QueryType,
		scenario.Interpretation,
		scenario.AnalysisScope,
		stepsJSON,
		scenario.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetScenario(projectID, scenarioID string) (domain.Scenario, error) {
	row := s.db.QueryRow(
		`SELECT project_id::text, scenario_id, planning_mode, user_query, query_type, interpretation, analysis_scope, steps, created_at
		 FROM scenarios
		 WHERE project_id = $1::uuid AND scenario_id = $2`,
		projectID,
		scenarioID,
	)
	var scenario domain.Scenario
	var stepsRaw []byte
	if err := row.Scan(
		&scenario.ProjectID,
		&scenario.ScenarioID,
		&scenario.PlanningMode,
		&scenario.UserQuery,
		&scenario.QueryType,
		&scenario.Interpretation,
		&scenario.AnalysisScope,
		&stepsRaw,
		&scenario.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Scenario{}, ErrNotFound
		}
		return domain.Scenario{}, err
	}
	if err := unmarshalJSON(stepsRaw, &scenario.Steps, []domain.ScenarioStep{}); err != nil {
		return domain.Scenario{}, err
	}
	return scenario, nil
}

func (s *PostgresStore) ListScenarios(projectID string) ([]domain.Scenario, error) {
	rows, err := s.db.Query(
		`SELECT project_id::text, scenario_id, planning_mode, user_query, query_type, interpretation, analysis_scope, steps, created_at
		 FROM scenarios
		 WHERE project_id = $1::uuid
		 ORDER BY created_at ASC, scenario_id ASC`,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Scenario, 0)
	for rows.Next() {
		var scenario domain.Scenario
		var stepsRaw []byte
		if err := rows.Scan(
			&scenario.ProjectID,
			&scenario.ScenarioID,
			&scenario.PlanningMode,
			&scenario.UserQuery,
			&scenario.QueryType,
			&scenario.Interpretation,
			&scenario.AnalysisScope,
			&stepsRaw,
			&scenario.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(stepsRaw, &scenario.Steps, []domain.ScenarioStep{}); err != nil {
			return nil, err
		}
		items = append(items, scenario)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) SaveDataset(dataset domain.Dataset) error {
	_, err := s.db.Exec(
		`INSERT INTO datasets (
		     dataset_id, project_id, name, description, data_type,
		     active_dataset_version_id, active_version_updated_at, created_at
		 ) VALUES (
		     $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8
		 )
		 ON CONFLICT (dataset_id) DO UPDATE
		 SET project_id = EXCLUDED.project_id,
		     name = EXCLUDED.name,
		     description = EXCLUDED.description,
		     data_type = EXCLUDED.data_type,
		     active_dataset_version_id = EXCLUDED.active_dataset_version_id,
		     active_version_updated_at = EXCLUDED.active_version_updated_at,
		     created_at = EXCLUDED.created_at`,
		dataset.DatasetID,
		dataset.ProjectID,
		dataset.Name,
		nullableString(dataset.Description),
		dataset.DataType,
		nullableString(dataset.ActiveDatasetVersionID),
		nullableTime(dataset.ActiveVersionUpdatedAt),
		dataset.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetDataset(projectID, datasetID string) (domain.Dataset, error) {
	row := s.db.QueryRow(
		`SELECT dataset_id::text, project_id::text, name, description, data_type,
		        active_dataset_version_id, active_version_updated_at, created_at
		 FROM datasets
		 WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	)

	var dataset domain.Dataset
	var description sql.NullString
	var activeDatasetVersionID sql.NullString
	if err := row.Scan(
		&dataset.DatasetID,
		&dataset.ProjectID,
		&dataset.Name,
		&description,
		&dataset.DataType,
		&activeDatasetVersionID,
		&dataset.ActiveVersionUpdatedAt,
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
	return dataset, nil
}

func (s *PostgresStore) ListDatasets(projectID string) ([]domain.Dataset, error) {
	rows, err := s.db.Query(
		`SELECT dataset_id::text, project_id::text, name, description, data_type,
		        active_dataset_version_id, active_version_updated_at, created_at
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
		if err := rows.Scan(
			&dataset.DatasetID,
			&dataset.ProjectID,
			&dataset.Name,
			&description,
			&dataset.DataType,
			&activeDatasetVersionID,
			&dataset.ActiveVersionUpdatedAt,
			&dataset.CreatedAt,
		); err != nil {
			return nil, err
		}
		if description.Valid {
			dataset.Description = &description.String
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

	_, err = s.db.Exec(
		`INSERT INTO dataset_versions (
		     dataset_version_id, dataset_id, project_id, storage_uri, data_type, record_count,
		     metadata, profile, clean_status, clean_uri, cleaned_at,
		     prepare_status, prepare_llm_mode, prepare_model, prepare_prompt_version, prepare_uri, prepared_at,
		     sentiment_status, sentiment_llm_mode, sentiment_model, sentiment_uri, sentiment_labeled_at, sentiment_prompt_version,
		     embedding_status, embedding_model, embedding_uri, created_at, ready_at
		 ) VALUES (
		     $1, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11,
		     $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
		     $26, $27, $28
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
		     prepare_status = EXCLUDED.prepare_status,
		     prepare_llm_mode = EXCLUDED.prepare_llm_mode,
		     prepare_model = EXCLUDED.prepare_model,
		     prepare_prompt_version = EXCLUDED.prepare_prompt_version,
		     prepare_uri = EXCLUDED.prepare_uri,
		     prepared_at = EXCLUDED.prepared_at,
		     sentiment_status = EXCLUDED.sentiment_status,
		     sentiment_llm_mode = EXCLUDED.sentiment_llm_mode,
		     sentiment_model = EXCLUDED.sentiment_model,
		     sentiment_uri = EXCLUDED.sentiment_uri,
		     sentiment_labeled_at = EXCLUDED.sentiment_labeled_at,
		     sentiment_prompt_version = EXCLUDED.sentiment_prompt_version,
		     embedding_status = EXCLUDED.embedding_status,
		     embedding_model = EXCLUDED.embedding_model,
		     embedding_uri = EXCLUDED.embedding_uri,
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
		version.PrepareStatus,
		version.PrepareLLMMode,
		nullableString(version.PrepareModel),
		nullableString(version.PreparePromptVer),
		nullableString(version.PrepareURI),
		nullableTime(version.PreparedAt),
		version.SentimentStatus,
		version.SentimentLLMMode,
		nullableString(version.SentimentModel),
		nullableString(version.SentimentURI),
		nullableTime(version.SentimentLabeledAt),
		nullableString(version.SentimentPromptVer),
		version.EmbeddingStatus,
		nullableString(version.EmbeddingModel),
		nullableString(version.EmbeddingURI),
		version.CreatedAt,
		nullableTime(version.ReadyAt),
	)
	return err
}

func (s *PostgresStore) GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error) {
	row := s.db.QueryRow(
		`SELECT dataset_version_id, dataset_id::text, project_id::text, storage_uri, data_type,
		        record_count, metadata, profile, clean_status, clean_uri, cleaned_at,
		        prepare_status, prepare_llm_mode, prepare_model, prepare_prompt_version,
		        prepare_uri, prepared_at, sentiment_status, sentiment_llm_mode, sentiment_model, sentiment_uri,
		        sentiment_labeled_at, sentiment_prompt_version, embedding_status, embedding_model,
		        embedding_uri, created_at, ready_at
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
	var prepareModel sql.NullString
	var preparePromptVersion sql.NullString
	var prepareURI sql.NullString
	var sentimentModel sql.NullString
	var sentimentURI sql.NullString
	var sentimentPromptVersion sql.NullString
	var embeddingModel sql.NullString
	var embeddingURI sql.NullString
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
		&version.PrepareStatus,
		&version.PrepareLLMMode,
		&prepareModel,
		&preparePromptVersion,
		&prepareURI,
		&version.PreparedAt,
		&version.SentimentStatus,
		&version.SentimentLLMMode,
		&sentimentModel,
		&sentimentURI,
		&version.SentimentLabeledAt,
		&sentimentPromptVersion,
		&version.EmbeddingStatus,
		&embeddingModel,
		&embeddingURI,
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
	if prepareModel.Valid {
		version.PrepareModel = &prepareModel.String
	}
	if preparePromptVersion.Valid {
		version.PreparePromptVer = &preparePromptVersion.String
	}
	if prepareURI.Valid {
		version.PrepareURI = &prepareURI.String
	}
	if sentimentModel.Valid {
		version.SentimentModel = &sentimentModel.String
	}
	if sentimentURI.Valid {
		version.SentimentURI = &sentimentURI.String
	}
	if sentimentPromptVersion.Valid {
		version.SentimentPromptVer = &sentimentPromptVersion.String
	}
	if embeddingModel.Valid {
		version.EmbeddingModel = &embeddingModel.String
	}
	if embeddingURI.Valid {
		version.EmbeddingURI = &embeddingURI.String
	}
	return normalizeDatasetVersionCleanFields(version), nil
}

func (s *PostgresStore) ListDatasetVersions(projectID, datasetID string) ([]domain.DatasetVersion, error) {
	rows, err := s.db.Query(
		`SELECT dataset_version_id, dataset_id::text, project_id::text, storage_uri, data_type,
		        record_count, metadata, profile, clean_status, clean_uri, cleaned_at,
		        prepare_status, prepare_llm_mode, prepare_model, prepare_prompt_version,
		        prepare_uri, prepared_at, sentiment_status, sentiment_llm_mode, sentiment_model, sentiment_uri,
		        sentiment_labeled_at, sentiment_prompt_version, embedding_status, embedding_model,
		        embedding_uri, created_at, ready_at
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
		var prepareModel sql.NullString
		var preparePromptVersion sql.NullString
		var prepareURI sql.NullString
		var sentimentModel sql.NullString
		var sentimentURI sql.NullString
		var sentimentPromptVersion sql.NullString
		var embeddingModel sql.NullString
		var embeddingURI sql.NullString
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
			&version.PrepareStatus,
			&version.PrepareLLMMode,
			&prepareModel,
			&preparePromptVersion,
			&prepareURI,
			&version.PreparedAt,
			&version.SentimentStatus,
			&version.SentimentLLMMode,
			&sentimentModel,
			&sentimentURI,
			&version.SentimentLabeledAt,
			&sentimentPromptVersion,
			&version.EmbeddingStatus,
			&embeddingModel,
			&embeddingURI,
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
		if prepareModel.Valid {
			version.PrepareModel = &prepareModel.String
		}
		if preparePromptVersion.Valid {
			version.PreparePromptVer = &preparePromptVersion.String
		}
		if prepareURI.Valid {
			version.PrepareURI = &prepareURI.String
		}
		if sentimentModel.Valid {
			version.SentimentModel = &sentimentModel.String
		}
		if sentimentURI.Valid {
			version.SentimentURI = &sentimentURI.String
		}
		if sentimentPromptVersion.Valid {
			version.SentimentPromptVer = &sentimentPromptVersion.String
		}
		if embeddingModel.Valid {
			version.EmbeddingModel = &embeddingModel.String
		}
		if embeddingURI.Valid {
			version.EmbeddingURI = &embeddingURI.String
		}
		items = append(items, normalizeDatasetVersionCleanFields(version))
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

func (s *PostgresStore) SaveDatasetBuildJob(job domain.DatasetBuildJob) error {
	requestJSON, err := marshalJSON(defaultMetadataMap(job.Request))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO dataset_build_jobs (
			job_id, project_id, dataset_id, dataset_version_id, build_type, status,
			request, triggered_by, workflow_id, workflow_run_id, attempt, error_message, last_error_type,
			resumed_execution_count, created_at, started_at, completed_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
			$7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
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
		    resumed_execution_count = EXCLUDED.resumed_execution_count,
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
		job.ResumedExecutionCount,
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
		        resumed_execution_count, created_at, started_at, completed_at
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
		&job.ResumedExecutionCount,
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
	                     resumed_execution_count, created_at, started_at, completed_at
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
			&job.ResumedExecutionCount,
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

func (s *PostgresStore) SaveRequest(request domain.AnalysisRequest) error {
	contextJSON, err := marshalJSON(request.Context)
	if err != nil {
		return err
	}
	constraintsJSON, err := marshalJSON(request.Constraints)
	if err != nil {
		return err
	}
	var requestedPlan any
	if request.RequestedPlan != nil {
		requestedPlan, err = marshalJSON(request.RequestedPlan)
		if err != nil {
			return err
		}
	}

	_, err = s.db.Exec(
		`INSERT INTO analysis_requests (
		     request_id, project_id, dataset_name, dataset_version_id, goal,
		     constraints, context, requested_plan, created_at
		 ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9)
		 ON CONFLICT (request_id) DO UPDATE
		 SET dataset_name = EXCLUDED.dataset_name,
		     dataset_version_id = EXCLUDED.dataset_version_id,
		     goal = EXCLUDED.goal,
		     constraints = EXCLUDED.constraints,
		     context = EXCLUDED.context,
		     requested_plan = EXCLUDED.requested_plan,
		     created_at = EXCLUDED.created_at`,
		request.RequestID,
		request.ProjectID,
		nullableString(request.DatasetName),
		nullableString(request.DatasetVersionID),
		request.Goal,
		constraintsJSON,
		contextJSON,
		requestedPlan,
		request.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetRequest(projectID, requestID string) (domain.AnalysisRequest, error) {
	row := s.db.QueryRow(
		`SELECT request_id::text, project_id::text, dataset_name, dataset_version_id, goal,
		        constraints, context, requested_plan, created_at
		 FROM analysis_requests
		 WHERE project_id = $1::uuid AND request_id = $2::uuid`,
		projectID,
		requestID,
	)

	var request domain.AnalysisRequest
	var datasetName sql.NullString
	var datasetVersionID sql.NullString
	var constraintsRaw []byte
	var contextRaw []byte
	var requestedPlanRaw []byte
	if err := row.Scan(
		&request.RequestID,
		&request.ProjectID,
		&datasetName,
		&datasetVersionID,
		&request.Goal,
		&constraintsRaw,
		&contextRaw,
		&requestedPlanRaw,
		&request.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AnalysisRequest{}, ErrNotFound
		}
		return domain.AnalysisRequest{}, err
	}

	if datasetName.Valid {
		request.DatasetName = &datasetName.String
	}
	if datasetVersionID.Valid {
		request.DatasetVersionID = &datasetVersionID.String
	}
	if err := unmarshalJSON(constraintsRaw, &request.Constraints, []string{}); err != nil {
		return domain.AnalysisRequest{}, err
	}
	if err := unmarshalJSON(contextRaw, &request.Context, map[string]any{}); err != nil {
		return domain.AnalysisRequest{}, err
	}
	if len(requestedPlanRaw) > 0 {
		var requestedPlan domain.SkillPlan
		if err := json.Unmarshal(requestedPlanRaw, &requestedPlan); err != nil {
			return domain.AnalysisRequest{}, err
		}
		request.RequestedPlan = &requestedPlan
	}
	return request, nil
}

func (s *PostgresStore) SavePlan(plan domain.PlanRecord) error {
	planJSON, err := marshalJSON(plan.Plan)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`INSERT INTO skill_plans (
		     plan_id, request_id, project_id, dataset_name, dataset_version_id, status,
		     planner_type, planner_model, planner_prompt_version, plan_hash, created_by,
		     approvals, plan, created_at
		 ) VALUES (
		     $1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10, NULL,
		     '[]'::jsonb, $11::jsonb, $12
		 )
		 ON CONFLICT (plan_id) DO UPDATE
		 SET request_id = EXCLUDED.request_id,
		     project_id = EXCLUDED.project_id,
		     dataset_name = EXCLUDED.dataset_name,
		     dataset_version_id = EXCLUDED.dataset_version_id,
		     status = EXCLUDED.status,
		     planner_type = EXCLUDED.planner_type,
		     planner_model = EXCLUDED.planner_model,
		     planner_prompt_version = EXCLUDED.planner_prompt_version,
		     plan_hash = EXCLUDED.plan_hash,
		     plan = EXCLUDED.plan,
		     created_at = EXCLUDED.created_at`,
		plan.PlanID,
		plan.RequestID,
		plan.ProjectID,
		plan.DatasetName,
		nullableString(plan.DatasetVersionID),
		plan.Status,
		nullableString(plan.PlannerType),
		nullableString(plan.PlannerModel),
		nullableString(plan.PlannerPromptVersion),
		nullableString(plan.PlanHash),
		planJSON,
		plan.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetPlan(projectID, planID string) (domain.PlanRecord, error) {
	row := s.db.QueryRow(
		`SELECT plan_id::text, request_id::text, project_id::text, dataset_name, dataset_version_id,
		        status, planner_type, planner_model, planner_prompt_version, plan_hash, plan, created_at
		 FROM skill_plans
		 WHERE project_id = $1::uuid AND plan_id = $2::uuid`,
		projectID,
		planID,
	)

	var plan domain.PlanRecord
	var datasetVersionID sql.NullString
	var plannerType sql.NullString
	var plannerModel sql.NullString
	var plannerPromptVersion sql.NullString
	var planHash sql.NullString
	var planRaw []byte
	if err := row.Scan(
		&plan.PlanID,
		&plan.RequestID,
		&plan.ProjectID,
		&plan.DatasetName,
		&datasetVersionID,
		&plan.Status,
		&plannerType,
		&plannerModel,
		&plannerPromptVersion,
		&planHash,
		&planRaw,
		&plan.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.PlanRecord{}, ErrNotFound
		}
		return domain.PlanRecord{}, err
	}
	if datasetVersionID.Valid {
		plan.DatasetVersionID = &datasetVersionID.String
	}
	if plannerType.Valid {
		plan.PlannerType = &plannerType.String
	}
	if plannerModel.Valid {
		plan.PlannerModel = &plannerModel.String
	}
	if plannerPromptVersion.Valid {
		plan.PlannerPromptVersion = &plannerPromptVersion.String
	}
	if planHash.Valid {
		plan.PlanHash = &planHash.String
	}
	if err := json.Unmarshal(planRaw, &plan.Plan); err != nil {
		return domain.PlanRecord{}, err
	}
	return plan, nil
}

func (s *PostgresStore) SaveExecution(execution domain.ExecutionSummary) error {
	if execution.CreatedAt.IsZero() {
		execution.CreatedAt = time.Now().UTC()
	}
	eventsJSON, err := marshalJSON(execution.Events)
	if err != nil {
		return err
	}
	requiredHashesJSON, err := marshalJSON(execution.RequiredHashes)
	if err != nil {
		return err
	}
	artifactsJSON, err := marshalJSON(execution.Artifacts)
	if err != nil {
		return err
	}
	resultV1SnapshotJSON, err := marshalJSON(execution.ResultV1Snapshot)
	if err != nil {
		return err
	}
	finalAnswerSnapshotJSON, err := marshalJSON(execution.FinalAnswerSnapshot)
	if err != nil {
		return err
	}
	profileSnapshotJSON, err := marshalJSON(execution.ProfileSnapshot)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`INSERT INTO executions (
		     execution_id, project_id, plan_id, status, ended_at, embedding_model_version,
		     required_hashes, artifacts, dataset_version_id, code_version, params_hash,
		     skill_bundle_version, events, result_v1_snapshot, final_answer_snapshot, final_answer_prompt_version,
		     final_answer_error, profile_snapshot, created_at
		 ) VALUES (
		     $1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15::jsonb, $16, $17, $18::jsonb, $19
		 )
		 ON CONFLICT (execution_id) DO UPDATE
		 SET status = EXCLUDED.status,
		     ended_at = EXCLUDED.ended_at,
		     embedding_model_version = EXCLUDED.embedding_model_version,
		     required_hashes = EXCLUDED.required_hashes,
		     artifacts = EXCLUDED.artifacts,
		     dataset_version_id = EXCLUDED.dataset_version_id,
		     code_version = EXCLUDED.code_version,
		     params_hash = EXCLUDED.params_hash,
		     skill_bundle_version = EXCLUDED.skill_bundle_version,
		     events = EXCLUDED.events,
		     result_v1_snapshot = EXCLUDED.result_v1_snapshot,
		     final_answer_snapshot = EXCLUDED.final_answer_snapshot,
		     final_answer_prompt_version = EXCLUDED.final_answer_prompt_version,
		     final_answer_error = EXCLUDED.final_answer_error,
		     profile_snapshot = EXCLUDED.profile_snapshot`,
		execution.ExecutionID,
		execution.ProjectID,
		execution.Plan.PlanID,
		execution.Status,
		nullableTime(execution.EndedAt),
		nullableString(execution.EmbeddingModel),
		requiredHashesJSON,
		artifactsJSON,
		nullableString(execution.DatasetVersionID),
		nullableString(execution.CodeVersion),
		nullableString(execution.ParamsHash),
		nullableString(execution.SkillBundleVersion),
		eventsJSON,
		resultV1SnapshotJSON,
		finalAnswerSnapshotJSON,
		nullableString(execution.FinalAnswerPromptVersion),
		nullableString(execution.FinalAnswerError),
		profileSnapshotJSON,
		execution.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetExecution(projectID, executionID string) (domain.ExecutionSummary, error) {
	row := s.db.QueryRow(
		`SELECT e.execution_id::text, e.project_id::text, p.request_id::text, p.plan,
		        e.status, e.ended_at, e.required_hashes, e.embedding_model_version, e.artifacts,
		        e.dataset_version_id, e.code_version, e.params_hash, e.skill_bundle_version, e.events,
		        e.created_at,
		        e.result_v1_snapshot, e.final_answer_snapshot, e.final_answer_prompt_version,
		        e.final_answer_error, e.profile_snapshot
		 FROM executions e
		 JOIN skill_plans p ON p.plan_id = e.plan_id
		 WHERE e.project_id = $1::uuid AND e.execution_id = $2::uuid`,
		projectID,
		executionID,
	)

	var execution domain.ExecutionSummary
	var planRaw []byte
	var requiredHashesRaw []byte
	var embeddingModel sql.NullString
	var artifactsRaw []byte
	var datasetVersionID sql.NullString
	var codeVersion sql.NullString
	var paramsHash sql.NullString
	var skillBundleVersion sql.NullString
	var eventsRaw []byte
	var resultV1SnapshotRaw []byte
	var finalAnswerSnapshotRaw []byte
	var finalAnswerPromptVersion sql.NullString
	var finalAnswerError sql.NullString
	var profileSnapshotRaw []byte
	if err := row.Scan(
		&execution.ExecutionID,
		&execution.ProjectID,
		&execution.RequestID,
		&planRaw,
		&execution.Status,
		&execution.EndedAt,
		&requiredHashesRaw,
		&embeddingModel,
		&artifactsRaw,
		&datasetVersionID,
		&codeVersion,
		&paramsHash,
		&skillBundleVersion,
		&eventsRaw,
		&execution.CreatedAt,
		&resultV1SnapshotRaw,
		&finalAnswerSnapshotRaw,
		&finalAnswerPromptVersion,
		&finalAnswerError,
		&profileSnapshotRaw,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExecutionSummary{}, ErrNotFound
		}
		return domain.ExecutionSummary{}, err
	}

	if err := json.Unmarshal(planRaw, &execution.Plan); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(requiredHashesRaw, &execution.RequiredHashes, []string{}); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(artifactsRaw, &execution.Artifacts, map[string]string{}); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(eventsRaw, &execution.Events, []domain.ExecutionEvent{}); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(resultV1SnapshotRaw, &execution.ResultV1Snapshot, (*domain.ExecutionResultV1)(nil)); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(finalAnswerSnapshotRaw, &execution.FinalAnswerSnapshot, (*domain.ExecutionFinalAnswer)(nil)); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if err := unmarshalJSON(profileSnapshotRaw, &execution.ProfileSnapshot, (*domain.DatasetProfile)(nil)); err != nil {
		return domain.ExecutionSummary{}, err
	}
	if embeddingModel.Valid {
		execution.EmbeddingModel = &embeddingModel.String
	}
	if datasetVersionID.Valid {
		execution.DatasetVersionID = &datasetVersionID.String
	}
	if codeVersion.Valid {
		execution.CodeVersion = &codeVersion.String
	}
	if paramsHash.Valid {
		execution.ParamsHash = &paramsHash.String
	}
	if skillBundleVersion.Valid {
		execution.SkillBundleVersion = &skillBundleVersion.String
	}
	if finalAnswerPromptVersion.Valid {
		execution.FinalAnswerPromptVersion = &finalAnswerPromptVersion.String
	}
	if finalAnswerError.Valid {
		execution.FinalAnswerError = &finalAnswerError.String
	}
	return execution, nil
}

func (s *PostgresStore) ListExecutions(projectID string) ([]domain.ExecutionSummary, error) {
	rows, err := s.db.Query(
		`SELECT execution_id::text, project_id::text, status, created_at, ended_at, dataset_version_id,
		        result_v1_snapshot, final_answer_snapshot, final_answer_error
		 FROM executions
		 WHERE project_id = $1::uuid
		 ORDER BY created_at DESC`,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.ExecutionSummary, 0)
	for rows.Next() {
		var execution domain.ExecutionSummary
		var datasetVersionID sql.NullString
		var resultV1SnapshotRaw []byte
		var finalAnswerSnapshotRaw []byte
		var finalAnswerError sql.NullString
		if err := rows.Scan(
			&execution.ExecutionID,
			&execution.ProjectID,
			&execution.Status,
			&execution.CreatedAt,
			&execution.EndedAt,
			&datasetVersionID,
			&resultV1SnapshotRaw,
			&finalAnswerSnapshotRaw,
			&finalAnswerError,
		); err != nil {
			return nil, err
		}
		if datasetVersionID.Valid {
			execution.DatasetVersionID = &datasetVersionID.String
		}
		if err := unmarshalJSON(resultV1SnapshotRaw, &execution.ResultV1Snapshot, (*domain.ExecutionResultV1)(nil)); err != nil {
			return nil, err
		}
		if err := unmarshalJSON(finalAnswerSnapshotRaw, &execution.FinalAnswerSnapshot, (*domain.ExecutionFinalAnswer)(nil)); err != nil {
			return nil, err
		}
		if finalAnswerError.Valid {
			execution.FinalAnswerError = &finalAnswerError.String
		}
		items = append(items, execution)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *PostgresStore) SaveReportDraft(draft domain.ReportDraft) error {
	executionIDsJSON, err := marshalJSON(draft.ExecutionIDs)
	if err != nil {
		return err
	}
	contentJSON, err := marshalJSON(draft.Content)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO report_drafts (
			draft_id, project_id, title, execution_ids, content, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4::jsonb, $5::jsonb, $6
		)
		ON CONFLICT (draft_id) DO UPDATE
		SET title = EXCLUDED.title,
		    execution_ids = EXCLUDED.execution_ids,
		    content = EXCLUDED.content`,
		draft.DraftID,
		draft.ProjectID,
		draft.Title,
		executionIDsJSON,
		contentJSON,
		draft.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetReportDraft(projectID, draftID string) (domain.ReportDraft, error) {
	row := s.db.QueryRow(
		`SELECT draft_id::text, project_id::text, title, execution_ids, content, created_at
		 FROM report_drafts
		 WHERE project_id = $1::uuid AND draft_id = $2::uuid`,
		projectID,
		draftID,
	)
	var draft domain.ReportDraft
	var executionIDsRaw []byte
	var contentRaw []byte
	if err := row.Scan(
		&draft.DraftID,
		&draft.ProjectID,
		&draft.Title,
		&executionIDsRaw,
		&contentRaw,
		&draft.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ReportDraft{}, ErrNotFound
		}
		return domain.ReportDraft{}, err
	}
	if err := unmarshalJSON(executionIDsRaw, &draft.ExecutionIDs, []string{}); err != nil {
		return domain.ReportDraft{}, err
	}
	if err := unmarshalJSON(contentRaw, &draft.Content, domain.ReportDraftV1{}); err != nil {
		return domain.ReportDraft{}, err
	}
	return draft, nil
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
		`CREATE TABLE IF NOT EXISTS prompts (
			prompt_id UUID PRIMARY KEY,
			version TEXT NOT NULL,
			operation TEXT NOT NULL,
			title TEXT NOT NULL,
			status TEXT NOT NULL,
			summary TEXT NOT NULL DEFAULT '',
			content TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			UNIQUE (version, operation)
		)`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS summary TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS content TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE prompts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
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
		`ALTER TABLE project_prompt_defaults ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`CREATE TABLE IF NOT EXISTS scenarios (
			project_id UUID NOT NULL REFERENCES projects(project_id),
			scenario_id TEXT NOT NULL,
			planning_mode TEXT NOT NULL DEFAULT 'strict',
			user_query TEXT NOT NULL,
			query_type TEXT NOT NULL,
			interpretation TEXT NOT NULL,
			analysis_scope TEXT NOT NULL,
			steps JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (project_id, scenario_id)
		)`,
		`ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS planning_mode TEXT NOT NULL DEFAULT 'strict'`,
		`CREATE TABLE IF NOT EXISTS datasets (
				dataset_id UUID PRIMARY KEY,
				project_id UUID NOT NULL REFERENCES projects(project_id),
				name TEXT NOT NULL,
				description TEXT,
				data_type TEXT NOT NULL,
				active_dataset_version_id TEXT,
				active_version_updated_at TIMESTAMPTZ,
				created_at TIMESTAMPTZ NOT NULL
			)`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS active_dataset_version_id TEXT`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS active_version_updated_at TIMESTAMPTZ`,
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
			prepare_status TEXT NOT NULL DEFAULT 'not_requested',
			prepare_llm_mode TEXT NOT NULL DEFAULT 'default',
			prepare_model TEXT,
			prepare_prompt_version TEXT,
			prepare_uri TEXT,
			prepared_at TIMESTAMPTZ,
			sentiment_status TEXT NOT NULL DEFAULT 'not_requested',
			sentiment_llm_mode TEXT NOT NULL DEFAULT 'default',
			sentiment_model TEXT,
			sentiment_uri TEXT,
			sentiment_labeled_at TIMESTAMPTZ,
			sentiment_prompt_version TEXT,
			embedding_status TEXT NOT NULL,
			embedding_model TEXT,
			embedding_uri TEXT,
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
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_status TEXT NOT NULL DEFAULT 'not_requested'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_llm_mode TEXT NOT NULL DEFAULT 'default'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_model TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_prompt_version TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_uri TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepared_at TIMESTAMPTZ`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_status TEXT NOT NULL DEFAULT 'not_requested'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_llm_mode TEXT NOT NULL DEFAULT 'default'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_model TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_uri TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_labeled_at TIMESTAMPTZ`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_prompt_version TEXT`,
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
			resumed_execution_count INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ
		)`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS workflow_id TEXT`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS workflow_run_id TEXT`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS last_error_type TEXT`,
		`ALTER TABLE dataset_build_jobs ADD COLUMN IF NOT EXISTS resumed_execution_count INTEGER NOT NULL DEFAULT 0`,
		`CREATE INDEX IF NOT EXISTS dataset_build_jobs_project_version_idx ON dataset_build_jobs(project_id, dataset_version_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS analysis_requests (
			request_id UUID PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			dataset_name TEXT,
			dataset_version_id TEXT,
			goal TEXT NOT NULL,
			constraints JSONB NOT NULL,
			context JSONB NOT NULL,
			requested_plan JSONB,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS skill_plans (
			plan_id UUID PRIMARY KEY,
			request_id UUID,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			dataset_name TEXT,
			dataset_version_id TEXT,
			status TEXT NOT NULL,
			planner_type TEXT,
			planner_model TEXT,
			planner_prompt_version TEXT,
			plan_hash TEXT,
			created_by TEXT,
			approvals JSONB,
			plan JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS executions (
			execution_id UUID PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			plan_id UUID NOT NULL REFERENCES skill_plans(plan_id),
			status TEXT NOT NULL,
			ended_at TIMESTAMPTZ,
			embedding_model_version TEXT,
			required_hashes JSONB,
			artifacts JSONB,
			dataset_version_id TEXT,
			code_version TEXT,
			params_hash TEXT,
			skill_bundle_version TEXT,
			result_v1_snapshot JSONB,
			final_answer_snapshot JSONB,
			final_answer_prompt_version TEXT,
			final_answer_error TEXT,
			profile_snapshot JSONB,
			events JSONB,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS result_v1_snapshot JSONB`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS final_answer_snapshot JSONB`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS final_answer_prompt_version TEXT`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS final_answer_error TEXT`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS profile_snapshot JSONB`,
		`CREATE TABLE IF NOT EXISTS report_drafts (
			draft_id UUID PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			title TEXT NOT NULL,
			execution_ids JSONB NOT NULL,
			content JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		)`,
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
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return s.promoteTimestampColumnsToTimestamptz(ctx)
}

func (s *PostgresStore) promoteTimestampColumnsToTimestamptz(ctx context.Context) error {
	columns := []timestampColumn{
		{tableName: "projects", columnName: "created_at"},
		{tableName: "prompts", columnName: "created_at"},
		{tableName: "prompts", columnName: "updated_at"},
		{tableName: "project_prompts", columnName: "created_at"},
		{tableName: "project_prompts", columnName: "updated_at"},
		{tableName: "project_prompt_defaults", columnName: "updated_at"},
		{tableName: "scenarios", columnName: "created_at"},
		{tableName: "datasets", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "cleaned_at"},
		{tableName: "dataset_versions", columnName: "prepared_at"},
		{tableName: "dataset_versions", columnName: "sentiment_labeled_at"},
		{tableName: "dataset_versions", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "ready_at"},
		{tableName: "dataset_build_jobs", columnName: "created_at"},
		{tableName: "dataset_build_jobs", columnName: "started_at"},
		{tableName: "dataset_build_jobs", columnName: "completed_at"},
		{tableName: "analysis_requests", columnName: "created_at"},
		{tableName: "skill_plans", columnName: "created_at"},
		{tableName: "executions", columnName: "ended_at"},
		{tableName: "executions", columnName: "created_at"},
		{tableName: "report_drafts", columnName: "created_at"},
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
