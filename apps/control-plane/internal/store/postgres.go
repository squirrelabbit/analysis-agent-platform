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

func (s *PostgresStore) SaveDataset(dataset domain.Dataset) error {
	_, err := s.db.Exec(
		`INSERT INTO datasets (dataset_id, project_id, name, description, data_type, created_at)
		 VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
		 ON CONFLICT (dataset_id) DO UPDATE
		 SET project_id = EXCLUDED.project_id,
		     name = EXCLUDED.name,
		     description = EXCLUDED.description,
		     data_type = EXCLUDED.data_type,
		     created_at = EXCLUDED.created_at`,
		dataset.DatasetID,
		dataset.ProjectID,
		dataset.Name,
		nullableString(dataset.Description),
		dataset.DataType,
		dataset.CreatedAt,
	)
	return err
}

func (s *PostgresStore) GetDataset(projectID, datasetID string) (domain.Dataset, error) {
	row := s.db.QueryRow(
		`SELECT dataset_id::text, project_id::text, name, description, data_type, created_at
		 FROM datasets
		 WHERE project_id = $1::uuid AND dataset_id = $2::uuid`,
		projectID,
		datasetID,
	)

	var dataset domain.Dataset
	var description sql.NullString
	if err := row.Scan(
		&dataset.DatasetID,
		&dataset.ProjectID,
		&dataset.Name,
		&description,
		&dataset.DataType,
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
	return dataset, nil
}

func (s *PostgresStore) SaveDatasetVersion(version domain.DatasetVersion) error {
	metadataJSON, err := marshalJSON(version.Metadata)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`INSERT INTO dataset_versions (
		     dataset_version_id, dataset_id, project_id, storage_uri, data_type, record_count,
		     metadata, prepare_status, prepare_model, prepare_prompt_version, prepare_uri, prepared_at,
		     sentiment_status, sentiment_model, sentiment_uri, sentiment_labeled_at, sentiment_prompt_version,
		     embedding_status, embedding_model, embedding_uri, created_at, ready_at
		 ) VALUES (
		     $1, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		 )
		 ON CONFLICT (dataset_version_id) DO UPDATE
		 SET dataset_id = EXCLUDED.dataset_id,
		     project_id = EXCLUDED.project_id,
		     storage_uri = EXCLUDED.storage_uri,
		     data_type = EXCLUDED.data_type,
		     record_count = EXCLUDED.record_count,
		     metadata = EXCLUDED.metadata,
		     prepare_status = EXCLUDED.prepare_status,
		     prepare_model = EXCLUDED.prepare_model,
		     prepare_prompt_version = EXCLUDED.prepare_prompt_version,
		     prepare_uri = EXCLUDED.prepare_uri,
		     prepared_at = EXCLUDED.prepared_at,
		     sentiment_status = EXCLUDED.sentiment_status,
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
		version.PrepareStatus,
		nullableString(version.PrepareModel),
		nullableString(version.PreparePromptVer),
		nullableString(version.PrepareURI),
		nullableTime(version.PreparedAt),
		version.SentimentStatus,
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
		        record_count, metadata, prepare_status, prepare_model, prepare_prompt_version,
		        prepare_uri, prepared_at, sentiment_status, sentiment_model, sentiment_uri,
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
		&version.PrepareStatus,
		&prepareModel,
		&preparePromptVersion,
		&prepareURI,
		&version.PreparedAt,
		&version.SentimentStatus,
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
	if recordCount.Valid {
		value := int(recordCount.Int64)
		version.RecordCount = &value
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
	return version, nil
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

	_, err = s.db.Exec(
		`INSERT INTO executions (
		     execution_id, project_id, plan_id, status, ended_at, embedding_model_version,
		     required_hashes, artifacts, dataset_version_id, code_version, params_hash,
		     skill_bundle_version, events, result_v1_snapshot, created_at
		 ) VALUES (
		     $1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13::jsonb, $14::jsonb, NOW()
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
		     result_v1_snapshot = EXCLUDED.result_v1_snapshot`,
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
	)
	return err
}

func (s *PostgresStore) GetExecution(projectID, executionID string) (domain.ExecutionSummary, error) {
	row := s.db.QueryRow(
		`SELECT e.execution_id::text, e.project_id::text, p.request_id::text, p.plan,
		        e.status, e.ended_at, e.required_hashes, e.embedding_model_version, e.artifacts,
		        e.dataset_version_id, e.code_version, e.params_hash, e.skill_bundle_version, e.events,
		        e.result_v1_snapshot
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
		&resultV1SnapshotRaw,
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
	return execution, nil
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
		`CREATE TABLE IF NOT EXISTS datasets (
			dataset_id UUID PRIMARY KEY,
			project_id UUID NOT NULL REFERENCES projects(project_id),
			name TEXT NOT NULL,
			description TEXT,
			data_type TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS dataset_versions (
			dataset_version_id TEXT PRIMARY KEY,
			dataset_id UUID NOT NULL REFERENCES datasets(dataset_id),
			project_id UUID NOT NULL REFERENCES projects(project_id),
			storage_uri TEXT NOT NULL,
			data_type TEXT NOT NULL,
			record_count BIGINT,
			metadata JSONB NOT NULL,
			prepare_status TEXT NOT NULL DEFAULT 'not_requested',
			prepare_model TEXT,
			prepare_prompt_version TEXT,
			prepare_uri TEXT,
			prepared_at TIMESTAMPTZ,
			sentiment_status TEXT NOT NULL DEFAULT 'not_requested',
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
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_status TEXT NOT NULL DEFAULT 'not_requested'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_model TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_prompt_version TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepare_uri TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS prepared_at TIMESTAMPTZ`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_status TEXT NOT NULL DEFAULT 'not_requested'`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_model TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_uri TEXT`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_labeled_at TIMESTAMPTZ`,
		`ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS sentiment_prompt_version TEXT`,
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
			events JSONB,
			created_at TIMESTAMPTZ NOT NULL
		)`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS result_v1_snapshot JSONB`,
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
		{tableName: "datasets", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "prepared_at"},
		{tableName: "dataset_versions", columnName: "sentiment_labeled_at"},
		{tableName: "dataset_versions", columnName: "created_at"},
		{tableName: "dataset_versions", columnName: "ready_at"},
		{tableName: "analysis_requests", columnName: "created_at"},
		{tableName: "skill_plans", columnName: "created_at"},
		{tableName: "executions", columnName: "ended_at"},
		{tableName: "executions", columnName: "created_at"},
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
