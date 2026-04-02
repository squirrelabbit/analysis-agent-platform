package store

import (
	"errors"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/domain"
)

var ErrNotFound = errors.New("not found")
var ErrNotImplemented = errors.New("not implemented")

type Repository interface {
	SaveProject(project domain.Project) error
	GetProject(projectID string) (domain.Project, error)
	SaveDataset(dataset domain.Dataset) error
	GetDataset(projectID, datasetID string) (domain.Dataset, error)
	SaveDatasetVersion(version domain.DatasetVersion) error
	GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error)
	SaveRequest(request domain.AnalysisRequest) error
	GetRequest(projectID, requestID string) (domain.AnalysisRequest, error)
	SavePlan(plan domain.PlanRecord) error
	GetPlan(projectID, planID string) (domain.PlanRecord, error)
	SaveExecution(execution domain.ExecutionSummary) error
	GetExecution(projectID, executionID string) (domain.ExecutionSummary, error)
	ListExecutions(projectID string) ([]domain.ExecutionSummary, error)
	SaveReportDraft(draft domain.ReportDraft) error
	GetReportDraft(projectID, draftID string) (domain.ReportDraft, error)
}

type EmbeddingChunkIndexer interface {
	ReplaceEmbeddingChunkIndex(datasetVersionID string, records []domain.EmbeddingIndexChunk) error
}

func NewRepository(cfg config.Config) (Repository, error) {
	switch cfg.StoreBackend {
	case "", "memory":
		return NewMemoryStore(), nil
	case "postgres":
		return NewPostgresStore(cfg.DatabaseURL)
	default:
		return nil, errors.New("unsupported store backend: " + cfg.StoreBackend)
	}
}
