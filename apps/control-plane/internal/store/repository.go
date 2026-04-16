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
	ListProjects() ([]domain.Project, error)
	DeleteProject(projectID string) error
	SaveProjectPrompt(prompt domain.ProjectPrompt) error
	GetProjectPrompt(projectID, version, operation string) (domain.ProjectPrompt, error)
	ListProjectPrompts(projectID string) ([]domain.ProjectPrompt, error)
	SaveProjectPromptDefaults(defaults domain.ProjectPromptDefaults) error
	GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error)
	SaveScenario(scenario domain.Scenario) error
	GetScenario(projectID, scenarioID string) (domain.Scenario, error)
	ListScenarios(projectID string) ([]domain.Scenario, error)
	SaveDataset(dataset domain.Dataset) error
	GetDataset(projectID, datasetID string) (domain.Dataset, error)
	ListDatasets(projectID string) ([]domain.Dataset, error)
	DeleteDataset(projectID, datasetID string) error
	SaveDatasetVersion(version domain.DatasetVersion) error
	GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error)
	ListDatasetVersions(projectID, datasetID string) ([]domain.DatasetVersion, error)
	SaveDatasetBuildJob(job domain.DatasetBuildJob) error
	GetDatasetBuildJob(projectID, jobID string) (domain.DatasetBuildJob, error)
	ListDatasetBuildJobs(projectID, datasetVersionID string) ([]domain.DatasetBuildJob, error)
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
