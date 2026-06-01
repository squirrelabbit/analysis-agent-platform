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
	// 5/6 화면기획서 B안 채택: 전역 prompts 테이블 폐기. 글로벌 prompt는
	// .md 코드 계약. 프로젝트별 prompt만 SaveProjectPrompt 흐름.
	SaveProjectPrompt(prompt domain.ProjectPrompt) error
	GetProjectPrompt(projectID, version, operation string) (domain.ProjectPrompt, error)
	ListProjectPrompts(projectID string) ([]domain.ProjectPrompt, error)
	SaveProjectPromptDefaults(defaults domain.ProjectPromptDefaults) error
	GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error)

	// ADR-015 §C audit log. Append-only — every prompt mutation
	// (create/update/revert) emits one ProjectPromptChange row.
	AppendProjectPromptChange(change domain.ProjectPromptChange) error
	ListProjectPromptChanges(projectID, operation string) ([]domain.ProjectPromptChange, error)

	SaveDataset(dataset domain.Dataset) error
	GetDataset(projectID, datasetID string) (domain.Dataset, error)
	ListDatasets(projectID string) ([]domain.Dataset, error)
	DeleteDataset(projectID, datasetID string) error
	SaveDatasetVersion(version domain.DatasetVersion) error
	GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error)
	ListDatasetVersions(projectID, datasetID string) ([]domain.DatasetVersion, error)
	DeleteDatasetVersion(projectID, datasetID, datasetVersionID string) error
	ListDatasetVersionArtifacts(projectID, datasetVersionID string) ([]domain.DatasetVersionArtifact, error)
	SaveDatasetBuildJob(job domain.DatasetBuildJob) error
	GetDatasetBuildJob(projectID, jobID string) (domain.DatasetBuildJob, error)
	ListDatasetBuildJobs(projectID, datasetVersionID string) ([]domain.DatasetBuildJob, error)
	SaveAnalysisThread(thread domain.AnalysisThread) error
	GetAnalysisThread(projectID, datasetID, threadID string) (domain.AnalysisThread, error)
	ListAnalysisThreads(projectID, datasetID string) ([]domain.AnalysisThread, error)
	// silverone 2026-06-01 — project sidebar 채팅 count용 단일 COUNT 쿼리.
	// dataset 단위 thread list보다 가볍고 N+1 회피.
	CountAnalysisThreadsByProject(projectID string) (int, error)
	SaveAnalysisMessage(message domain.AnalysisMessage) error
	ListAnalysisMessages(projectID, threadID string) ([]domain.AnalysisMessage, error)
	SaveAnalysisRun(run domain.AnalysisRun) error
	GetAnalysisRun(projectID, runID string) (domain.AnalysisRun, error)
	// silverone 2026-05-26 (plan reuse POC-1) — thread 안에서 가장 최근
	// completed run을 가져온다. completed run이 하나도 없으면 ErrNotFound.
	// reuse classifier가 이전 plan을 patch할 때 사용.
	GetLastSuccessfulAnalysisRun(projectID, threadID string) (domain.AnalysisRun, error)

	// silverone 2026-05-27 (Codex adversarial review fix-2) — control-plane
	// 재기동 시 reconciliation에서 사용. status가 queued/running으로 남아 있는
	// in-flight row를 모두 가져온다. project_id 무관 — 전체 system 단위.
	ListInFlightDatasetBuildJobs() ([]domain.DatasetBuildJob, error)
	ListInFlightAnalysisRuns() ([]domain.AnalysisRun, error)

	// ClusterProfileBuild / ClusterConfirmation 관련 method는 β2 (5/19)
	// 결정으로 제거.
	// AnalysisRequest / PlanRecord / ExecutionSummary / ReportDraft / Scenario
	// 관련 method는 δ-2/δ-3 (5/21)에서 plan_v2 + executor_v2 + analyze_v2
	// 도입에 따라 제거.
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
