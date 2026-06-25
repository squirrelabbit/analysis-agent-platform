package store

import (
	"errors"
	"time"

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
	// silverone 2026-06-01 — 단건 thread 삭제 (테스트 정리). project_id+dataset_id+
	// thread_id가 모두 일치하는 row만 삭제. 일치 row 없으면 ErrNotFound.
	// messages/runs/rejection_events는 FK ON DELETE CASCADE에 위임.
	DeleteAnalysisThread(projectID, datasetID, threadID string) error
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

	// silverone 2026-06-01 (PR2) — planner가 answerable=false로 거절한 이벤트 적재.
	// message_id UNIQUE로 중복 무시(idempotent). skill upgrade backlog 축적용.
	SaveRejectionEvent(event domain.PlannerRejectionEvent) error

	// silverone 2026-06-10 — 보고서 보관함. 분석 결과 스냅샷 저장/조회/삭제.
	// ListReportSavedResults는 datasetID가 빈 문자열이면 project 전체를 반환한다.
	SaveReportSavedResult(result domain.ReportSavedResult) error
	ListReportSavedResults(projectID, datasetID string) ([]domain.ReportSavedResult, error)
	GetReportSavedResult(projectID, resultID string) (domain.ReportSavedResult, error)
	DeleteReportSavedResult(projectID, resultID string) error

	// silverone 2026-06-11 — 보고서 문서 CRUD. CreateReport는 INSERT,
	// UpdateReport는 UPDATE(없으면 ErrNotFound). ListReports는 경량 summary.
	CreateReport(report domain.Report) error
	UpdateReport(report domain.Report) error
	ListReports(projectID string) ([]domain.ReportSummary, error)
	GetReport(projectID, reportID string) (domain.Report, error)
	DeleteReport(projectID, reportID string) error

	// silverone 2026-06-11 — 진성 라벨 수동 보정 overlay. (version, doc) upsert,
	// ListByVersion은 진성 GET overlay·summary 재집계용.
	UpsertDocGenuinenessOverride(override domain.DocGenuinenessOverride) error
	DeleteDocGenuinenessOverride(projectID, datasetVersionID, docID string) error
	ListDocGenuinenessOverrides(projectID, datasetVersionID string) ([]domain.DocGenuinenessOverride, error)

	// silverone 2026-06-11 — 절 라벨링 aspect/sentiment 수동 보정 overlay.
	// (version, clause_id) upsert, ListByVersion은 절 라벨링 GET overlay·summary 재집계용.
	UpsertClauseLabelOverride(override domain.ClauseLabelOverride) error
	DeleteClauseLabelOverride(projectID, datasetVersionID, clauseID string) error
	ListClauseLabelOverrides(projectID, datasetVersionID string) ([]domain.ClauseLabelOverride, error)

	// 키워드 정제 사전 (silverone 2026-06-25). dataset 단위 rule(현재 상태, soft
	// delete=active) + append-only event(감사). 키워드 뷰 overlay에서 ListRules로
	// 활성 규칙을 읽어 block 제외/synonym 병합을 적용한다.
	UpsertKeywordDictionaryRule(rule domain.KeywordDictionaryRule) error
	SetKeywordDictionaryRuleActive(projectID, datasetID, ruleID string, active bool, updatedAt time.Time) error
	GetKeywordDictionaryRule(projectID, datasetID, ruleID string) (domain.KeywordDictionaryRule, error)
	ListKeywordDictionaryRules(projectID, datasetID string, activeOnly bool) ([]domain.KeywordDictionaryRule, error)
	AppendKeywordDictionaryEvent(event domain.KeywordDictionaryEvent) error
	ListKeywordDictionaryEvents(projectID, datasetID string) ([]domain.KeywordDictionaryEvent, error)

	// 인증/RBAC (ADR-025, silverone 2026-06-12). Google OIDC = 인증,
	// project_members = 권한. UpsertUserByExternal는 (auth_provider, external_id)
	// 기준 upsert(첫 로그인 가입 + 재로그인 갱신).
	UpsertUserByExternal(user domain.User) (domain.User, error)
	GetUserByID(userID string) (domain.User, error)
	CreateSession(session domain.Session) error
	GetSessionByTokenHash(tokenHash string) (domain.Session, error)
	TouchSession(sessionID string, lastSeen time.Time) error
	DeleteSession(sessionID string) error
	ListProjectRolesForUser(userID string) (map[string]string, error)
	GetProjectRole(projectID, userID string) (string, error)
	UpsertProjectMember(member domain.ProjectMember) error
	DeleteProjectMember(projectID, userID string) error
	ListProjectMembers(projectID string) ([]domain.ProjectMember, error)

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
