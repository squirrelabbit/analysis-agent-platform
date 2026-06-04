package service

import (
	"context"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// AnalysisThreadService — analysis thread/run/message 책임을 DatasetService에서
// 분리한 sub-service (service_decomposition.md 2순위). silverone 2026-06-04.
//
// DatasetService는 facade로 유지되어 핸들러/기존 호출자가 그대로
// `DatasetService.AnalyzeDatasetAsNewThread(...)` 등을 부른다. 내부는
// AnalysisThreadService에 위임한다 (public API·동작 불변). thread 로직 본문은
// analysis_threads.go / plan_reuse.go에 두되 receiver를 *AnalysisThreadService로
// 둔다.
//
// 의존은 단방향이다: AnalysisThreadService → (store + threadServiceDeps).
// threadServiceDeps는 dataset/version 조회 + analyze 실행을 묶은 최소 인터페이스로,
// DatasetService(AnalyzeService facade 포함)가 구현한다 → thread는 analyze에
// 의존하지만 역방향은 없다.

// threadServiceDeps — AnalysisThreadService가 의존하는 외부 기능의 최소 집합.
// DatasetService가 이를 구현한다(GetDataset/GetDatasetVersion은 core,
// ExecuteAnalyze*는 AnalyzeService facade).
type threadServiceDeps interface {
	GetDataset(projectID, datasetID string) (domain.Dataset, error)
	GetDatasetVersion(projectID, datasetID, versionID string) (domain.DatasetVersion, error)
	ExecuteAnalyze(ctx context.Context, projectID, datasetID, versionID string, req AnalyzeRequest) (AnalyzeResponse, error)
	ExecuteAnalyzeOnActiveVersion(ctx context.Context, projectID, datasetID string, req AnalyzeRequest) (AnalyzeResponse, error)
}

type AnalysisThreadService struct {
	store store.Repository
	deps  threadServiceDeps
}

func NewAnalysisThreadService(repo store.Repository, deps threadServiceDeps) *AnalysisThreadService {
	return &AnalysisThreadService{store: repo, deps: deps}
}

// threads — DatasetService가 자신을 deps로 넘겨 AnalysisThreadService를 구성한다.
func (s *DatasetService) threads() *AnalysisThreadService {
	return NewAnalysisThreadService(s.store, s)
}

// ===== facade (public API 유지 — AnalysisThreadService로 위임) =====

func (s *DatasetService) CreateAnalysisThread(projectID, datasetID string, input domain.AnalysisThreadCreateRequest) (domain.AnalysisThread, error) {
	return s.threads().CreateAnalysisThread(projectID, datasetID, input)
}

func (s *DatasetService) ListAnalysisThreads(projectID, datasetID string) (domain.AnalysisThreadListResponse, error) {
	return s.threads().ListAnalysisThreads(projectID, datasetID)
}

func (s *DatasetService) GetAnalysisThread(projectID, datasetID, threadID string) (domain.AnalysisThreadDetail, error) {
	return s.threads().GetAnalysisThread(projectID, datasetID, threadID)
}

func (s *DatasetService) DeleteAnalysisThread(projectID, datasetID, threadID string) error {
	return s.threads().DeleteAnalysisThread(projectID, datasetID, threadID)
}

func (s *DatasetService) GetAnalysisRun(projectID, datasetID, runID string) (domain.AnalysisRun, error) {
	return s.threads().GetAnalysisRun(projectID, datasetID, runID)
}

func (s *DatasetService) AnalyzeDatasetAsNewThread(ctx context.Context, projectID, datasetID string, req AnalyzeRequest) (domain.AnalysisThreadMessageResponse, error) {
	return s.threads().AnalyzeDatasetAsNewThread(ctx, projectID, datasetID, req)
}

func (s *DatasetService) PostAnalysisThreadMessage(ctx context.Context, projectID, datasetID, threadID string, input domain.AnalysisThreadMessageRequest) (domain.AnalysisThreadMessageResponse, error) {
	return s.threads().PostAnalysisThreadMessage(ctx, projectID, datasetID, threadID, input)
}
