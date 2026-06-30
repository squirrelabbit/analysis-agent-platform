package service

import (
	"context"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// AnalyzeService — analyze 실행 책임을 DatasetService에서 분리한 sub-service
// (service_decomposition.md 1순위). silverone 2026-06-04.
//
// DatasetService는 facade로 유지되어 핸들러/기존 호출자가 그대로
// `DatasetService.ExecuteAnalyze(...)`를 부른다. 내부적으로 AnalyzeService에
// 위임한다 (public API·동작 불변). analyze 로직 본문(ExecuteAnalyze /
// ExecuteAnalyzeOnActiveVersion / resolveAnalyzeArtifactPaths / postPythonAITask)은
// analyze.go에 두되 receiver를 *AnalyzeService로 둔다.
//
// 의존은 단방향이다: AnalyzeService → (version resolver + worker). thread/build
// 등 상위 흐름이 AnalyzeService(또는 facade)를 의존하며, 그 역방향은 없다.

// analyzeVersionResolver — AnalyzeService가 dataset/version을 조회하기 위한 최소
// 인터페이스. DatasetService가 이를 구현한다(같은 package). 전체 DatasetService에
// 의존하지 않고 필요한 것만 받아 순환 의존을 피한다.
type analyzeVersionResolver interface {
	GetDataset(projectID, datasetID string) (domain.Dataset, error)
	GetDatasetVersion(projectID, datasetID, versionID string) (domain.DatasetVersion, error)
	// #24 — clause_keywords 정제 사전(block/synonym)을 analyze 시점에 overlay로
	// 적용하기 위한 dataset 단위 활성 규칙 조회. 실패는 best-effort(미적용).
	ListKeywordDictionaryRules(projectID, datasetID string, activeOnly bool) ([]domain.KeywordDictionaryRule, error)
}

type AnalyzeService struct {
	versions      analyzeVersionResolver
	workerURL     string
	workerTimeout time.Duration
}

// NewAnalyzeService — versions resolver + python worker 접속 정보를 받아 구성.
// workerTimeout이 0 이하면 postPythonAITask가 defaultPythonAITaskTimeout로 fallback.
func NewAnalyzeService(versions analyzeVersionResolver, workerURL string, workerTimeout time.Duration) *AnalyzeService {
	return &AnalyzeService{
		versions:      versions,
		workerURL:     workerURL,
		workerTimeout: workerTimeout,
	}
}

// analyze — DatasetService가 보유한 현재 worker URL/timeout으로 AnalyzeService를
// 구성한다. 매 호출 시 현재 필드값을 읽어 SetPythonAITaskTimeout / test의
// pythonAIWorkerURL 재할당을 반영한다(buildClient 패턴과 동일 의도).
func (s *DatasetService) analyze() *AnalyzeService {
	return NewAnalyzeService(s, s.pythonAIWorkerURL, s.pythonAITaskTimeout)
}

// ExecuteAnalyze — facade. AnalyzeService로 위임 (public API 유지).
func (s *DatasetService) ExecuteAnalyze(
	ctx context.Context,
	projectID, datasetID, versionID string,
	req AnalyzeRequest,
) (AnalyzeResponse, error) {
	return s.analyze().ExecuteAnalyze(ctx, projectID, datasetID, versionID, req)
}

// ExecuteAnalyzeOnActiveVersion — facade. AnalyzeService로 위임 (public API 유지).
func (s *DatasetService) ExecuteAnalyzeOnActiveVersion(
	ctx context.Context,
	projectID, datasetID string,
	req AnalyzeRequest,
) (AnalyzeResponse, error) {
	return s.analyze().ExecuteAnalyzeOnActiveVersion(ctx, projectID, datasetID, req)
}
