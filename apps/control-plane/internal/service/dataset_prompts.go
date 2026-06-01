package service

import (
	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-28 subpackage pilot — 옛 dataset_prompts.go (823 lines)
// 전체가 `internal/service/datasetprompts/`로 이동했다. 본 파일은 DatasetService
// 위 public method를 datasetprompts.Service로 위임하는 facade만 보유한다.
//
// silverone 2026-05-28 후속 정리 — ADR-015 4-tier prompt resolver 잔존 제거:
// ResolvePromptVersion / ResolveEffectiveProjectPromptVersion / PromptTier* /
// prompt.resolved obs event가 모두 production caller 0건이라 datasetprompts
// 패키지에서 함께 제거됐다. facade도 본 파일에서 제외. 부활 필요 시 git
// history에서 복원 (commit 직전 datasetprompts/service.go 참조).

// SetPromptTemplatesDir — silverone 2026-05-28 dead field 정리. 옛 file-based
// prompt loading 흐름의 잔존이라 본문에서 read하는 코드가 0건. 내부 field +
// subpackage setter는 모두 제거됐고, 본 facade는 외부 caller (http/server.go의
// init wiring)를 보호하기 위해 *deprecated no-op*으로만 유지된다. 후속 PR에서
// server.go 호출까지 함께 제거 가능.
func (s *DatasetService) SetPromptTemplatesDir(path string) {
	_ = path
}

func (s *DatasetService) SaveProjectPrompt(projectID string, input domain.ProjectPromptUpsertRequest) (domain.ProjectPrompt, error) {
	return s.prompts.SaveProjectPrompt(projectID, input)
}

func (s *DatasetService) ListProjectPromptHistory(projectID, operation string) (domain.ProjectPromptHistoryResponse, error) {
	return s.prompts.ListProjectPromptHistory(projectID, operation)
}

func (s *DatasetService) RevertProjectPrompt(projectID, operation string, input domain.ProjectPromptRevertRequest) (domain.ProjectPrompt, error) {
	return s.prompts.RevertProjectPrompt(projectID, operation, input)
}

func (s *DatasetService) DiffProjectPromptVersions(projectID, operation, baseVersion, headVersion string) (domain.ProjectPromptDiffResponse, error) {
	return s.prompts.DiffProjectPromptVersions(projectID, operation, baseVersion, headVersion)
}

func (s *DatasetService) ListProjectPrompts(projectID string) (domain.ProjectPromptListResponse, error) {
	return s.prompts.ListProjectPrompts(projectID)
}

func (s *DatasetService) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	return s.prompts.GetProjectPromptDefaults(projectID)
}

func (s *DatasetService) UpdateProjectPromptDefaults(projectID string, input domain.ProjectPromptDefaultsUpdateRequest) (domain.ProjectPromptDefaults, error) {
	return s.prompts.UpdateProjectPromptDefaults(projectID, input)
}
