// Package datasetprompts — silverone 2026-05-28 pilot. 옛 service/dataset_prompts.go
// (823 lines) 전체를 subpackage로 이동. DatasetService는 facade로 같은
// public method signature를 그대로 노출한다 (외부 호출자 변경 0).
//
// 의존성:
//   - store.Repository 의 8 method (Store interface로 좁힘 — 의존 역전)
//   - serviceerror (typed error — service.Err* alias의 본체)
//   - domain (도메인 타입)
//   - id (change_id 생성)
//   - obs (resolve log)
//
// service 패키지에 의존 안 함 (단방향: service → datasetprompts).
package datasetprompts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/serviceerror"
	"analysis-support-platform/control-plane/internal/store"
)

// Store — datasetprompts가 의존하는 store.Repository method subset.
// store.Repository 전체에 의존하지 않고 의존 역전. test에서도 8 method만 mock.
type Store interface {
	GetProject(projectID string) (domain.Project, error)
	GetProjectPrompt(projectID, version, operation string) (domain.ProjectPrompt, error)
	SaveProjectPrompt(prompt domain.ProjectPrompt) error
	ListProjectPrompts(projectID string) ([]domain.ProjectPrompt, error)
	SaveProjectPromptDefaults(defaults domain.ProjectPromptDefaults) error
	GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error)
	AppendProjectPromptChange(change domain.ProjectPromptChange) error
	ListProjectPromptChanges(projectID, operation string) ([]domain.ProjectPromptChange, error)
}

// Service — project prompt CRUD + 4-tier resolution + access/audit 정책.
type Service struct {
	store Store
}

// New — datasetprompts.Service 생성. `store`는 보통 store.Repository를 그대로
// 넘긴다 (Go duck typing — Store subset method 8개를 만족).
func New(store Store) *Service {
	return &Service{store: store}
}

// silverone 2026-05-28 — 옛 `promptTemplatesDir` field + `SetTemplatesDir`
// setter는 read 호출자가 0이라 본 패키지에서 제거됐다 (옛 file-based prompt
// loading 흐름 잔존). 외부에서 dir을 받던 `DatasetService.SetPromptTemplatesDir`
// facade는 server.go init wiring 보호를 위해 no-op로 유지.

// ProjectPromptTemplates — resolveProjectPromptTemplates 반환 타입. service
// 패키지에서 옛 `projectPromptTemplates` (private)였으나, subpackage 분리로
// public export. 다른 service 파일이 import해서 사용하면 됨. 현재 호출자는
// datasetprompts 내부에만 — 외부 호출자 없으면 후속 PR에서 private로 회귀
// 가능 (silverone 2026-05-28 audit 후속 정리 후보).
type ProjectPromptTemplates struct {
	RowTemplate     string
	BatchTemplate   string
	UsesProjectSlot bool
}

var promptPlaceholderPattern = regexp.MustCompile(`{{\s*([a-zA-Z0-9_]+)\s*}}`)

// allowedPromptOperations maps each supported prompt operation to the
// placeholder names its template MUST carry. Validated in
// validatePromptTemplatePlaceholders so a missing placeholder fails
// fast at upsert time rather than at LLM dispatch.
//
// ADR-015 §A1~A3 added the 4 analysis-tier operations (planner /
// planner_meta / issue_evidence_summary / execution_final_answer) so
// the registry is the single source of truth for all 8 LLM operations.
// (β2 / 5/19) prepare / sentiment / prepare_batch / sentiment_batch operation
// map entry는 β2로 제거됨 — 의존하던 dataset_build task 4종이 통째 삭제됐다.
// “ProjectPromptDefaults.PreparePromptVersion“ 등 DB schema 필드는 audit
// 호환을 위해 보존(read-only inactive).
var allowedPromptOperations = map[string]map[string]struct{}{
	"planner": {
		"allowed_skills":           {},
		"active_layers":            {},
		"skill_descriptions_block": {},
		"recommendations_block":    {},
		"dataset_name":             {},
		"dataset_version_id":       {},
		"data_type":                {},
		"goal":                     {},
		"constraints_json":         {},
		"context_json":             {},
	},
	"planner_meta": {
		"question":       {},
		"allowed_layers": {},
	},
	"issue_evidence_summary": {
		"dataset_name":          {},
		"query":                 {},
		"analysis_context_json": {},
		"documents_json":        {},
	},
	"execution_final_answer": {
		"question":      {},
		"scenario_json": {},
		"result_json":   {},
		"evidence_json": {},
	},
}

func (s *Service) SaveProjectPrompt(projectID string, input domain.ProjectPromptUpsertRequest) (domain.ProjectPrompt, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPrompt{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPrompt{}, err
	}

	version := strings.TrimSpace(input.Version)
	if version == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "version is required"}
	}
	operation, err := normalizePromptOperation(input.Operation)
	if err != nil {
		return domain.ProjectPrompt{}, err
	}
	content := strings.TrimSpace(input.Content)
	if content == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "content is required"}
	}

	metadata := parsePromptFrontMatter(content)
	if frontOperation := strings.TrimSpace(metadata["operation"]); frontOperation != "" && frontOperation != operation {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "front matter operation does not match request operation"}
	}
	if err := validatePromptTemplatePlaceholders(content, operation); err != nil {
		return domain.ProjectPrompt{}, err
	}

	// ADR-015 §C2: change_reason is required so the audit log carries the
	// "왜 바꿨나" — empty string ⇒ 400. silverone가 "변경이 진짜 많을
	// 것"이라고 했고 audit 부재가 운영자 가치를 깎는다고 명시.
	changeReason := strings.TrimSpace(input.ChangeReason)
	if changeReason == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "change_reason is required"}
	}

	// ADR-015 §D1: planner / planner_meta are operator_only — analyst
	// edit blocked unless caller carries the operator header.
	if err := requireOperatorForOperation(operation, input.CallerIsOperator); err != nil {
		return domain.ProjectPrompt{}, err
	}

	if _, err := s.store.GetProjectPrompt(projectID, version, operation); err == nil {
		return domain.ProjectPrompt{}, serviceerror.ErrConflict{Message: "project prompt version already exists for operation"}
	} else if err != store.ErrNotFound {
		return domain.ProjectPrompt{}, err
	}

	now := time.Now().UTC()
	prompt := domain.ProjectPrompt{
		ProjectID:   projectID,
		Version:     version,
		Operation:   operation,
		Title:       defaultPromptMetaValue(metadata["title"], version),
		Status:      defaultPromptMetaValue(metadata["status"], "active"),
		Summary:     strings.TrimSpace(metadata["summary"]),
		Content:     content,
		ContentHash: sha256Hex(content),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.store.SaveProjectPrompt(prompt); err != nil {
		return domain.ProjectPrompt{}, err
	}
	if err := s.store.AppendProjectPromptChange(domain.ProjectPromptChange{
		ChangeID:       id.New(),
		ProjectID:      projectID,
		Version:        version,
		Operation:      operation,
		Action:         "create",
		ChangeReason:   changeReason,
		NewContentHash: prompt.ContentHash,
		ChangedAt:      now,
	}); err != nil {
		return domain.ProjectPrompt{}, err
	}
	return prompt, nil
}

// ListProjectPromptHistory returns the ADR-015 §C audit log filtered by
// project and (optional) operation. Used by the prompt edit UI to render
// the timeline + diff viewer.
func (s *Service) ListProjectPromptHistory(projectID, operation string) (domain.ProjectPromptHistoryResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptHistoryResponse{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptHistoryResponse{}, err
	}
	items, err := s.store.ListProjectPromptChanges(projectID, strings.TrimSpace(operation))
	if err != nil {
		return domain.ProjectPromptHistoryResponse{}, err
	}
	return domain.ProjectPromptHistoryResponse{Items: items}, nil
}

// RevertProjectPrompt creates a new prompt version whose body is copied
// from an earlier “to_version“. Implemented as a new immutable version
// (action=revert) instead of mutating the active row, so replay/audit
// stays clean (Codex review §Q4).
func (s *Service) RevertProjectPrompt(projectID, operation string, input domain.ProjectPromptRevertRequest) (domain.ProjectPrompt, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPrompt{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPrompt{}, err
	}
	normalizedOp, err := normalizePromptOperation(operation)
	if err != nil {
		return domain.ProjectPrompt{}, err
	}
	toVersion := strings.TrimSpace(input.ToVersion)
	if toVersion == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "to_version is required"}
	}
	newVersion := strings.TrimSpace(input.NewVersion)
	if newVersion == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "new_version is required"}
	}
	changeReason := strings.TrimSpace(input.ChangeReason)
	if changeReason == "" {
		return domain.ProjectPrompt{}, serviceerror.ErrInvalidArgument{Message: "change_reason is required"}
	}

	// ADR-015 §D1: revert is also gated on operator role (analyst
	// reverting a planner version would silently regress the plan
	// layer).
	if err := requireOperatorForOperation(normalizedOp, input.CallerIsOperator); err != nil {
		return domain.ProjectPrompt{}, err
	}

	source, err := s.store.GetProjectPrompt(projectID, toVersion, normalizedOp)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPrompt{}, serviceerror.ErrNotFound{Resource: "to_version"}
		}
		return domain.ProjectPrompt{}, err
	}
	if existing, err := s.store.GetProjectPrompt(projectID, newVersion, normalizedOp); err == nil {
		_ = existing
		return domain.ProjectPrompt{}, serviceerror.ErrConflict{Message: "new_version already exists for operation"}
	} else if err != store.ErrNotFound {
		return domain.ProjectPrompt{}, err
	}

	now := time.Now().UTC()
	cloned := domain.ProjectPrompt{
		ProjectID:   projectID,
		Version:     newVersion,
		Operation:   normalizedOp,
		Title:       source.Title,
		Status:      source.Status,
		Summary:     source.Summary,
		Content:     source.Content,
		ContentHash: source.ContentHash,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.store.SaveProjectPrompt(cloned); err != nil {
		return domain.ProjectPrompt{}, err
	}
	if err := s.store.AppendProjectPromptChange(domain.ProjectPromptChange{
		ChangeID:       id.New(),
		ProjectID:      projectID,
		Version:        newVersion,
		Operation:      normalizedOp,
		Action:         "revert",
		ChangeReason:   changeReason,
		NewContentHash: cloned.ContentHash,
		BaseVersion:    toVersion,
		ChangedAt:      now,
	}); err != nil {
		return domain.ProjectPrompt{}, err
	}
	return cloned, nil
}

// DiffProjectPromptVersions returns a unified diff + line stats between
// two stored versions of the same project + operation. Used by the
// edit/history UI.
func (s *Service) DiffProjectPromptVersions(projectID, operation, baseVersion, headVersion string) (domain.ProjectPromptDiffResponse, error) {
	normalizedOp, err := normalizePromptOperation(operation)
	if err != nil {
		return domain.ProjectPromptDiffResponse{}, err
	}
	baseVersion = strings.TrimSpace(baseVersion)
	headVersion = strings.TrimSpace(headVersion)
	if baseVersion == "" || headVersion == "" {
		return domain.ProjectPromptDiffResponse{}, serviceerror.ErrInvalidArgument{Message: "base_version and head_version are required"}
	}
	base, err := s.store.GetProjectPrompt(projectID, baseVersion, normalizedOp)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDiffResponse{}, serviceerror.ErrNotFound{Resource: "base_version"}
		}
		return domain.ProjectPromptDiffResponse{}, err
	}
	head, err := s.store.GetProjectPrompt(projectID, headVersion, normalizedOp)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDiffResponse{}, serviceerror.ErrNotFound{Resource: "head_version"}
		}
		return domain.ProjectPromptDiffResponse{}, err
	}
	unified, stats := computeUnifiedDiff(base.Content, head.Content)
	return domain.ProjectPromptDiffResponse{
		ProjectID:   projectID,
		Operation:   normalizedOp,
		BaseVersion: baseVersion,
		HeadVersion: headVersion,
		BaseContent: base.Content,
		HeadContent: head.Content,
		UnifiedDiff: unified,
		Stats:       stats,
	}, nil
}

// ComputeUnifiedDiff — silverone 2026-05-28 subpackage 이동에 따른 test 호환
// export. 옛 service.computeUnifiedDiff와 동일. service/dataset_prompts.go에
// 같은 이름의 private wrapper를 두어 옛 service-패키지 test가 그대로 동작한다.
func ComputeUnifiedDiff(base, head string) (string, domain.ProjectPromptDiffStats) {
	return computeUnifiedDiff(base, head)
}

// computeUnifiedDiff produces a minimal line-based unified diff between
// two prompt bodies and the corresponding line stats.
//
// We don't depend on a third-party library — for a few-hundred-line
// prompt template the naive O(n+m) marker walk is fine. It's a UI aid,
// not a merge-base, so we don't need optimal LCS.
func computeUnifiedDiff(base, head string) (string, domain.ProjectPromptDiffStats) {
	baseLines := strings.Split(base, "\n")
	headLines := strings.Split(head, "\n")
	stats := domain.ProjectPromptDiffStats{
		BaseLines: len(baseLines),
		HeadLines: len(headLines),
	}
	baseSet := make(map[string]int, len(baseLines))
	for _, line := range baseLines {
		baseSet[line]++
	}
	headSet := make(map[string]int, len(headLines))
	for _, line := range headLines {
		headSet[line]++
	}
	var diff strings.Builder
	for _, line := range baseLines {
		if headSet[line] > 0 {
			headSet[line]--
		} else {
			stats.RemovedLines++
			diff.WriteString("- ")
			diff.WriteString(line)
			diff.WriteString("\n")
		}
	}
	for _, line := range headLines {
		if baseSet[line] > 0 {
			baseSet[line]--
		} else {
			stats.AddedLines++
			diff.WriteString("+ ")
			diff.WriteString(line)
			diff.WriteString("\n")
		}
	}
	return diff.String(), stats
}

func (s *Service) ListProjectPrompts(projectID string) (domain.ProjectPromptListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptListResponse{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptListResponse{}, err
	}
	items, err := s.store.ListProjectPrompts(projectID)
	if err != nil {
		return domain.ProjectPromptListResponse{}, err
	}
	return domain.ProjectPromptListResponse{Items: items}, nil
}

// 5/6 화면기획서 B안 채택: 전역 prompt 라이브러리 (CreatePrompt/GetPrompt/
// ListPrompts/UpdatePrompt/DeletePrompt) 5개 service 함수 제거. 글로벌
// prompt는 .md 코드 계약, 프로젝트별만 DB. 운영자 hot-edit은
// SaveProjectPrompt 흐름.

func (s *Service) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptDefaults{}, err
	}

	defaults, err := s.store.GetProjectPromptDefaults(projectID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{ProjectID: projectID}, nil
		}
		return domain.ProjectPromptDefaults{}, err
	}
	return defaults, nil
}

func (s *Service) UpdateProjectPromptDefaults(projectID string, input domain.ProjectPromptDefaultsUpdateRequest) (domain.ProjectPromptDefaults, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{}, serviceerror.ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptDefaults{}, err
	}

	defaults := domain.ProjectPromptDefaults{
		ProjectID:                         projectID,
		PreparePromptVersion:              trimStringPointer(input.PreparePromptVersion),
		SentimentPromptVersion:            trimStringPointer(input.SentimentPromptVersion),
		PlannerPromptVersion:              trimStringPointer(input.PlannerPromptVersion),
		PlannerMetaPromptVersion:          trimStringPointer(input.PlannerMetaPromptVersion),
		IssueEvidenceSummaryPromptVersion: trimStringPointer(input.IssueEvidenceSummaryPromptVersion),
		ExecutionFinalAnswerPromptVersion: trimStringPointer(input.ExecutionFinalAnswerPromptVersion),
	}
	// (β2 / 5/19) PreparePromptVersion / SentimentPromptVersion 필드는 DB schema
	// audit 호환을 위해 보존하지만 active 운영 path가 없다(의존 dataset_build
	// task β2 정리). 따라서 validation check도 함께 정리 — 값을 그대로
	// 저장만 한다. 다시 활성화될 때까지는 read 시 inactive로 노출.
	// ADR-015 Phase A5: planner/planner_meta/issue_evidence_summary/execution_final_answer
	// defaults validate against the project prompt catalog. The same
	// `projectHasPromptVersion(operation)` predicate is reused — the
	// operations are looked up by the prompt registry's
	// `_infer_prompt_operation` mapping (see scripts and Python registry).
	if defaults.PlannerPromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.PlannerPromptVersion, "planner") {
		return domain.ProjectPromptDefaults{}, serviceerror.ErrInvalidArgument{Message: "planner default prompt version must reference a project planner prompt"}
	}
	if defaults.PlannerMetaPromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.PlannerMetaPromptVersion, "planner_meta") {
		return domain.ProjectPromptDefaults{}, serviceerror.ErrInvalidArgument{Message: "planner_meta default prompt version must reference a project planner_meta prompt"}
	}
	if defaults.IssueEvidenceSummaryPromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.IssueEvidenceSummaryPromptVersion, "issue_evidence_summary") {
		return domain.ProjectPromptDefaults{}, serviceerror.ErrInvalidArgument{Message: "issue_evidence_summary default prompt version must reference a project issue_evidence_summary prompt"}
	}
	if defaults.ExecutionFinalAnswerPromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.ExecutionFinalAnswerPromptVersion, "execution_final_answer") {
		return domain.ProjectPromptDefaults{}, serviceerror.ErrInvalidArgument{Message: "execution_final_answer default prompt version must reference a project execution_final_answer prompt"}
	}

	now := time.Now().UTC()
	defaults.UpdatedAt = &now
	if err := s.store.SaveProjectPromptDefaults(defaults); err != nil {
		return domain.ProjectPromptDefaults{}, err
	}
	return defaults, nil
}

func parsePromptFrontMatter(raw string) map[string]string {
	metadata, _ := splitPromptFrontMatter(raw)
	return metadata
}

func defaultPromptMetaValue(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func normalizePromptOperation(value string) (string, error) {
	operation := strings.TrimSpace(value)
	if operation == "" {
		return "", serviceerror.ErrInvalidArgument{Message: "operation is required"}
	}
	if _, ok := allowedPromptOperations[operation]; !ok {
		return "", serviceerror.ErrInvalidArgument{Message: "unsupported prompt operation"}
	}
	return operation, nil
}

func validatePromptTemplatePlaceholders(content string, operation string) error {
	allowed, ok := allowedPromptOperations[operation]
	if !ok {
		return serviceerror.ErrInvalidArgument{Message: "unsupported prompt operation"}
	}
	_, body := splitPromptFrontMatter(content)
	found := make(map[string]struct{}, len(allowed))
	for _, matches := range promptPlaceholderPattern.FindAllStringSubmatch(body, -1) {
		if len(matches) < 2 {
			continue
		}
		placeholder := strings.TrimSpace(matches[1])
		if placeholder == "" {
			continue
		}
		if _, ok := allowed[placeholder]; ok {
			found[placeholder] = struct{}{}
			continue
		}
		return serviceerror.ErrInvalidArgument{Message: fmt.Sprintf("unsupported placeholder %q for %s prompt", placeholder, operation)}
	}
	missing := make([]string, 0)
	for placeholder := range allowed {
		if _, ok := found[placeholder]; ok {
			continue
		}
		missing = append(missing, placeholder)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return serviceerror.ErrInvalidArgument{Message: fmt.Sprintf("missing placeholders for %s prompt: %s", operation, strings.Join(missing, ", "))}
	}
	return nil
}

func splitPromptFrontMatter(raw string) (map[string]string, string) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "---\n") {
		return map[string]string{}, trimmed
	}
	lines := strings.Split(trimmed, "\n")
	metadata := make(map[string]string)
	closingIndex := -1
	for index, line := range lines[1:] {
		if strings.TrimSpace(line) == "---" {
			closingIndex = index + 1
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		metadata[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if closingIndex < 0 {
		return map[string]string{}, trimmed
	}
	body := strings.TrimSpace(strings.Join(lines[closingIndex+1:], "\n"))
	return metadata, body
}

func (s *Service) projectHasPromptVersion(projectID, version string, allowedOperations ...string) bool {
	for _, operation := range allowedOperations {
		if _, err := s.store.GetProjectPrompt(projectID, version, operation); err == nil {
			return true
		}
	}
	return false
}

// silverone 2026-05-28 — ADR-015 §B 4-tier prompt resolver 잔존 제거.
// production 호출자 0건이라 ResolvePromptVersion / ResolveEffectiveProject
// PromptVersion / projectPromptDefaults / projectPromptDefaultForOperation /
// datasetVersionPromptOverride / resolvePromptVersionInner / PromptTier* /
// prompt.resolved obs event 일체 제거 (resolution_test.go 함께 삭제).
// 옛 plan v1 + prepare/sentiment build path 부산물로, δ-2~δ-4 정리로 caller가
// 모두 사라졌다. 현재 plan v2는 Python worker registry/env로 prompt 선택.
// 4-tier 정책 부활 시 git history (commit f076adb6 직전)에서 복원.

// PromptEditableTier names the access tier for a prompt operation. See
// ADR-015 §D1 for the full matrix; the values here correspond to the
// `PromptTemplateMetadata.EditableBy` field exposed in the prompt
// catalog API response.
const (
	PromptEditableByAnalyst      = "analyst"
	PromptEditableByOperatorOnly = "operator_only"
	PromptEditableBySystem       = "system"
)

// PromptOperationEditableBy returns the access tier for the given
// operation. Unknown operations default to “analyst“ so a custom
// project-only prompt is editable by analysts unless the operator
// explicitly tags it.
//
// The matrix mirrors Codex review §Q2 권고 (planner is operator-only
// because allowed_skills + replay rules are deeply coupled — analyst
// edits could break the plan layer entirely).
func PromptOperationEditableBy(operation string) string {
	switch strings.TrimSpace(operation) {
	case "planner", "planner_meta":
		return PromptEditableByOperatorOnly
	default:
		return PromptEditableByAnalyst
	}
}

// requireOperatorForOperation returns an ErrInvalidArgument when the
// operation is operator_only and the caller did not declare
// “CallerIsOperator“ (set from the X-Operator-Mode header). Until
// auth lands this is soft enforcement — the analyst UI uses the
// “editable_by“ field to render the editor read-only, and the
// header-protected POST is the demo-period guard against accidental
// edits.
func requireOperatorForOperation(operation string, callerIsOperator bool) error {
	if PromptOperationEditableBy(operation) == PromptEditableByOperatorOnly && !callerIsOperator {
		return serviceerror.ErrInvalidArgument{
			Message: "operation '" + operation + "' is operator_only — set X-Operator-Mode: 1 (ADR-015 §D1)",
		}
	}
	return nil
}

// LookupProjectPromptContent — export (옛 lookupProjectPromptContent).
// 외부 호출자 없으면 후속 PR에서 private로 회귀 가능.
func (s *Service) LookupProjectPromptContent(projectID, version, operation string) (string, bool, error) {
	trimmedVersion := strings.TrimSpace(version)
	if trimmedVersion == "" {
		return "", false, nil
	}
	prompt, err := s.store.GetProjectPrompt(projectID, trimmedVersion, operation)
	if err != nil {
		if err == store.ErrNotFound {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(prompt.Content), true, nil
}

// 5/6 화면기획서 B안 채택: lookupGlobalPromptContent 제거. 글로벌 prompt는
// .md 코드 계약이라 DB lookup 안 함. project_prompts에 등록된 row/batch
// template만 검사 — 없으면 빈 응답 (worker가 .md fallback 사용).

// ResolveProjectPromptTemplates — export (옛 resolveProjectPromptTemplates).
func (s *Service) ResolveProjectPromptTemplates(projectID, version, rowOperation, batchOperation string) (ProjectPromptTemplates, error) {
	rowTemplate, rowExists, err := s.LookupProjectPromptContent(projectID, version, rowOperation)
	if err != nil {
		return ProjectPromptTemplates{}, err
	}
	batchTemplate := ""
	batchExists := false
	if strings.TrimSpace(batchOperation) != "" {
		batchTemplate, batchExists, err = s.LookupProjectPromptContent(projectID, version, batchOperation)
		if err != nil {
			return ProjectPromptTemplates{}, err
		}
	}
	if !rowExists && !batchExists {
		return ProjectPromptTemplates{}, nil
	}
	if !rowExists {
		return ProjectPromptTemplates{}, serviceerror.ErrInvalidArgument{Message: fmt.Sprintf("project prompt version %q requires %s template", strings.TrimSpace(version), rowOperation)}
	}
	return ProjectPromptTemplates{
		RowTemplate:     rowTemplate,
		BatchTemplate:   batchTemplate,
		UsesProjectSlot: true,
	}, nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// trimStringPointer — silverone 2026-05-28 subpackage 이동에 따른 helper 복사.
// 옛 service/dataset_metadata.go::trimStringPointer와 동일. 후속 PR에서 공통
// helper를 별도 패키지로 추출하면 dedup 가능.
func trimStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
