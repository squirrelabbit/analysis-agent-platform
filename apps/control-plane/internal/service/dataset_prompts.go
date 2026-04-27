package service

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
	"analysis-support-platform/control-plane/internal/store"
)

var promptPlaceholderPattern = regexp.MustCompile(`{{\s*([a-zA-Z0-9_]+)\s*}}`)

var allowedPromptOperations = map[string]map[string]struct{}{
	"prepare": {
		"raw_text": {},
	},
	"prepare_batch": {
		"rows_json": {},
	},
	"sentiment": {
		"text": {},
	},
	"sentiment_batch": {
		"rows_json": {},
	},
}

type projectPromptTemplates struct {
	RowTemplate     string
	BatchTemplate   string
	UsesProjectSlot bool
}

func (s *DatasetService) SetPromptTemplatesDir(path string) {
	s.promptTemplatesDir = strings.TrimSpace(path)
}

func (s *DatasetService) SaveProjectPrompt(projectID string, input domain.ProjectPromptUpsertRequest) (domain.ProjectPrompt, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPrompt{}, ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPrompt{}, err
	}

	version := strings.TrimSpace(input.Version)
	if version == "" {
		return domain.ProjectPrompt{}, ErrInvalidArgument{Message: "version is required"}
	}
	operation, err := normalizePromptOperation(input.Operation)
	if err != nil {
		return domain.ProjectPrompt{}, err
	}
	content := strings.TrimSpace(input.Content)
	if content == "" {
		return domain.ProjectPrompt{}, ErrInvalidArgument{Message: "content is required"}
	}

	metadata := parsePromptFrontMatter(content)
	if frontOperation := strings.TrimSpace(metadata["operation"]); frontOperation != "" && frontOperation != operation {
		return domain.ProjectPrompt{}, ErrInvalidArgument{Message: "front matter operation does not match request operation"}
	}
	if err := validatePromptTemplatePlaceholders(content, operation); err != nil {
		return domain.ProjectPrompt{}, err
	}

	if _, err := s.store.GetProjectPrompt(projectID, version, operation); err == nil {
		return domain.ProjectPrompt{}, ErrConflict{Message: "project prompt version already exists for operation"}
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
	return prompt, nil
}

func (s *DatasetService) ListProjectPrompts(projectID string) (domain.ProjectPromptListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptListResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptListResponse{}, err
	}
	items, err := s.store.ListProjectPrompts(projectID)
	if err != nil {
		return domain.ProjectPromptListResponse{}, err
	}
	return domain.ProjectPromptListResponse{Items: items}, nil
}

func (s *DatasetService) CreatePrompt(input domain.PromptCreateRequest) (domain.Prompt, error) {
	version := strings.TrimSpace(input.Version)
	if version == "" {
		return domain.Prompt{}, ErrInvalidArgument{Message: "version is required"}
	}
	operation, err := normalizePromptOperation(input.Operation)
	if err != nil {
		return domain.Prompt{}, err
	}
	content := strings.TrimSpace(input.Content)
	if content == "" {
		return domain.Prompt{}, ErrInvalidArgument{Message: "content is required"}
	}

	metadata := parsePromptFrontMatter(content)
	if frontOperation := strings.TrimSpace(metadata["operation"]); frontOperation != "" && frontOperation != operation {
		return domain.Prompt{}, ErrInvalidArgument{Message: "front matter operation does not match request operation"}
	}
	if err := validatePromptTemplatePlaceholders(content, operation); err != nil {
		return domain.Prompt{}, err
	}
	if _, err := s.store.GetPromptByVersion(version, operation); err == nil {
		return domain.Prompt{}, ErrConflict{Message: "prompt version already exists for operation"}
	} else if err != store.ErrNotFound {
		return domain.Prompt{}, err
	}

	now := time.Now().UTC()
	prompt := domain.Prompt{
		PromptID:    id.New(),
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
	if err := s.store.SavePrompt(prompt); err != nil {
		return domain.Prompt{}, err
	}
	return prompt, nil
}

func (s *DatasetService) GetPrompt(promptID string) (domain.Prompt, error) {
	prompt, err := s.store.GetPrompt(promptID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Prompt{}, ErrNotFound{Resource: "prompt"}
		}
		return domain.Prompt{}, err
	}
	return prompt, nil
}

func (s *DatasetService) ListPrompts(operation string) (domain.PromptListResponse, error) {
	filter := strings.TrimSpace(operation)
	if filter != "" {
		normalized, err := normalizePromptOperation(filter)
		if err != nil {
			return domain.PromptListResponse{}, err
		}
		filter = normalized
	}
	items, err := s.store.ListPrompts(filter)
	if err != nil {
		return domain.PromptListResponse{}, err
	}
	return domain.PromptListResponse{Items: items}, nil
}

func (s *DatasetService) UpdatePrompt(promptID string, input domain.PromptUpdateRequest) (domain.Prompt, error) {
	prompt, err := s.store.GetPrompt(promptID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Prompt{}, ErrNotFound{Resource: "prompt"}
		}
		return domain.Prompt{}, err
	}

	version := prompt.Version
	if input.Version != nil {
		version = strings.TrimSpace(*input.Version)
	}
	if version == "" {
		return domain.Prompt{}, ErrInvalidArgument{Message: "version is required"}
	}

	operation := prompt.Operation
	if input.Operation != nil {
		operation, err = normalizePromptOperation(*input.Operation)
		if err != nil {
			return domain.Prompt{}, err
		}
	}

	content := prompt.Content
	if input.Content != nil {
		content = strings.TrimSpace(*input.Content)
	}
	if content == "" {
		return domain.Prompt{}, ErrInvalidArgument{Message: "content is required"}
	}
	if input.Version == nil && input.Operation == nil && input.Content == nil {
		return domain.Prompt{}, ErrInvalidArgument{Message: "at least one field must be provided"}
	}

	metadata := parsePromptFrontMatter(content)
	if frontOperation := strings.TrimSpace(metadata["operation"]); frontOperation != "" && frontOperation != operation {
		return domain.Prompt{}, ErrInvalidArgument{Message: "front matter operation does not match request operation"}
	}
	if err := validatePromptTemplatePlaceholders(content, operation); err != nil {
		return domain.Prompt{}, err
	}
	if existing, err := s.store.GetPromptByVersion(version, operation); err == nil && existing.PromptID != promptID {
		return domain.Prompt{}, ErrConflict{Message: "prompt version already exists for operation"}
	} else if err != nil && err != store.ErrNotFound {
		return domain.Prompt{}, err
	}

	prompt.Version = version
	prompt.Operation = operation
	prompt.Title = defaultPromptMetaValue(metadata["title"], version)
	prompt.Status = defaultPromptMetaValue(metadata["status"], "active")
	prompt.Summary = strings.TrimSpace(metadata["summary"])
	prompt.Content = content
	prompt.ContentHash = sha256Hex(content)
	prompt.UpdatedAt = time.Now().UTC()
	if err := s.store.SavePrompt(prompt); err != nil {
		return domain.Prompt{}, err
	}
	return prompt, nil
}

func (s *DatasetService) DeletePrompt(promptID string) error {
	if err := s.store.DeletePrompt(promptID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "prompt"}
		}
		return err
	}
	return nil
}

func (s *DatasetService) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{}, ErrNotFound{Resource: "project"}
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

func (s *DatasetService) UpdateProjectPromptDefaults(projectID string, input domain.ProjectPromptDefaultsUpdateRequest) (domain.ProjectPromptDefaults, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{}, ErrNotFound{Resource: "project"}
		}
		return domain.ProjectPromptDefaults{}, err
	}

	defaults := domain.ProjectPromptDefaults{
		ProjectID:              projectID,
		PreparePromptVersion:   trimStringPointer(input.PreparePromptVersion),
		SentimentPromptVersion: trimStringPointer(input.SentimentPromptVersion),
	}
	if defaults.PreparePromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.PreparePromptVersion, "prepare") {
		return domain.ProjectPromptDefaults{}, ErrInvalidArgument{Message: "prepare default prompt version must reference a project prepare prompt"}
	}
	if defaults.SentimentPromptVersion != nil && !s.projectHasPromptVersion(projectID, *defaults.SentimentPromptVersion, "sentiment") {
		return domain.ProjectPromptDefaults{}, ErrInvalidArgument{Message: "sentiment default prompt version must reference a project sentiment prompt"}
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

func inferPromptOperation(version string) string {
	trimmed := strings.TrimSpace(version)
	switch {
	case strings.Contains(trimmed, "prepare-anthropic-batch"):
		return "prepare_batch"
	case strings.Contains(trimmed, "prepare-anthropic"):
		return "prepare"
	case strings.Contains(trimmed, "sentiment-anthropic-batch"):
		return "sentiment_batch"
	case strings.Contains(trimmed, "sentiment-anthropic"):
		return "sentiment"
	default:
		return "custom"
	}
}

func inferPromptDefaultGroups(version string) []string {
	switch strings.TrimSpace(version) {
	case "dataset-prepare-anthropic-v1":
		return []string{"prepare"}
	case "dataset-prepare-anthropic-batch-v1":
		return []string{"prepare_batch"}
	case "sentiment-anthropic-v1":
		return []string{"sentiment"}
	case "sentiment-anthropic-batch-v1":
		return []string{"sentiment_batch"}
	default:
		return nil
	}
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
		return "", ErrInvalidArgument{Message: "operation is required"}
	}
	if _, ok := allowedPromptOperations[operation]; !ok {
		return "", ErrInvalidArgument{Message: "unsupported prompt operation"}
	}
	return operation, nil
}

func validatePromptTemplatePlaceholders(content string, operation string) error {
	allowed, ok := allowedPromptOperations[operation]
	if !ok {
		return ErrInvalidArgument{Message: "unsupported prompt operation"}
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
		return ErrInvalidArgument{Message: fmt.Sprintf("unsupported placeholder %q for %s prompt", placeholder, operation)}
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
		return ErrInvalidArgument{Message: fmt.Sprintf("missing placeholders for %s prompt: %s", operation, strings.Join(missing, ", "))}
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

func (s *DatasetService) projectHasPromptVersion(projectID, version string, allowedOperations ...string) bool {
	for _, operation := range allowedOperations {
		if _, err := s.store.GetProjectPrompt(projectID, version, operation); err == nil {
			return true
		}
	}
	return false
}

func (s *DatasetService) projectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	defaults, err := s.store.GetProjectPromptDefaults(projectID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProjectPromptDefaults{ProjectID: projectID}, nil
		}
		return domain.ProjectPromptDefaults{}, err
	}
	return defaults, nil
}

func (s *DatasetService) resolveEffectiveProjectPromptVersion(projectID string, explicit *string, operation string) (string, error) {
	if value := trimStringPointer(explicit); value != nil {
		return *value, nil
	}

	defaults, err := s.projectPromptDefaults(projectID)
	if err != nil {
		return "", err
	}
	switch operation {
	case "prepare":
		if defaults.PreparePromptVersion != nil {
			return *defaults.PreparePromptVersion, nil
		}
	case "sentiment":
		if defaults.SentimentPromptVersion != nil {
			return *defaults.SentimentPromptVersion, nil
		}
	}
	return "", nil
}

func (s *DatasetService) lookupProjectPromptContent(projectID, version, operation string) (string, bool, error) {
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

func (s *DatasetService) lookupGlobalPromptContent(version, operation string) (string, bool, error) {
	trimmedVersion := strings.TrimSpace(version)
	if trimmedVersion == "" {
		return "", false, nil
	}
	prompt, err := s.store.GetPromptByVersion(trimmedVersion, operation)
	if err != nil {
		if err == store.ErrNotFound {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(prompt.Content), true, nil
}

func (s *DatasetService) resolveProjectPromptTemplates(projectID, version, rowOperation, batchOperation string) (projectPromptTemplates, error) {
	rowTemplate, rowExists, err := s.lookupProjectPromptContent(projectID, version, rowOperation)
	if err != nil {
		return projectPromptTemplates{}, err
	}
	batchTemplate := ""
	batchExists := false
	if strings.TrimSpace(batchOperation) != "" {
		batchTemplate, batchExists, err = s.lookupProjectPromptContent(projectID, version, batchOperation)
		if err != nil {
			return projectPromptTemplates{}, err
		}
	}
	if !rowExists && !batchExists {
		rowTemplate, rowExists, err = s.lookupGlobalPromptContent(version, rowOperation)
		if err != nil {
			return projectPromptTemplates{}, err
		}
		if strings.TrimSpace(batchOperation) != "" {
			batchTemplate, batchExists, err = s.lookupGlobalPromptContent(version, batchOperation)
			if err != nil {
				return projectPromptTemplates{}, err
			}
		}
	}
	if !rowExists && !batchExists {
		return projectPromptTemplates{}, nil
	}
	if !rowExists {
		return projectPromptTemplates{}, ErrInvalidArgument{Message: fmt.Sprintf("project prompt version %q requires %s template", strings.TrimSpace(version), rowOperation)}
	}
	return projectPromptTemplates{
		RowTemplate:     rowTemplate,
		BatchTemplate:   batchTemplate,
		UsesProjectSlot: true,
	}, nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func promptMetadataKey(version, operation string) string {
	return strings.TrimSpace(version) + "::" + strings.TrimSpace(operation)
}
