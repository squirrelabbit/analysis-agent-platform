package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

type workerCapabilitiesResponse struct {
	PromptCatalog         []domain.PromptTemplateMetadata      `json:"prompt_catalog"`
	RuleCatalog           domain.DatasetProfileRuleCatalog     `json:"rule_catalog"`
	SkillPolicyCatalog    []domain.SkillPolicyMetadata         `json:"skill_policy_catalog"`
	SkillPolicyValidation domain.SkillPolicyValidationResponse `json:"skill_policy_validation"`
}

func (s *DatasetService) GetPromptCatalog() (domain.PromptCatalogResponse, error) {
	catalog, err := s.promptCatalog()
	if err != nil {
		return domain.PromptCatalogResponse{}, err
	}
	if len(catalog) == 0 {
		capabilities, err := s.fetchWorkerCapabilities()
		if err != nil {
			return domain.PromptCatalogResponse{}, err
		}
		if capabilities != nil {
			catalog = append([]domain.PromptTemplateMetadata(nil), capabilities.PromptCatalog...)
		}
	}
	return domain.PromptCatalogResponse{
		SourcePath: s.promptTemplatesDir,
		Items:      catalog,
	}, nil
}

func (s *DatasetService) GetRuleCatalog() (domain.RuleCatalogResponse, error) {
	capabilities, err := s.fetchWorkerCapabilities()
	if err != nil {
		return domain.RuleCatalogResponse{
			Available: false,
			Source:    "worker_capabilities",
			Warning:   fmt.Sprintf("python-ai worker capability 조회에 실패했습니다: %v", err),
		}, nil
	}
	if capabilities == nil {
		return domain.RuleCatalogResponse{
			Available: false,
			Source:    "worker_capabilities",
			Warning:   "python-ai worker URL이 설정되지 않아 rule catalog를 조회하지 못했습니다.",
		}, nil
	}
	catalog := capabilities.RuleCatalog
	return domain.RuleCatalogResponse{
		Available: true,
		Source:    "worker_capabilities",
		Catalog:   &catalog,
	}, nil
}

func (s *DatasetService) GetSkillPolicyCatalog() (domain.SkillPolicyCatalogResponse, error) {
	capabilities, err := s.fetchWorkerCapabilities()
	if err != nil {
		return domain.SkillPolicyCatalogResponse{
			Available: false,
			Source:    "worker_capabilities",
			Warning:   fmt.Sprintf("python-ai worker capability 조회에 실패했습니다: %v", err),
		}, nil
	}
	if capabilities == nil {
		return domain.SkillPolicyCatalogResponse{
			Available: false,
			Source:    "worker_capabilities",
			Warning:   "python-ai worker URL이 설정되지 않아 skill policy catalog를 조회하지 못했습니다.",
		}, nil
	}
	return domain.SkillPolicyCatalogResponse{
		Available: true,
		Source:    "worker_capabilities",
		Items:     append([]domain.SkillPolicyMetadata(nil), capabilities.SkillPolicyCatalog...),
	}, nil
}

func (s *DatasetService) ValidateSkillPolicies() (domain.SkillPolicyValidationResponse, error) {
	capabilities, err := s.fetchWorkerCapabilities()
	if err != nil {
		return domain.SkillPolicyValidationResponse{
			Available: false,
			Source:    "worker_capabilities",
			Valid:     false,
			Warning:   fmt.Sprintf("python-ai worker capability 조회에 실패했습니다: %v", err),
		}, nil
	}
	if capabilities == nil {
		return domain.SkillPolicyValidationResponse{
			Available: false,
			Source:    "worker_capabilities",
			Valid:     false,
			Warning:   "python-ai worker URL이 설정되지 않아 skill policy validation을 조회하지 못했습니다.",
		}, nil
	}
	response := capabilities.SkillPolicyValidation
	response.Available = true
	if strings.TrimSpace(response.Source) == "" {
		response.Source = "worker_capabilities"
	}
	return response, nil
}

func (s *DatasetService) availablePromptVersions() ([]string, error) {
	catalog, err := s.promptCatalog()
	if err != nil {
		return nil, err
	}
	versions := make([]string, 0, len(catalog))
	for _, item := range catalog {
		version := strings.TrimSpace(item.Version)
		if version == "" {
			continue
		}
		versions = append(versions, version)
	}
	return versions, nil
}

func (s *DatasetService) promptCatalog() ([]domain.PromptTemplateMetadata, error) {
	fileCatalog, err := s.filePromptCatalog()
	if err != nil {
		return nil, err
	}
	storedCatalog, err := s.storedPromptCatalog()
	if err != nil {
		return nil, err
	}
	if len(fileCatalog) == 0 {
		return storedCatalog, nil
	}
	if len(storedCatalog) == 0 {
		return fileCatalog, nil
	}

	merged := make(map[string]domain.PromptTemplateMetadata, len(fileCatalog)+len(storedCatalog))
	for _, item := range fileCatalog {
		key := promptMetadataKey(item.Version, item.Operation)
		merged[key] = item
	}
	for _, item := range storedCatalog {
		key := promptMetadataKey(item.Version, item.Operation)
		merged[key] = item
	}

	items := make([]domain.PromptTemplateMetadata, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Operation == items[j].Operation {
			return items[i].Version < items[j].Version
		}
		return items[i].Operation < items[j].Operation
	})
	return items, nil
}

func (s *DatasetService) filePromptCatalog() ([]domain.PromptTemplateMetadata, error) {
	dir := strings.TrimSpace(s.promptTemplatesDir)
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	catalog := make([]domain.PromptTemplateMetadata, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if !strings.HasSuffix(name, ".md") {
			continue
		}
		stem := strings.TrimSuffix(name, ".md")
		if stem == "" || stem == "README" || stem == "CHANGELOG" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		metadata := parsePromptFrontMatter(string(content))
		catalog = append(catalog, domain.PromptTemplateMetadata{
			Version:       stem,
			Title:         defaultPromptMetaValue(metadata["title"], stem),
			Operation:     defaultPromptMetaValue(metadata["operation"], inferPromptOperation(stem)),
			Status:        defaultPromptMetaValue(metadata["status"], "active"),
			Summary:       strings.TrimSpace(metadata["summary"]),
			DefaultGroups: inferPromptDefaultGroups(stem),
		})
	}
	sort.Slice(catalog, func(i, j int) bool {
		return catalog[i].Version < catalog[j].Version
	})
	return catalog, nil
}

func (s *DatasetService) storedPromptCatalog() ([]domain.PromptTemplateMetadata, error) {
	items, err := s.store.ListPrompts("")
	if err != nil {
		return nil, err
	}
	catalog := make([]domain.PromptTemplateMetadata, 0, len(items))
	for _, item := range items {
		catalog = append(catalog, domain.PromptTemplateMetadata{
			Version:       item.Version,
			Title:         item.Title,
			Operation:     item.Operation,
			Status:        item.Status,
			Summary:       item.Summary,
			DefaultGroups: inferPromptDefaultGroups(item.Version),
		})
	}
	return catalog, nil
}

func (s *DatasetService) fetchWorkerCapabilities() (*workerCapabilitiesResponse, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/capabilities", nil)
	if err != nil {
		return nil, err
	}
	client := s.httpClient
	if client == nil {
		client = &http.Client{}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("python-ai worker returned %d", resp.StatusCode)
	}
	var payload workerCapabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return &payload, nil
}
