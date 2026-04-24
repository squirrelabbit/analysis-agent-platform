package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestValidateDatasetProfilesReportsMissingPrompt(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	profilesPath := filepath.Join(t.TempDir(), "dataset_profiles.json")
	if err := os.WriteFile(profilesPath, []byte(`{
  "defaults":{"unstructured":"default-unstructured-v1"},
  "profiles":{
    "default-unstructured-v1":{
      "profile_id":"default-unstructured-v1",
      "prepare_prompt_version":"missing-prepare-v9",
      "sentiment_prompt_version":"sentiment-anthropic-v1"
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("unexpected write profile registry error: %v", err)
	}
	promptsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(promptsDir, "sentiment-anthropic-v1.md"), []byte("{{text}}"), 0o644); err != nil {
		t.Fatalf("unexpected write sentiment prompt error: %v", err)
	}
	if err := service.SetDatasetProfilesPath(profilesPath); err != nil {
		t.Fatalf("unexpected set profiles path error: %v", err)
	}
	service.SetPromptTemplatesDir(promptsDir)

	validation, err := service.ValidateDatasetProfiles()
	if err != nil {
		t.Fatalf("unexpected validate dataset profiles error: %v", err)
	}
	if validation.Valid {
		t.Fatalf("expected invalid validation result: %+v", validation)
	}
	if len(validation.Registry.AvailablePromptVersions) != 1 || validation.Registry.AvailablePromptVersions[0] != "sentiment-anthropic-v1" {
		t.Fatalf("unexpected available prompts: %+v", validation.Registry.AvailablePromptVersions)
	}
	found := false
	for _, issue := range validation.Issues {
		if issue.Code == "prepare_prompt_missing" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected prepare_prompt_missing issue: %+v", validation.Issues)
	}
}

func TestValidateDatasetProfilesUsesWorkerRuleCatalogAndScansDatasetVersions(t *testing.T) {
	repository := store.NewMemoryStore()
	promptsDir := t.TempDir()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	for name, body := range map[string]string{
		"dataset-prepare-anthropic-v1.md": "---\ntitle: Prepare\noperation: prepare\nstatus: active\nsummary: prepare\n---\n{{raw_text}}\n",
		"sentiment-anthropic-v1.md":       "---\ntitle: Sentiment\noperation: sentiment\nstatus: active\nsummary: sentiment\n---\n{{text}}\n",
	} {
		if err := os.WriteFile(filepath.Join(promptsDir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("unexpected write prompt error: %v", err)
		}
	}
	service.SetPromptTemplatesDir(promptsDir)

	profilesPath := filepath.Join(t.TempDir(), "dataset_profiles.json")
	if err := os.WriteFile(profilesPath, []byte(`{
  "defaults":{"unstructured":"default-unstructured-v1"},
  "profiles":{
    "default-unstructured-v1":{
      "profile_id":"default-unstructured-v1",
      "prepare_prompt_version":"dataset-prepare-anthropic-v1",
      "sentiment_prompt_version":"sentiment-anthropic-v1",
      "regex_rule_names":["media_placeholder"],
      "garbage_rule_names":["missing-garbage-rule"]
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("unexpected write profile registry error: %v", err)
	}
	if err := service.SetDatasetProfilesPath(profilesPath); err != nil {
		t.Fatalf("unexpected set profiles path error: %v", err)
	}

	project := domain.Project{ProjectID: "project-validate", Name: "validate", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-validate",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-validate",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Profile: &domain.DatasetProfile{
			ProfileID:        "default-unstructured-v1",
			GarbageRuleNames: []string{"missing-garbage-rule"},
		},
		PrepareStatus:    "ready",
		PreparePromptVer: datasetStringPtr("dataset-prepare-anthropic-v1"),
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"prompt_catalog": []map[string]any{
				{"version": "dataset-prepare-anthropic-v1", "operation": "prepare"},
				{"version": "sentiment-anthropic-v1", "operation": "sentiment"},
			},
			"rule_catalog": map[string]any{
				"available_prepare_regex_rule_names": []string{"media_placeholder", "html_artifact"},
				"default_prepare_regex_rule_names":   []string{"media_placeholder"},
				"available_garbage_rule_names":       []string{"ad_marker"},
				"default_garbage_rule_names":         []string{"ad_marker"},
			},
		})
	}))
	defer worker.Close()
	service.pythonAIWorkerURL = worker.URL

	validation, err := service.ValidateDatasetProfiles()
	if err != nil {
		t.Fatalf("unexpected validate dataset profiles error: %v", err)
	}
	if validation.Registry.RuleCatalog == nil {
		t.Fatalf("expected rule catalog in validation response: %+v", validation)
	}
	if len(validation.Registry.PromptCatalog) != 2 {
		t.Fatalf("expected prompt catalog metadata: %+v", validation.Registry.PromptCatalog)
	}
	foundProfileRuleIssue := false
	foundVersionRuleIssue := false
	for _, issue := range validation.Issues {
		if issue.Code == "garbage_rule_missing" && issue.Scope == "profile" {
			foundProfileRuleIssue = true
		}
		if issue.Code == "garbage_rule_missing" && issue.Scope == "dataset_version" {
			foundVersionRuleIssue = true
		}
	}
	if !foundProfileRuleIssue || !foundVersionRuleIssue {
		t.Fatalf("expected profile and dataset version garbage_rule_missing issues: %+v", validation.Issues)
	}
}

func TestSaveProjectPromptRejectsMissingRequiredPlaceholder(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prompt", Name: "prompt", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	_, err := service.SaveProjectPrompt(project.ProjectID, domain.ProjectPromptUpsertRequest{
		Version:   "project-prepare-v1",
		Operation: "prepare",
		Content:   "---\ntitle: Broken prepare\noperation: prepare\n---\n고정 프롬프트\n",
	})
	if err == nil || !strings.Contains(err.Error(), "missing placeholders") {
		t.Fatalf("expected missing placeholder error, got %v", err)
	}
}

func TestSaveProjectPromptRejectsDuplicateVersionAndOperation(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prompt-duplicate", Name: "prompt", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	_, err := service.SaveProjectPrompt(project.ProjectID, domain.ProjectPromptUpsertRequest{
		Version:   "project-prepare-v1",
		Operation: "prepare",
		Content:   "---\ntitle: Prepare\noperation: prepare\n---\n{{raw_text}}\n",
	})
	if err != nil {
		t.Fatalf("unexpected initial save error: %v", err)
	}

	_, err = service.SaveProjectPrompt(project.ProjectID, domain.ProjectPromptUpsertRequest{
		Version:   "project-prepare-v1",
		Operation: "prepare",
		Content:   "---\ntitle: Prepare 2\noperation: prepare\n---\n{{raw_text}}\n",
	})
	var conflict ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("expected conflict error, got %v", err)
	}
}

func TestUpdateProjectPromptDefaultsRejectsMissingProjectPromptVersion(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prompt-defaults", Name: "prompt", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	_, err := service.UpdateProjectPromptDefaults(project.ProjectID, domain.ProjectPromptDefaultsUpdateRequest{
		PreparePromptVersion: datasetStringPtr("missing-prepare-v1"),
	})
	if err == nil || !strings.Contains(err.Error(), "prepare default prompt version") {
		t.Fatalf("expected invalid default prompt version error, got %v", err)
	}
}

func TestValidateDatasetProfilesAcceptsProjectPromptVersionReference(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-project-prompt", Name: "project prompt", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-project-prompt",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	if err := repository.SaveProjectPrompt(domain.ProjectPrompt{
		ProjectID:   project.ProjectID,
		Version:     "project-prepare-v1",
		Operation:   "prepare",
		Title:       "Project prepare",
		Status:      "active",
		Content:     "---\ntitle: Project prepare\noperation: prepare\n---\n{{raw_text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save project prompt error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-project-prompt",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Profile: &domain.DatasetProfile{
			PreparePromptVersion: datasetStringPtr("project-prepare-v1"),
		},
		PrepareStatus: "queued",
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	validation, err := service.ValidateDatasetProfiles()
	if err != nil {
		t.Fatalf("unexpected validate dataset profiles error: %v", err)
	}
	for _, issue := range validation.Issues {
		if issue.Code == "prepare_prompt_missing" {
			t.Fatalf("expected project prompt reference to pass validation: %+v", validation.Issues)
		}
	}
}

func TestGetPromptCatalogFallsBackToWorkerCapabilities(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"prompt_catalog": []map[string]any{
				{"version": "dataset-prepare-anthropic-v1", "operation": "prepare", "title": "Prepare"},
			},
			"rule_catalog": map[string]any{},
		})
	}))
	defer worker.Close()
	service.pythonAIWorkerURL = worker.URL

	response, err := service.GetPromptCatalog()
	if err != nil {
		t.Fatalf("unexpected get prompt catalog error: %v", err)
	}
	if len(response.Items) != 1 || response.Items[0].Version != "dataset-prepare-anthropic-v1" {
		t.Fatalf("unexpected prompt catalog fallback response: %+v", response)
	}
}

func TestGetRuleCatalogReturnsUnavailableWhenWorkerNotConfigured(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	response, err := service.GetRuleCatalog()
	if err != nil {
		t.Fatalf("unexpected get rule catalog error: %v", err)
	}
	if response.Available {
		t.Fatalf("expected unavailable rule catalog response: %+v", response)
	}
	if strings.TrimSpace(response.Warning) == "" {
		t.Fatalf("expected rule catalog warning: %+v", response)
	}
}

func TestGetSkillPolicyCatalogFallsBackToWorkerCapabilities(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"prompt_catalog": []map[string]any{},
			"rule_catalog":   map[string]any{},
			"skill_policy_catalog": []map[string]any{
				{"version": "embedding-cluster-v1", "skill_name": "embedding_cluster", "policy_hash": "abc123"},
			},
			"skill_policy_validation": map[string]any{
				"valid": true,
				"catalog": []map[string]any{
					{"version": "embedding-cluster-v1", "skill_name": "embedding_cluster", "policy_hash": "abc123"},
				},
			},
		})
	}))
	defer worker.Close()
	service.pythonAIWorkerURL = worker.URL

	response, err := service.GetSkillPolicyCatalog()
	if err != nil {
		t.Fatalf("unexpected get skill policy catalog error: %v", err)
	}
	if !response.Available || len(response.Items) != 1 || response.Items[0].Version != "embedding-cluster-v1" {
		t.Fatalf("unexpected skill policy catalog response: %+v", response)
	}

	validation, err := service.ValidateSkillPolicies()
	if err != nil {
		t.Fatalf("unexpected validate skill policies error: %v", err)
	}
	if !validation.Available || !validation.Valid || len(validation.Catalog) != 1 {
		t.Fatalf("unexpected skill policy validation response: %+v", validation)
	}
}

func TestValidateSkillPoliciesReturnsUnavailableWhenWorkerNotConfigured(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	response, err := service.ValidateSkillPolicies()
	if err != nil {
		t.Fatalf("unexpected validate skill policies error: %v", err)
	}
	if response.Available {
		t.Fatalf("expected unavailable skill policy validation response: %+v", response)
	}
	if strings.TrimSpace(response.Warning) == "" {
		t.Fatalf("expected skill policy validation warning: %+v", response)
	}
}
