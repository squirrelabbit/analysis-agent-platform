package service

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestCreateDatasetVersionStoresExplicitLLMModes(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-llm-mode", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-llm-mode",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:        "/tmp/issues.csv",
		DataType:          datasetStringPtr("unstructured"),
		Metadata:          map[string]any{"text_columns": []string{"text"}},
		PrepareLLMMode:    datasetStringPtr("disabled"),
		SentimentRequired: datasetBoolPtr(true),
		SentimentLLMMode:  datasetStringPtr("enabled"),
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}
	if version.PrepareLLMMode != "disabled" {
		t.Fatalf("unexpected prepare llm mode: %s", version.PrepareLLMMode)
	}
	if version.SentimentLLMMode != "enabled" {
		t.Fatalf("unexpected sentiment llm mode: %s", version.SentimentLLMMode)
	}

	stored, err := service.GetDatasetVersion(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	if stored.PrepareLLMMode != "disabled" || stored.SentimentLLMMode != "enabled" {
		t.Fatalf("unexpected stored llm modes: prepare=%s sentiment=%s", stored.PrepareLLMMode, stored.SentimentLLMMode)
	}
}

func TestCreateDatasetVersionRejectsInvalidLLMMode(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-invalid-llm-mode", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-invalid-llm-mode",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	_, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:     "/tmp/issues.csv",
		PrepareLLMMode: datasetStringPtr("sometimes"),
	})
	if err == nil {
		t.Fatalf("expected invalid llm mode error")
	}
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("expected ErrInvalidArgument, got %T", err)
	}
}

func TestCreateDatasetVersionRequiresTextColumnsWhenPrepareRequired(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-required-columns", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-required-columns",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	_, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:      "/tmp/issues.csv",
		DataType:        datasetStringPtr("unstructured"),
		PrepareRequired: datasetBoolPtr(true),
	})
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestListDatasetsAndVersions(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-list", Name: "list", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-list",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-list",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "queued",
		SentimentStatus:  "not_requested",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	datasets, err := service.ListDatasets(project.ProjectID)
	if err != nil {
		t.Fatalf("unexpected list datasets error: %v", err)
	}
	if len(datasets.Items) != 1 || datasets.Items[0].DatasetID != dataset.DatasetID {
		t.Fatalf("unexpected dataset list response: %+v", datasets)
	}

	versions, err := service.ListDatasetVersions(project.ProjectID, dataset.DatasetID)
	if err != nil {
		t.Fatalf("unexpected list dataset versions error: %v", err)
	}
	if len(versions.Items) != 1 || versions.Items[0].DatasetVersionID != version.DatasetVersionID {
		t.Fatalf("unexpected dataset version list response: %+v", versions)
	}
}

func TestCreateDatasetVersionAutoActivatesLatestVersion(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-active-version", Name: "active", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-active-version",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	first, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues-v1.csv",
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create first dataset version error: %v", err)
	}
	if !first.IsActive {
		t.Fatalf("expected first version to be active: %+v", first)
	}

	second, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues-v2.csv",
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create second dataset version error: %v", err)
	}
	if !second.IsActive {
		t.Fatalf("expected second version to be active: %+v", second)
	}

	loadedDataset, err := service.GetDataset(project.ProjectID, dataset.DatasetID)
	if err != nil {
		t.Fatalf("unexpected get dataset error: %v", err)
	}
	if loadedDataset.ActiveDatasetVersionID == nil || *loadedDataset.ActiveDatasetVersionID != second.DatasetVersionID {
		t.Fatalf("unexpected active dataset version: %+v", loadedDataset)
	}

	loadedFirst, err := service.GetDatasetVersion(project.ProjectID, dataset.DatasetID, first.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get first version error: %v", err)
	}
	if loadedFirst.IsActive {
		t.Fatalf("expected previous active version to be inactive: %+v", loadedFirst)
	}
}

func TestDatasetVersionActivationCanBeUpdatedManually(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-manual-activate", Name: "manual", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-manual-activate",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	first, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues-v1.csv",
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create first version error: %v", err)
	}
	second, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:       "/tmp/issues-v2.csv",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		ActivateOnCreate: datasetBoolPtr(false),
	})
	if err != nil {
		t.Fatalf("unexpected create second version error: %v", err)
	}
	if second.IsActive {
		t.Fatalf("expected second version to remain inactive when activate_on_create=false: %+v", second)
	}

	updatedDataset, err := service.ActivateDatasetVersion(project.ProjectID, dataset.DatasetID, first.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected activate dataset version error: %v", err)
	}
	if updatedDataset.ActiveDatasetVersionID == nil || *updatedDataset.ActiveDatasetVersionID != first.DatasetVersionID {
		t.Fatalf("unexpected active dataset version after manual activate: %+v", updatedDataset)
	}

	deactivatedDataset, err := service.DeactivateDatasetVersion(project.ProjectID, dataset.DatasetID)
	if err != nil {
		t.Fatalf("unexpected deactivate dataset version error: %v", err)
	}
	if deactivatedDataset.ActiveDatasetVersionID != nil {
		t.Fatalf("expected dataset to have no active version: %+v", deactivatedDataset)
	}

	versions, err := service.ListDatasetVersions(project.ProjectID, dataset.DatasetID)
	if err != nil {
		t.Fatalf("unexpected list dataset versions error: %v", err)
	}
	for _, item := range versions.Items {
		if item.IsActive {
			t.Fatalf("expected all versions to be inactive after deactivation: %+v", versions.Items)
		}
	}
}

func TestCreateDatasetVersionStoresNormalizedProfile(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
		Metadata:   map[string]any{"text_columns": []string{"text"}},
		Profile: &domain.DatasetProfile{
			ProfileID:              "  festival-default  ",
			PreparePromptVersion:   datasetStringPtr("  dataset-prepare-anthropic-batch-v2 "),
			SentimentPromptVersion: datasetStringPtr(" sentiment-anthropic-v2 "),
			RegexRuleNames:         []string{"media_placeholder", "url_cleanup", "media_placeholder", " "},
			GarbageRuleNames:       []string{"ad_marker", "platform_placeholder", "ad_marker"},
			EmbeddingModel:         datasetStringPtr(" intfloat/multilingual-e5-small "),
		},
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	if version.Profile == nil {
		t.Fatalf("expected dataset profile to be stored")
	}
	if version.Profile.ProfileID != "festival-default" {
		t.Fatalf("unexpected profile id: %+v", version.Profile)
	}
	if version.Profile.PreparePromptVersion == nil || *version.Profile.PreparePromptVersion != "dataset-prepare-anthropic-batch-v2" {
		t.Fatalf("unexpected prepare prompt version: %+v", version.Profile)
	}
	if len(version.Profile.RegexRuleNames) != 2 {
		t.Fatalf("unexpected regex rule names: %+v", version.Profile.RegexRuleNames)
	}
	if len(version.Profile.GarbageRuleNames) != 2 {
		t.Fatalf("unexpected garbage rule names: %+v", version.Profile.GarbageRuleNames)
	}
	if got := metadataString(version.Metadata, "profile_id", ""); got != "festival-default" {
		t.Fatalf("unexpected metadata profile_id: %s", got)
	}
}

func TestCreateDatasetVersionResolvesDefaultProfileFromRegistry(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	registryPath := filepath.Join(t.TempDir(), "dataset_profiles.json")
	if err := os.WriteFile(registryPath, []byte(`{
  "defaults": {
    "unstructured": "default-unstructured-v1"
  },
  "profiles": {
    "default-unstructured-v1": {
      "profile_id": "default-unstructured-v1",
      "prepare_prompt_version": "dataset-prepare-anthropic-batch-v1",
      "sentiment_prompt_version": "sentiment-anthropic-v1",
      "regex_rule_names": ["media_placeholder", "url_cleanup"],
      "garbage_rule_names": ["ad_marker", "empty_or_noise"],
      "embedding_model": "intfloat/multilingual-e5-small"
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("unexpected registry write error: %v", err)
	}
	if err := service.SetDatasetProfilesPath(registryPath); err != nil {
		t.Fatalf("unexpected registry load error: %v", err)
	}

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	if version.Profile == nil {
		t.Fatalf("expected default profile to be resolved")
	}
	if version.Profile.ProfileID != "default-unstructured-v1" {
		t.Fatalf("unexpected profile id: %+v", version.Profile)
	}
	if version.Profile.EmbeddingModel == nil || *version.Profile.EmbeddingModel != "intfloat/multilingual-e5-small" {
		t.Fatalf("unexpected embedding model: %+v", version.Profile)
	}
	if got := metadataString(version.Metadata, "profile_id", ""); got != "default-unstructured-v1" {
		t.Fatalf("unexpected metadata profile_id: %s", got)
	}
}

func TestCreateDatasetVersionMergesRegistryProfileWithExplicitOverride(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	registryPath := filepath.Join(t.TempDir(), "dataset_profiles.json")
	if err := os.WriteFile(registryPath, []byte(`{
  "defaults": {
    "unstructured": "default-unstructured-v1"
  },
  "profiles": {
    "festival-default": {
      "profile_id": "festival-default",
      "prepare_prompt_version": "dataset-prepare-anthropic-batch-v1",
      "sentiment_prompt_version": "sentiment-anthropic-v1",
      "regex_rule_names": ["media_placeholder", "url_cleanup"],
      "garbage_rule_names": ["ad_marker", "empty_or_noise"],
      "embedding_model": "intfloat/multilingual-e5-small"
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("unexpected registry write error: %v", err)
	}
	if err := service.SetDatasetProfilesPath(registryPath); err != nil {
		t.Fatalf("unexpected registry load error: %v", err)
	}

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
		Metadata:   map[string]any{"text_columns": []string{"text"}},
		Profile: &domain.DatasetProfile{
			ProfileID:        "festival-default",
			EmbeddingModel:   datasetStringPtr("text-embedding-3-small"),
			GarbageRuleNames: []string{"platform_placeholder"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	if version.Profile == nil {
		t.Fatalf("expected merged profile")
	}
	if version.Profile.PreparePromptVersion == nil || *version.Profile.PreparePromptVersion != "dataset-prepare-anthropic-batch-v1" {
		t.Fatalf("unexpected merged prepare prompt: %+v", version.Profile)
	}
	if version.Profile.EmbeddingModel == nil || *version.Profile.EmbeddingModel != "text-embedding-3-small" {
		t.Fatalf("unexpected merged embedding model: %+v", version.Profile)
	}
	if len(version.Profile.GarbageRuleNames) != 1 || version.Profile.GarbageRuleNames[0] != "platform_placeholder" {
		t.Fatalf("unexpected merged garbage rules: %+v", version.Profile.GarbageRuleNames)
	}
}
