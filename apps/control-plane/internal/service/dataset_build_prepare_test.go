package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestBuildPrepareSetsReadyStatusAndMetadata(t *testing.T) {
	repository := store.NewMemoryStore()
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	service := NewDatasetService(repository, "", uploadRoot, artifactRoot)

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

	var requestedPath string
	var requestedOutputPath string
	var requestedLLMMode string
	var requestedModel string
	var requestedProgressPath string
	var requestedMaxRows int
	var requestedBatchSize int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPath = r.URL.Path
		requestedOutputPath = payload["output_path"].(string)
		requestedLLMMode = payload["llm_mode"].(string)
		requestedModel = payload["model"].(string)
		requestedProgressPath = payload["progress_path"].(string)
		requestedMaxRows = intValue(payload["max_rows"])
		requestedBatchSize = intValue(payload["prepare_batch_size"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"prepare completed"},
			"artifact": map[string]any{
				"skill_name":               "dataset_prepare",
				"prepare_uri":              "/tmp/issues.prepared.parquet",
				"prepared_ref":             "/tmp/issues.prepared.parquet",
				"prepare_format":           "parquet",
				"prepare_model":            "claude-haiku-4-5",
				"prepare_prompt_version":   "dataset-prepare-anthropic-v1",
				"progress_ref":             requestedProgressPath,
				"max_rows":                 requestedMaxRows,
				"prepared_text_column":     "normalized_text",
				"row_id_column":            "row_id",
				"storage_contract_version": "unstructured-storage-v1",
				"usage": map[string]any{
					"provider":               "anthropic",
					"model":                  "claude-haiku-4-5",
					"operation":              "dataset_prepare",
					"request_count":          2,
					"input_tokens":           120,
					"output_tokens":          40,
					"total_tokens":           160,
					"cost_estimation_status": "not_configured",
				},
				"summary": map[string]any{
					"input_row_count":  10,
					"output_row_count": 7,
					"kept_count":       6,
					"review_count":     1,
					"dropped_count":    3,
				},
			},
		})
	}))
	defer server.Close()

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}
	service.pythonAIWorkerURL = server.URL

	version, err = service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{
		TextColumns: []string{"text"},
		Model:       datasetStringPtr("claude-haiku-4-5"),
		MaxRows:     datasetIntPtr(10),
		BatchSize:   datasetIntPtr(4),
	})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}

	if requestedPath != "/tasks/dataset_prepare" {
		t.Fatalf("unexpected worker path: %s", requestedPath)
	}
	if requestedLLMMode != "default" {
		t.Fatalf("unexpected prepare llm mode: %s", requestedLLMMode)
	}
	if requestedModel != "claude-haiku-4-5" {
		t.Fatalf("unexpected prepare model: %s", requestedModel)
	}
	if requestedMaxRows != 10 {
		t.Fatalf("unexpected prepare max rows: %d", requestedMaxRows)
	}
	if requestedBatchSize != 4 {
		t.Fatalf("unexpected prepare batch size: %d", requestedBatchSize)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected prepare output path: %s", requestedOutputPath)
	}
	if !strings.HasPrefix(requestedProgressPath, artifactRoot) {
		t.Fatalf("unexpected prepare progress path: %s", requestedProgressPath)
	}
	if version.PrepareStatus != "ready" {
		t.Fatalf("unexpected prepare status: %s", version.PrepareStatus)
	}
	if totalTokens := metadataUsageInt(t, version.Metadata, "prepare_usage", "total_tokens"); totalTokens != 160 {
		t.Fatalf("unexpected prepare usage: %+v", version.Metadata["prepare_usage"])
	}
	if version.PrepareURI == nil || *version.PrepareURI != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected prepare uri: %+v", version.PrepareURI)
	}
	if got := metadataString(version.Metadata, "prepared_text_column", ""); got != "normalized_text" {
		t.Fatalf("unexpected prepared text column: %s", got)
	}
	if got := metadataString(version.Metadata, "prepared_ref", ""); got != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected prepared ref: %s", got)
	}
	if got := metadataString(version.Metadata, "prepared_format", ""); got != "parquet" {
		t.Fatalf("unexpected prepared format: %s", got)
	}
	if got := metadataString(version.Metadata, "row_id_column", ""); got != "row_id" {
		t.Fatalf("unexpected row id column: %s", got)
	}
	if got := metadataString(version.Metadata, "prepare_progress_ref", ""); got != requestedProgressPath {
		t.Fatalf("unexpected prepare progress ref: %s", got)
	}
	if version.PrepareSummary == nil || version.PrepareSummary.OutputRowCount != 7 {
		t.Fatalf("unexpected prepare summary: %+v", version.PrepareSummary)
	}
	if version.RecordCount == nil || *version.RecordCount != 7 {
		t.Fatalf("unexpected record count: %+v", version.RecordCount)
	}
	if version.PrepareLLMMode != "default" {
		t.Fatalf("unexpected stored prepare llm mode: %s", version.PrepareLLMMode)
	}
}

func TestBuildPrepareSupportsMultipleTextColumns(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-multi-prepare", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-multi-prepare",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	var requestedTextColumn string
	var requestedTextColumns []string
	var requestedTextJoiner string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedTextColumn = payload["text_column"].(string)
		requestedTextColumns = valuesAsStrings(t, payload["text_columns"])
		requestedTextJoiner = payload["text_joiner"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"prepare completed"},
			"artifact": map[string]any{
				"prepare_uri":              "/tmp/issues.prepared.parquet",
				"prepared_ref":             "/tmp/issues.prepared.parquet",
				"prepare_format":           "parquet",
				"prepared_text_column":     "normalized_text",
				"row_id_column":            "row_id",
				"storage_contract_version": "unstructured-storage-v1",
				"summary": map[string]any{
					"input_row_count":  2,
					"output_row_count": 2,
					"kept_count":       2,
					"review_count":     0,
					"dropped_count":    0,
					"text_column":      "제목 + 본문",
					"text_columns":     []string{"제목", "본문"},
					"text_joiner":      "\n\n",
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	version := domain.DatasetVersion{
		DatasetVersionID: "version-multi-prepare",
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

	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{
		TextColumns: []string{"제목", "본문"},
		Force:       datasetBoolPtr(true),
	})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}

	if requestedTextColumn != "제목 + 본문" {
		t.Fatalf("unexpected worker text_column: %s", requestedTextColumn)
	}
	if !reflect.DeepEqual(requestedTextColumns, []string{"제목", "본문"}) {
		t.Fatalf("unexpected worker text_columns: %+v", requestedTextColumns)
	}
	if requestedTextJoiner != "\n\n" {
		t.Fatalf("unexpected worker text_joiner: %q", requestedTextJoiner)
	}
	if got := metadataString(result.Metadata, "raw_text_column", ""); got != "제목 + 본문" {
		t.Fatalf("unexpected raw_text_column: %s", got)
	}
	if got := metadataStringList(result.Metadata, "raw_text_columns"); !reflect.DeepEqual(got, []string{"제목", "본문"}) {
		t.Fatalf("unexpected raw_text_columns: %+v", got)
	}
	if result.PrepareSummary == nil || !reflect.DeepEqual(result.PrepareSummary.TextColumns, []string{"제목", "본문"}) {
		t.Fatalf("unexpected prepare summary: %+v", result.PrepareSummary)
	}
}

func TestBuildPrepareRequiresTextColumns(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-required-columns", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-required-columns",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-required-columns",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "queued",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	_, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{})
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestBuildPrepareSampleReturnsTableColumns(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-sample-columns", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-sample-columns",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-sample-columns",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		PrepareStatus:    "not_requested",
		SentimentStatus:  "not_requested",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		outputPath := payload["output_path"].(string)
		writePreparedPreviewParquet(t, outputPath, []map[string]any{
			{
				"source_row_index":    0,
				"row_id":              "row-0",
				"raw_text":            "raw",
				"normalized_text":     "normalized",
				"prepare_disposition": "keep",
				"prepare_reason":      "valid",
			},
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepared_ref":   outputPath,
				"prepare_format": "parquet",
				"summary": map[string]any{
					"input_row_count":  1,
					"output_row_count": 1,
					"kept_count":       1,
					"review_count":     0,
					"dropped_count":    0,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	response, err := service.BuildPrepareSample(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{
		MaxRows: datasetIntPtr(1),
	})
	if err != nil {
		t.Fatalf("unexpected build prepare sample error: %v", err)
	}
	if len(response.Columns) != 6 {
		t.Fatalf("unexpected sample columns: %+v", response.Columns)
	}
	if response.Columns[0].Key != "source_row_index" || response.Columns[0].Type != "number" {
		t.Fatalf("unexpected first sample column: %+v", response.Columns[0])
	}
	if response.Columns[3].Key != "normalized_text" || response.Columns[3].Label == "" {
		t.Fatalf("unexpected normalized text column: %+v", response.Columns[3])
	}
	if len(response.Samples) != 1 || response.Samples[0].NormalizedText != "normalized" {
		t.Fatalf("unexpected sample rows: %+v", response.Samples)
	}
}

func TestBuildPrepareUsesProfileDefaults(t *testing.T) {
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
	version := domain.DatasetVersion{
		DatasetVersionID: "version-profile",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		Profile: &domain.DatasetProfile{
			ProfileID:            "festival-default",
			PreparePromptVersion: datasetStringPtr("dataset-prepare-anthropic-batch-v2"),
			RegexRuleNames:       []string{"media_placeholder", "url_cleanup"},
		},
		PrepareStatus: "queued",
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedPromptVersion string
	var requestedRegexRules []any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPromptVersion = payload["prepare_prompt_version"].(string)
		requestedRegexRules = payload["regex_rule_names"].([]any)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":            "/tmp/issues.prepared.parquet",
				"prepared_ref":           "/tmp/issues.prepared.parquet",
				"prepare_format":         "parquet",
				"prepare_prompt_version": "dataset-prepare-anthropic-batch-v2",
				"prepared_text_column":   "normalized_text",
				"summary": map[string]any{
					"output_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}

	if requestedPromptVersion != "dataset-prepare-anthropic-batch-v2" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if len(requestedRegexRules) != 2 {
		t.Fatalf("unexpected regex rule payload: %+v", requestedRegexRules)
	}
	if result.PreparePromptVer == nil || *result.PreparePromptVer != "dataset-prepare-anthropic-batch-v2" {
		t.Fatalf("unexpected stored prepare prompt version: %+v", result.PreparePromptVer)
	}
}

func TestBuildPrepareUsesProjectPromptTemplateOverride(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-override", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-override",
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
		Content:     "---\ntitle: Project prepare\noperation: prepare\n---\n프로젝트 전용 전처리\n{{raw_text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save project prompt error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-override",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		Profile: &domain.DatasetProfile{
			PreparePromptVersion: datasetStringPtr("project-prepare-v1"),
		},
		PrepareStatus: "queued",
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedPromptVersion string
	var requestedPromptTemplate string
	var requestedBatchSize int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPromptVersion = payload["prepare_prompt_version"].(string)
		requestedPromptTemplate = payload["prepare_prompt_template"].(string)
		requestedBatchSize, _ = anyToInt(payload["prepare_batch_size"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":            "/tmp/issues.prepared.parquet",
				"prepared_ref":           "/tmp/issues.prepared.parquet",
				"prepare_format":         "parquet",
				"prepare_prompt_version": "project-prepare-v1",
				"prepared_text_column":   "normalized_text",
				"summary": map[string]any{
					"output_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}
	if requestedPromptVersion != "project-prepare-v1" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{raw_text}}") {
		t.Fatalf("expected project prompt template in payload: %s", requestedPromptTemplate)
	}
	if requestedBatchSize != 1 {
		t.Fatalf("expected row-only fallback batch size, got %d", requestedBatchSize)
	}
	if result.PreparePromptVer == nil || *result.PreparePromptVer != "project-prepare-v1" {
		t.Fatalf("unexpected stored prepare prompt version: %+v", result.PreparePromptVer)
	}
}

func TestBuildPrepareUsesGlobalPromptTemplateOverride(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-global", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-global",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	if err := repository.SavePrompt(domain.Prompt{
		PromptID:    "global-prepare-prompt",
		Version:     "global-prepare-v1",
		Operation:   "prepare",
		Title:       "Global prepare",
		Status:      "active",
		Content:     "---\ntitle: Global prepare\noperation: prepare\n---\n글로벌 전처리\n{{raw_text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save global prompt error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-global",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		Profile: &domain.DatasetProfile{
			PreparePromptVersion: datasetStringPtr("global-prepare-v1"),
		},
		PrepareStatus: "queued",
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedPromptVersion string
	var requestedPromptTemplate string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPromptVersion = payload["prepare_prompt_version"].(string)
		requestedPromptTemplate = payload["prepare_prompt_template"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":            "/tmp/issues.prepared.parquet",
				"prepared_ref":           "/tmp/issues.prepared.parquet",
				"prepare_format":         "parquet",
				"prepare_prompt_version": "global-prepare-v1",
				"prepared_text_column":   "normalized_text",
				"summary": map[string]any{
					"output_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}
	if requestedPromptVersion != "global-prepare-v1" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{raw_text}}") {
		t.Fatalf("expected global prompt template in payload: %s", requestedPromptTemplate)
	}
	if result.PreparePromptVer == nil || *result.PreparePromptVer != "global-prepare-v1" {
		t.Fatalf("unexpected stored prepare prompt version: %+v", result.PreparePromptVer)
	}
}

func TestBuildPrepareUsesProjectPromptDefaultWhenProfilePromptVersionMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-default", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-default",
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
		Version:     "project-prepare-v2",
		Operation:   "prepare",
		Title:       "Project prepare",
		Status:      "active",
		Content:     "---\ntitle: Project prepare\noperation: prepare\n---\n프로젝트 기본 전처리\n{{raw_text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save project prompt error: %v", err)
	}
	now := time.Now().UTC()
	if err := repository.SaveProjectPromptDefaults(domain.ProjectPromptDefaults{
		ProjectID:            project.ProjectID,
		PreparePromptVersion: datasetStringPtr("project-prepare-v2"),
		UpdatedAt:            &now,
	}); err != nil {
		t.Fatalf("unexpected save project prompt defaults error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-default",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata:         map[string]any{"text_columns": []string{"text"}},
		PrepareStatus:    "queued",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedPromptVersion string
	var requestedPromptTemplate string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPromptVersion = payload["prepare_prompt_version"].(string)
		requestedPromptTemplate = payload["prepare_prompt_template"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":            "/tmp/issues.prepared.parquet",
				"prepared_ref":           "/tmp/issues.prepared.parquet",
				"prepare_format":         "parquet",
				"prepare_prompt_version": "project-prepare-v2",
				"prepared_text_column":   "normalized_text",
				"summary": map[string]any{
					"output_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}
	if requestedPromptVersion != "project-prepare-v2" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{raw_text}}") {
		t.Fatalf("expected project default prompt template in payload: %s", requestedPromptTemplate)
	}
	if result.PreparePromptVer == nil || *result.PreparePromptVer != "project-prepare-v2" {
		t.Fatalf("unexpected stored prepare prompt version: %+v", result.PreparePromptVer)
	}
}
