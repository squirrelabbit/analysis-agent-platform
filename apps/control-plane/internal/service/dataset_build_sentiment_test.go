package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestBuildSentimentUsesPreparedDatasetWhenReady(t *testing.T) {
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

	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		PrepareStatus:    "ready",
		PrepareURI:       datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus:  "queued",
		SentimentLLMMode: "disabled",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedDatasetName string
	var requestedTextColumn string
	var requestedPath string
	var requestedOutputPath string
	var requestedLLMMode string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPath = r.URL.Path
		requestedDatasetName = payload["dataset_name"].(string)
		requestedTextColumn = payload["text_column"].(string)
		requestedOutputPath = payload["output_path"].(string)
		requestedLLMMode = payload["llm_mode"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"sentiment completed"},
			"artifact": map[string]any{
				"sentiment_uri":               "/tmp/issues.prepared.parquet.sentiment.parquet",
				"sentiment_ref":               "/tmp/issues.prepared.parquet.sentiment.parquet",
				"sentiment_format":            "parquet",
				"sentiment_model":             "claude-haiku-test",
				"sentiment_prompt_version":    "sentiment-anthropic-v1",
				"sentiment_label_column":      "sentiment_label",
				"sentiment_confidence_column": "sentiment_confidence",
				"sentiment_reason_column":     "sentiment_reason",
				"row_id_column":               "row_id",
				"storage_contract_version":    "unstructured-storage-v1",
				"usage": map[string]any{
					"provider":               "anthropic",
					"model":                  "claude-haiku-test",
					"operation":              "sentiment_label",
					"request_count":          7,
					"input_tokens":           210,
					"output_tokens":          70,
					"total_tokens":           280,
					"cost_estimation_status": "not_configured",
				},
				"summary": map[string]any{
					"labeled_row_count": 7,
					"label_counts": map[string]any{
						"negative": 5,
						"neutral":  2,
					},
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
	}

	if requestedPath != "/tasks/sentiment_label" {
		t.Fatalf("unexpected worker path: %s", requestedPath)
	}
	if requestedDatasetName != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected sentiment dataset source: %s", requestedDatasetName)
	}
	if requestedTextColumn != "normalized_text" {
		t.Fatalf("unexpected sentiment text column: %s", requestedTextColumn)
	}
	if requestedLLMMode != "disabled" {
		t.Fatalf("unexpected sentiment llm mode: %s", requestedLLMMode)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected sentiment output path: %s", requestedOutputPath)
	}
	if result.SentimentStatus != "ready" {
		t.Fatalf("unexpected sentiment status: %s", result.SentimentStatus)
	}
	if result.SentimentURI == nil || *result.SentimentURI != "/tmp/issues.prepared.parquet.sentiment.parquet" {
		t.Fatalf("unexpected sentiment uri: %+v", result.SentimentURI)
	}
	if totalTokens := metadataUsageInt(t, result.Metadata, "sentiment_usage", "total_tokens"); totalTokens != 280 {
		t.Fatalf("unexpected sentiment usage: %+v", result.Metadata["sentiment_usage"])
	}
	if got := metadataString(result.Metadata, "sentiment_label_column", ""); got != "sentiment_label" {
		t.Fatalf("unexpected sentiment label column: %s", got)
	}
	if got := metadataString(result.Metadata, "sentiment_ref", ""); got != "/tmp/issues.prepared.parquet.sentiment.parquet" {
		t.Fatalf("unexpected sentiment ref: %s", got)
	}
	if got := metadataString(result.Metadata, "sentiment_format", ""); got != "parquet" {
		t.Fatalf("unexpected sentiment format: %s", got)
	}
	if result.SentimentLLMMode != "disabled" {
		t.Fatalf("unexpected stored sentiment llm mode: %s", result.SentimentLLMMode)
	}
}

func TestBuildSentimentSupportsMultipleTextColumns(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-multi-sentiment", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-multi-sentiment",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-multi-sentiment",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		PrepareStatus:    "ready",
		PrepareURI:       datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus:  "queued",
		SentimentLLMMode: "disabled",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
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
			"notes": []string{"sentiment completed"},
			"artifact": map[string]any{
				"sentiment_uri":               "/tmp/issues.sentiment.parquet",
				"sentiment_ref":               "/tmp/issues.sentiment.parquet",
				"sentiment_format":            "parquet",
				"sentiment_label_column":      "sentiment_label",
				"sentiment_confidence_column": "sentiment_confidence",
				"sentiment_reason_column":     "sentiment_reason",
				"row_id_column":               "row_id",
				"storage_contract_version":    "unstructured-storage-v1",
				"summary": map[string]any{
					"input_row_count":      2,
					"labeled_row_count":    2,
					"text_column":          "제목 + 본문",
					"text_columns":         []string{"제목", "본문"},
					"text_joiner":          "\n\n",
					"sentiment_batch_size": 8,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{
		TextColumns: []string{"제목", "본문"},
	})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
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
	if got := metadataString(result.Metadata, "sentiment_text_column", ""); got != "제목 + 본문" {
		t.Fatalf("unexpected sentiment_text_column: %s", got)
	}
	if got := metadataStringList(result.Metadata, "sentiment_text_columns"); !reflect.DeepEqual(got, []string{"제목", "본문"}) {
		t.Fatalf("unexpected sentiment_text_columns: %+v", got)
	}
	if summary := buildSentimentSummary(result.Metadata); summary == nil || !reflect.DeepEqual(summary.TextColumns, []string{"제목", "본문"}) {
		t.Fatalf("unexpected sentiment summary: %+v", summary)
	}
}

func TestBuildSentimentUsesProfilePromptVersion(t *testing.T) {
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
		DatasetVersionID: "version-profile-sentiment",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		Profile: &domain.DatasetProfile{
			ProfileID:              "festival-default",
			SentimentPromptVersion: datasetStringPtr("sentiment-anthropic-v2"),
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus: "queued",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedPromptVersion string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPromptVersion = payload["sentiment_prompt_version"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"sentiment_uri":            "/tmp/issues.prepared.parquet.sentiment.parquet",
				"sentiment_ref":            "/tmp/issues.prepared.parquet.sentiment.parquet",
				"sentiment_format":         "parquet",
				"sentiment_prompt_version": "sentiment-anthropic-v2",
				"summary": map[string]any{
					"labeled_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
	}
	if requestedPromptVersion != "sentiment-anthropic-v2" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if result.SentimentPromptVer == nil || *result.SentimentPromptVer != "sentiment-anthropic-v2" {
		t.Fatalf("unexpected stored sentiment prompt version: %+v", result.SentimentPromptVer)
	}
}

func TestBuildSentimentUsesProjectPromptTemplateOverride(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-sentiment-override", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-sentiment-override",
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
		Version:     "project-sentiment-v1",
		Operation:   "sentiment",
		Title:       "Project sentiment",
		Status:      "active",
		Content:     "---\ntitle: Project sentiment\noperation: sentiment\n---\n프로젝트 전용 감성\n{{text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save project prompt error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-sentiment-override",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.prepared.parquet",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		Profile: &domain.DatasetProfile{
			SentimentPromptVersion: datasetStringPtr("project-sentiment-v1"),
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus: "queued",
		CreatedAt:       time.Now().UTC(),
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
		requestedPromptVersion = payload["sentiment_prompt_version"].(string)
		requestedPromptTemplate = payload["sentiment_prompt_template"].(string)
		requestedBatchSize, _ = anyToInt(payload["sentiment_batch_size"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"sentiment_uri":            "/tmp/issues.sentiment.parquet",
				"sentiment_ref":            "/tmp/issues.sentiment.parquet",
				"sentiment_format":         "parquet",
				"sentiment_prompt_version": "project-sentiment-v1",
				"summary": map[string]any{
					"labeled_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
	}
	if requestedPromptVersion != "project-sentiment-v1" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{text}}") {
		t.Fatalf("expected project sentiment template in payload: %s", requestedPromptTemplate)
	}
	if requestedBatchSize != 1 {
		t.Fatalf("expected row-only fallback batch size, got %d", requestedBatchSize)
	}
	if result.SentimentPromptVer == nil || *result.SentimentPromptVer != "project-sentiment-v1" {
		t.Fatalf("unexpected stored sentiment prompt version: %+v", result.SentimentPromptVer)
	}
}

func TestBuildSentimentUsesGlobalPromptTemplateOverride(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-sentiment-global", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-sentiment-global",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}
	if err := repository.SavePrompt(domain.Prompt{
		PromptID:    "global-sentiment-prompt",
		Version:     "global-sentiment-v1",
		Operation:   "sentiment",
		Title:       "Global sentiment",
		Status:      "active",
		Content:     "---\ntitle: Global sentiment\noperation: sentiment\n---\n글로벌 감성\n{{text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save global prompt error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-sentiment-global",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.prepared.parquet",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		Profile: &domain.DatasetProfile{
			SentimentPromptVersion: datasetStringPtr("global-sentiment-v1"),
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus: "queued",
		CreatedAt:       time.Now().UTC(),
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
		requestedPromptVersion = payload["sentiment_prompt_version"].(string)
		requestedPromptTemplate = payload["sentiment_prompt_template"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"sentiment_uri":            "/tmp/issues.sentiment.parquet",
				"sentiment_ref":            "/tmp/issues.sentiment.parquet",
				"sentiment_format":         "parquet",
				"sentiment_prompt_version": "global-sentiment-v1",
				"summary": map[string]any{
					"labeled_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
	}
	if requestedPromptVersion != "global-sentiment-v1" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{text}}") {
		t.Fatalf("expected global sentiment template in payload: %s", requestedPromptTemplate)
	}
	if result.SentimentPromptVer == nil || *result.SentimentPromptVer != "global-sentiment-v1" {
		t.Fatalf("unexpected stored sentiment prompt version: %+v", result.SentimentPromptVer)
	}
}

func TestBuildSentimentUsesProjectPromptDefaultWhenProfilePromptVersionMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-sentiment-default", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-sentiment-default",
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
		Version:     "project-sentiment-v2",
		Operation:   "sentiment",
		Title:       "Project sentiment",
		Status:      "active",
		Content:     "---\ntitle: Project sentiment\noperation: sentiment\n---\n프로젝트 기본 감성\n{{text}}\n",
		ContentHash: "hash",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("unexpected save project prompt error: %v", err)
	}
	now := time.Now().UTC()
	if err := repository.SaveProjectPromptDefaults(domain.ProjectPromptDefaults{
		ProjectID:              project.ProjectID,
		SentimentPromptVersion: datasetStringPtr("project-sentiment-v2"),
		UpdatedAt:              &now,
	}); err != nil {
		t.Fatalf("unexpected save project prompt defaults error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-sentiment-default",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.prepared.parquet",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		SentimentStatus: "queued",
		CreatedAt:       time.Now().UTC(),
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
		requestedPromptVersion = payload["sentiment_prompt_version"].(string)
		requestedPromptTemplate = payload["sentiment_prompt_template"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"sentiment_uri":            "/tmp/issues.sentiment.parquet",
				"sentiment_ref":            "/tmp/issues.sentiment.parquet",
				"sentiment_format":         "parquet",
				"sentiment_prompt_version": "project-sentiment-v2",
				"summary": map[string]any{
					"labeled_row_count": 1,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build sentiment error: %v", err)
	}
	if requestedPromptVersion != "project-sentiment-v2" {
		t.Fatalf("unexpected requested prompt version: %s", requestedPromptVersion)
	}
	if !strings.Contains(requestedPromptTemplate, "{{text}}") {
		t.Fatalf("expected project default sentiment template in payload: %s", requestedPromptTemplate)
	}
	if result.SentimentPromptVer == nil || *result.SentimentPromptVer != "project-sentiment-v2" {
		t.Fatalf("unexpected stored sentiment prompt version: %+v", result.SentimentPromptVer)
	}
}
