package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPath = r.URL.Path
		requestedOutputPath = payload["output_path"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"prepare completed"},
			"artifact": map[string]any{
				"skill_name":               "dataset_prepare",
				"prepare_uri":              "/tmp/issues.prepared.jsonl",
				"prepared_ref":             "/tmp/issues.prepared.jsonl",
				"prepare_format":           "jsonl",
				"prepare_model":            "claude-haiku-test",
				"prepare_prompt_version":   "dataset-prepare-anthropic-v1",
				"prepared_text_column":     "normalized_text",
				"row_id_column":            "row_id",
				"storage_contract_version": "unstructured-storage-v1",
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
	service.pythonAIWorkerURL = server.URL

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	version, err = service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{
		TextColumn: datasetStringPtr("text"),
	})
	if err != nil {
		t.Fatalf("unexpected build prepare error: %v", err)
	}

	if requestedPath != "/tasks/dataset_prepare" {
		t.Fatalf("unexpected worker path: %s", requestedPath)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected prepare output path: %s", requestedOutputPath)
	}
	if version.PrepareStatus != "ready" {
		t.Fatalf("unexpected prepare status: %s", version.PrepareStatus)
	}
	if version.PrepareURI == nil || *version.PrepareURI != "/tmp/issues.prepared.jsonl" {
		t.Fatalf("unexpected prepare uri: %+v", version.PrepareURI)
	}
	if got := metadataString(version.Metadata, "prepared_text_column", ""); got != "normalized_text" {
		t.Fatalf("unexpected prepared text column: %s", got)
	}
	if got := metadataString(version.Metadata, "prepared_ref", ""); got != "/tmp/issues.prepared.jsonl" {
		t.Fatalf("unexpected prepared ref: %s", got)
	}
	if got := metadataString(version.Metadata, "prepared_format", ""); got != "jsonl" {
		t.Fatalf("unexpected prepared format: %s", got)
	}
	if got := metadataString(version.Metadata, "row_id_column", ""); got != "row_id" {
		t.Fatalf("unexpected row id column: %s", got)
	}
	if version.RecordCount == nil || *version.RecordCount != 7 {
		t.Fatalf("unexpected record count: %+v", version.RecordCount)
	}
}

func TestBuildEmbeddingsUsesPreparedDatasetWhenReady(t *testing.T) {
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
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.jsonl"),
		EmbeddingStatus: "queued",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedDatasetName string
	var requestedTextColumn string
	var requestedOutputPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedDatasetName = payload["dataset_name"].(string)
		requestedTextColumn = payload["text_column"].(string)
		requestedOutputPath = payload["output_path"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"embedding completed"},
			"artifact": map[string]any{
				"embedding_uri":            "/tmp/issues.prepared.jsonl.embeddings.jsonl",
				"embedding_ref":            "/tmp/issues.prepared.jsonl.embeddings.jsonl",
				"embedding_format":         "jsonl",
				"embedding_model":          "token-overlap-v1",
				"document_count":           7,
				"row_id_column":            "row_id",
				"chunk_id_column":          "chunk_id",
				"chunking_strategy":        "row",
				"storage_contract_version": "unstructured-storage-v1",
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildEmbeddings(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetEmbeddingBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build embeddings error: %v", err)
	}

	if requestedDatasetName != "/tmp/issues.prepared.jsonl" {
		t.Fatalf("unexpected embedding dataset source: %s", requestedDatasetName)
	}
	if requestedTextColumn != "normalized_text" {
		t.Fatalf("unexpected embedding text column: %s", requestedTextColumn)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected embedding output path: %s", requestedOutputPath)
	}
	if result.EmbeddingStatus != "ready" {
		t.Fatalf("unexpected embedding status: %s", result.EmbeddingStatus)
	}
	if got := metadataString(result.Metadata, "embedding_ref", ""); got != "/tmp/issues.prepared.jsonl.embeddings.jsonl" {
		t.Fatalf("unexpected embedding ref: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_format", ""); got != "jsonl" {
		t.Fatalf("unexpected embedding format: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_id_column", ""); got != "chunk_id" {
		t.Fatalf("unexpected chunk id column: %s", got)
	}
}

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
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.jsonl"),
		SentimentStatus: "queued",
		EmbeddingStatus: "not_requested",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedDatasetName string
	var requestedTextColumn string
	var requestedPath string
	var requestedOutputPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPath = r.URL.Path
		requestedDatasetName = payload["dataset_name"].(string)
		requestedTextColumn = payload["text_column"].(string)
		requestedOutputPath = payload["output_path"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"sentiment completed"},
			"artifact": map[string]any{
				"sentiment_uri":               "/tmp/issues.prepared.jsonl.sentiment.jsonl",
				"sentiment_ref":               "/tmp/issues.prepared.jsonl.sentiment.jsonl",
				"sentiment_format":            "jsonl",
				"sentiment_model":             "claude-haiku-test",
				"sentiment_prompt_version":    "sentiment-anthropic-v1",
				"sentiment_label_column":      "sentiment_label",
				"sentiment_confidence_column": "sentiment_confidence",
				"sentiment_reason_column":     "sentiment_reason",
				"row_id_column":               "row_id",
				"storage_contract_version":    "unstructured-storage-v1",
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
	if requestedDatasetName != "/tmp/issues.prepared.jsonl" {
		t.Fatalf("unexpected sentiment dataset source: %s", requestedDatasetName)
	}
	if requestedTextColumn != "normalized_text" {
		t.Fatalf("unexpected sentiment text column: %s", requestedTextColumn)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected sentiment output path: %s", requestedOutputPath)
	}
	if result.SentimentStatus != "ready" {
		t.Fatalf("unexpected sentiment status: %s", result.SentimentStatus)
	}
	if result.SentimentURI == nil || *result.SentimentURI != "/tmp/issues.prepared.jsonl.sentiment.jsonl" {
		t.Fatalf("unexpected sentiment uri: %+v", result.SentimentURI)
	}
	if got := metadataString(result.Metadata, "sentiment_label_column", ""); got != "sentiment_label" {
		t.Fatalf("unexpected sentiment label column: %s", got)
	}
	if got := metadataString(result.Metadata, "sentiment_ref", ""); got != "/tmp/issues.prepared.jsonl.sentiment.jsonl" {
		t.Fatalf("unexpected sentiment ref: %s", got)
	}
	if got := metadataString(result.Metadata, "sentiment_format", ""); got != "jsonl" {
		t.Fatalf("unexpected sentiment format: %s", got)
	}
}

func datasetStringPtr(value string) *string {
	return &value
}
