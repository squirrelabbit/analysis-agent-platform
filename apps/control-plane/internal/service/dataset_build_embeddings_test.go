package service

import (
	"bufio"
	"encoding/json"
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

func TestBuildEmbeddingsUsesPreparedDatasetWhenReady(t *testing.T) {
	repository := &embeddingIndexCaptureStore{MemoryStore: store.NewMemoryStore()}
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
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		EmbeddingStatus: "queued",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedDatasetName string
	var requestedTextColumn string
	var requestedOutputPath string
	var requestedIndexOutputPath string
	var requestedEmbeddingModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedDatasetName = payload["dataset_name"].(string)
		requestedTextColumn = payload["text_column"].(string)
		if value, ok := payload["output_path"].(string); ok {
			requestedOutputPath = value
		}
		requestedIndexOutputPath = payload["index_output_path"].(string)
		requestedEmbeddingModel = payload["embedding_model"].(string)
		if requestedOutputPath != "" {
			if err := os.WriteFile(requestedOutputPath, []byte(strings.Join([]string{
				`{"source_index":0,"row_id":"version-1:row:0","chunk_id":"version-1:row:0:chunk:0","chunk_index":0,"char_start":0,"char_end":16,"token_counts":{"결제":1,"오류":1}}`,
				`{"source_index":1,"row_id":"version-1:row:1","chunk_id":"version-1:row:1:chunk:0","chunk_index":0,"char_start":0,"char_end":21,"token_counts":{"로그인":1,"오류":1}}`,
			}, "\n")), 0o644); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
		}
		writeEmbeddingIndexParquet(t, requestedIndexOutputPath, []map[string]any{
			{
				"source_index":       0,
				"row_id":             "version-1:row:0",
				"chunk_id":           "version-1:row:0:chunk:0",
				"chunk_index":        0,
				"char_start":         0,
				"char_end":           16,
				"embedding_json":     "",
				"embedding_dim":      0,
				"embedding_provider": "",
				"token_counts_json":  `{"결제":1,"오류":1}`,
			},
			{
				"source_index":       1,
				"row_id":             "version-1:row:1",
				"chunk_id":           "version-1:row:1:chunk:0",
				"chunk_index":        0,
				"char_start":         0,
				"char_end":           21,
				"embedding_json":     "",
				"embedding_dim":      0,
				"embedding_provider": "",
				"token_counts_json":  `{"로그인":1,"오류":1}`,
			},
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"embedding completed"},
			"artifact": map[string]any{
				"embedding_uri":                  "",
				"embedding_ref":                  "",
				"embedding_format":               "",
				"embedding_debug_export_enabled": false,
				"embedding_index_source_ref":     requestedIndexOutputPath,
				"embedding_index_source_format":  "parquet",
				"chunk_ref":                      "/tmp/issues.prepared.parquet.chunks.parquet",
				"chunk_format":                   "parquet",
				"embedding_model":                "token-overlap-v1",
				"document_count":                 7,
				"source_row_count":               5,
				"chunk_count":                    7,
				"row_id_column":                  "row_id",
				"chunk_id_column":                "chunk_id",
				"chunk_index_column":             "chunk_index",
				"chunk_text_column":              "chunk_text",
				"chunking_strategy":              "text-window-v1",
				"storage_contract_version":       "unstructured-storage-v1",
				"usage": map[string]any{
					"provider":               "token-overlap",
					"model":                  "token-overlap-v1",
					"operation":              "embedding",
					"request_count":          1,
					"input_text_count":       7,
					"vector_count":           7,
					"cost_estimation_status": "free_fallback",
					"estimated_cost_usd":     0.0,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildEmbeddings(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetEmbeddingBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build embeddings error: %v", err)
	}

	if requestedDatasetName != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected embedding dataset source: %s", requestedDatasetName)
	}
	if requestedTextColumn != "normalized_text" {
		t.Fatalf("unexpected embedding text column: %s", requestedTextColumn)
	}
	if requestedEmbeddingModel != DefaultEmbeddingModel {
		t.Fatalf("unexpected embedding model: %s", requestedEmbeddingModel)
	}
	if requestedOutputPath != "" {
		t.Fatalf("embedding output path should be empty without debug export: %s", requestedOutputPath)
	}
	if !strings.HasPrefix(requestedIndexOutputPath, artifactRoot) {
		t.Fatalf("unexpected embedding index output path: %s", requestedIndexOutputPath)
	}
	if result.EmbeddingStatus != "ready" {
		t.Fatalf("unexpected embedding status: %s", result.EmbeddingStatus)
	}
	if vectorCount := metadataUsageInt(t, result.Metadata, "embedding_usage", "vector_count"); vectorCount != 7 {
		t.Fatalf("unexpected embedding usage: %+v", result.Metadata["embedding_usage"])
	}
	if got := metadataString(result.Metadata, "embedding_ref", ""); got != "" {
		t.Fatalf("unexpected embedding ref: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_ref", ""); got != "/tmp/issues.prepared.parquet.chunks.parquet" {
		t.Fatalf("unexpected chunk ref: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_format", ""); got != "parquet" {
		t.Fatalf("unexpected chunk format: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_format", ""); got != "" {
		t.Fatalf("unexpected embedding format: %s", got)
	}
	if result.EmbeddingURI != nil {
		t.Fatalf("embedding uri should be empty by default: %+v", result.EmbeddingURI)
	}
	if got := metadataString(result.Metadata, "embedding_debug_export_jsonl", ""); got != "false" {
		t.Fatalf("unexpected embedding debug export flag: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_index_source_ref", ""); got != requestedIndexOutputPath {
		t.Fatalf("unexpected embedding index source ref: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_index_source_format", ""); got != "parquet" {
		t.Fatalf("unexpected embedding index source format: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_id_column", ""); got != "chunk_id" {
		t.Fatalf("unexpected chunk id column: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_index_column", ""); got != "chunk_index" {
		t.Fatalf("unexpected chunk index column: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_text_column", ""); got != "chunk_text" {
		t.Fatalf("unexpected chunk text column: %s", got)
	}
	if got := metadataString(result.Metadata, "chunking_strategy", ""); got != "text-window-v1" {
		t.Fatalf("unexpected chunking strategy: %s", got)
	}
	if got := metadataString(result.Metadata, "chunk_count", ""); got != "7" {
		t.Fatalf("unexpected chunk count: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_index_backend", ""); got != "pgvector" {
		t.Fatalf("unexpected embedding index backend: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_index_ref", ""); got != "pgvector://embedding_index_chunks?dataset_version_id=version-1" {
		t.Fatalf("unexpected embedding index ref: %s", got)
	}
	if got := metadataString(result.Metadata, "embedding_vector_dim", ""); got != "64" {
		t.Fatalf("unexpected embedding vector dim: %s", got)
	}
	if repository.datasetVersionID != "version-1" {
		t.Fatalf("unexpected indexed dataset version: %s", repository.datasetVersionID)
	}
	if len(repository.records) != 2 {
		t.Fatalf("unexpected indexed record count: %d", len(repository.records))
	}
	if repository.records[0].ChunkID != "version-1:row:0:chunk:0" {
		t.Fatalf("unexpected indexed chunk id: %+v", repository.records[0])
	}
	if repository.records[0].VectorDim != 64 || len(repository.records[0].Embedding) != 64 {
		t.Fatalf("unexpected indexed vector payload: %+v", repository.records[0])
	}
}

func TestLoadEmbeddingIndexChunksPrefersDenseEmbeddings(t *testing.T) {
	embeddingPath := t.TempDir() + "/issues.embeddings.jsonl"
	handle, err := os.Create(embeddingPath)
	if err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}
	writer := bufio.NewWriter(handle)
	if _, err := writer.WriteString(`{"source_index":0,"row_id":"version-dense:row:0","chunk_id":"version-dense:row:0:chunk:0","chunk_index":0,"char_start":0,"char_end":16,"embedding":[0.1,0.2,0.3],"embedding_dim":3,"embedding_provider":"openai","token_counts":{"결제":1,"오류":1}}` + "\n"); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	records, err := loadEmbeddingIndexChunks("version-dense", embeddingPath, "/tmp/issues.chunks.parquet", "text-embedding-3-small")
	if err != nil {
		t.Fatalf("unexpected load embedding chunks error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("unexpected record count: %d", len(records))
	}
	if records[0].VectorDim != 3 {
		t.Fatalf("unexpected vector dim: %d", records[0].VectorDim)
	}
	if len(records[0].Embedding) != 3 || records[0].Embedding[0] != float32(0.1) {
		t.Fatalf("unexpected dense embedding: %+v", records[0].Embedding)
	}
	if provider, ok := records[0].Metadata["embedding_provider"].(string); !ok || provider != "openai" {
		t.Fatalf("unexpected embedding provider metadata: %+v", records[0].Metadata)
	}
}

func TestLoadEmbeddingIndexChunksFromParquetPrefersDenseEmbeddings(t *testing.T) {
	embeddingPath := filepath.Join(t.TempDir(), "issues.embeddings.index.parquet")
	writeEmbeddingIndexParquet(t, embeddingPath, []map[string]any{
		{
			"source_index":       0,
			"row_id":             "version-dense:row:0",
			"chunk_id":           "version-dense:row:0:chunk:0",
			"chunk_index":        0,
			"char_start":         0,
			"char_end":           16,
			"embedding_json":     `[0.1,0.2,0.3]`,
			"embedding_dim":      3,
			"embedding_provider": "fastembed",
			"token_counts_json":  `{"결제":1,"오류":1}`,
		},
	})

	records, err := loadEmbeddingIndexChunks("version-dense", embeddingPath, "/tmp/issues.chunks.parquet", "intfloat/multilingual-e5-small")
	if err != nil {
		t.Fatalf("unexpected load embedding chunks error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("unexpected record count: %d", len(records))
	}
	if records[0].VectorDim != 3 {
		t.Fatalf("unexpected vector dim: %d", records[0].VectorDim)
	}
	if len(records[0].Embedding) != 3 || records[0].Embedding[0] != float32(0.1) {
		t.Fatalf("unexpected dense embedding: %+v", records[0].Embedding)
	}
	if provider, ok := records[0].Metadata["embedding_provider"].(string); !ok || provider != "fastembed" {
		t.Fatalf("unexpected embedding provider metadata: %+v", records[0].Metadata)
	}
}

func TestBuildEmbeddingsAllowsModelOverride(t *testing.T) {
	repository := &embeddingIndexCaptureStore{MemoryStore: store.NewMemoryStore()}
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
		DatasetVersionID: "version-override",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		PrepareStatus: "ready",
		PrepareURI:    datasetStringPtr("/tmp/issues.prepared.parquet"),
		CreatedAt:     time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedEmbeddingModel string
	var requestedOutputPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedEmbeddingModel = payload["embedding_model"].(string)
		outputPath := payload["output_path"].(string)
		requestedOutputPath = outputPath
		if err := os.WriteFile(outputPath, []byte(`{"source_index":0,"row_id":"version-override:row:0","chunk_id":"version-override:row:0:chunk:0","chunk_index":0,"char_start":0,"char_end":16,"token_counts":{"결제":1,"오류":1}}`), 0o644); err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"embedding_uri":    outputPath,
				"embedding_ref":    outputPath,
				"embedding_format": "jsonl",
				"chunk_ref":        "/tmp/issues.prepared.parquet.chunks.parquet",
				"chunk_format":     "parquet",
				"embedding_model":  "intfloat/multilingual-e5-small",
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildEmbeddings(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetEmbeddingBuildRequest{
		EmbeddingModel:   datasetStringPtr("intfloat/multilingual-e5-small"),
		DebugExportJSONL: datasetBoolPtr(true),
	})
	if err != nil {
		t.Fatalf("unexpected build embeddings error: %v", err)
	}
	if requestedEmbeddingModel != "intfloat/multilingual-e5-small" {
		t.Fatalf("unexpected requested embedding model: %s", requestedEmbeddingModel)
	}
	if !strings.HasSuffix(requestedOutputPath, ".jsonl") {
		t.Fatalf("unexpected debug export output path: %s", requestedOutputPath)
	}
	if result.EmbeddingModel == nil || *result.EmbeddingModel != "intfloat/multilingual-e5-small" {
		t.Fatalf("unexpected embedding model: %+v", result.EmbeddingModel)
	}
	if result.EmbeddingURI == nil || strings.TrimSpace(*result.EmbeddingURI) == "" {
		t.Fatalf("expected embedding uri when debug export is enabled: %+v", result.EmbeddingURI)
	}
}

func TestBuildEmbeddingsUsesProfileModelByDefault(t *testing.T) {
	repository := &embeddingIndexCaptureStore{MemoryStore: store.NewMemoryStore()}
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
		DatasetVersionID: "version-profile-embedding",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_text_column": "normalized_text",
		},
		Profile: &domain.DatasetProfile{
			ProfileID:      "festival-default",
			EmbeddingModel: datasetStringPtr("intfloat/multilingual-e5-small"),
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr("/tmp/issues.prepared.parquet"),
		EmbeddingStatus: "queued",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	var requestedEmbeddingModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedEmbeddingModel = payload["embedding_model"].(string)
		indexOutputPath := payload["index_output_path"].(string)
		writeEmbeddingIndexParquet(t, indexOutputPath, []map[string]any{
			{
				"source_index":       0,
				"row_id":             "version-profile-embedding:row:0",
				"chunk_id":           "version-profile-embedding:row:0:chunk:0",
				"chunk_index":        0,
				"char_start":         0,
				"char_end":           16,
				"embedding_json":     `[0.1,0.2,0.3]`,
				"embedding_dim":      3,
				"embedding_provider": "fastembed",
				"token_counts_json":  `{"결제":1}`,
			},
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"embedding_index_source_ref":    indexOutputPath,
				"embedding_index_source_format": "parquet",
				"chunk_ref":                     "/tmp/issues.prepared.parquet.chunks.parquet",
				"chunk_format":                  "parquet",
				"embedding_model":               "intfloat/multilingual-e5-small",
				"embedding_provider":            "fastembed",
				"embedding_vector_dim":          3,
				"usage": map[string]any{
					"provider":               "fastembed",
					"model":                  "intfloat/multilingual-e5-small",
					"operation":              "embedding",
					"vector_count":           1,
					"cost_estimation_status": "free_fallback",
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildEmbeddings(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetEmbeddingBuildRequest{})
	if err != nil {
		t.Fatalf("unexpected build embeddings error: %v", err)
	}
	if requestedEmbeddingModel != "intfloat/multilingual-e5-small" {
		t.Fatalf("unexpected requested embedding model: %s", requestedEmbeddingModel)
	}
	if result.EmbeddingModel == nil || *result.EmbeddingModel != "intfloat/multilingual-e5-small" {
		t.Fatalf("unexpected stored embedding model: %+v", result.EmbeddingModel)
	}
}
