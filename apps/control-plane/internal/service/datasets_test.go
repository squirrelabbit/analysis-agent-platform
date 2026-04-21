package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"

	_ "github.com/marcboeker/go-duckdb"
)

type embeddingIndexCaptureStore struct {
	*store.MemoryStore
	datasetVersionID string
	records          []domain.EmbeddingIndexChunk
}

type fakeDatasetBuildStarter struct {
	startCalls         []workflows.StartDatasetBuildInput
	analysisStartCalls []workflows.StartAnalysisInput
}

func (s *fakeDatasetBuildStarter) StartAnalysisWorkflow(input workflows.StartAnalysisInput) (string, error) {
	s.analysisStartCalls = append(s.analysisStartCalls, input)
	return "analysis-execution-" + input.ExecutionID, nil
}

func (s *fakeDatasetBuildStarter) StartDatasetBuildWorkflow(input workflows.StartDatasetBuildInput) (string, error) {
	s.startCalls = append(s.startCalls, input)
	return "dataset-build-" + input.JobID, nil
}

func (s *fakeDatasetBuildStarter) EngineName() string {
	return "temporal"
}

func (s *embeddingIndexCaptureStore) ReplaceEmbeddingChunkIndex(datasetVersionID string, records []domain.EmbeddingIndexChunk) error {
	s.datasetVersionID = datasetVersionID
	s.records = append([]domain.EmbeddingIndexChunk(nil), records...)
	return nil
}

func writeEmbeddingIndexParquet(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "embedding-index.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_index, '%s' AS row_id, '%s' AS chunk_id, %d AS chunk_index, %d AS char_start, %d AS char_end, '%s' AS embedding_json, %d AS embedding_dim, '%s' AS embedding_provider, '%s' AS token_counts_json`,
			int64Value(row["source_index"]),
			escapeDuckDBLiteral(stringValue(row["row_id"])),
			escapeDuckDBLiteral(stringValue(row["chunk_id"])),
			intValue(row["chunk_index"]),
			intValue(row["char_start"]),
			intValue(row["char_end"]),
			escapeDuckDBLiteral(stringValue(row["embedding_json"])),
			intValue(row["embedding_dim"]),
			escapeDuckDBLiteral(stringValue(row["embedding_provider"])),
			escapeDuckDBLiteral(stringValue(row["token_counts_json"])),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteral(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func writeClusterMembershipParquet(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cluster-membership.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		isSample := "FALSE"
		if boolValue(row["is_sample"]) {
			isSample = "TRUE"
		}
		selects = append(selects, fmt.Sprintf(
			`SELECT '%s' AS cluster_id, %d AS cluster_rank, %d AS cluster_document_count, %d AS source_index, '%s' AS row_id, '%s' AS chunk_id, %d AS chunk_index, '%s' AS text, %s AS is_sample`,
			escapeDuckDBLiteral(stringValue(row["cluster_id"])),
			intValue(row["cluster_rank"]),
			intValue(row["cluster_document_count"]),
			intValue(row["source_index"]),
			escapeDuckDBLiteral(stringValue(row["row_id"])),
			escapeDuckDBLiteral(stringValue(row["chunk_id"])),
			intValue(row["chunk_index"]),
			escapeDuckDBLiteral(stringValue(row["text"])),
			isSample,
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteral(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func writePreparedPreviewParquet(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "prepare-preview.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_row_index, '%s' AS row_id, '%s' AS raw_text, '%s' AS normalized_text, '%s' AS prepare_disposition, '%s' AS prepare_reason`,
			intValue(row["source_row_index"]),
			escapeDuckDBLiteral(stringValue(row["row_id"])),
			escapeDuckDBLiteral(stringValue(row["raw_text"])),
			escapeDuckDBLiteral(stringValue(row["normalized_text"])),
			escapeDuckDBLiteral(stringValue(row["prepare_disposition"])),
			escapeDuckDBLiteral(stringValue(row["prepare_reason"])),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteral(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func writeSentimentPreviewParquet(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "sentiment-preview.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_row_index, '%s' AS row_id, '%s' AS sentiment_label, %f AS sentiment_confidence, '%s' AS sentiment_reason, '%s' AS sentiment_prompt_version`,
			intValue(row["source_row_index"]),
			escapeDuckDBLiteral(stringValue(row["row_id"])),
			escapeDuckDBLiteral(stringValue(row["sentiment_label"])),
			floatValue(row["sentiment_confidence"]),
			escapeDuckDBLiteral(stringValue(row["sentiment_reason"])),
			escapeDuckDBLiteral(stringValue(row["sentiment_prompt_version"])),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteral(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func floatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	default:
		return 0
	}
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func metadataUsageInt(t *testing.T, metadata map[string]any, key string, usageKey string) int {
	t.Helper()
	usage, ok := metadata[key].(map[string]any)
	if !ok {
		t.Fatalf("unexpected %s payload: %+v", key, metadata[key])
	}
	value, ok := anyToInt(usage[usageKey])
	if !ok {
		t.Fatalf("unexpected %s.%s payload: %+v", key, usageKey, usage)
	}
	return value
}

func waitForDatasetBuildJobStatus(t *testing.T, service *DatasetService, projectID, jobID, expected string) domain.DatasetBuildJob {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.GetDatasetBuildJob(projectID, jobID)
		if err == nil && job.Status == expected {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, err := service.GetDatasetBuildJob(projectID, jobID)
	if err != nil {
		t.Fatalf("unexpected get dataset build job error: %v", err)
	}
	t.Fatalf("expected dataset build job %s status %s, got %s", jobID, expected, job.Status)
	return domain.DatasetBuildJob{}
}

func waitForDatasetVersionPrepareReady(t *testing.T, service *DatasetService, projectID, datasetID, versionID string) domain.DatasetVersion {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
		if err == nil && version.PrepareStatus == "ready" {
			return version
		}
		time.Sleep(20 * time.Millisecond)
	}
	version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	t.Fatalf("expected dataset version prepare ready, got %s", version.PrepareStatus)
	return domain.DatasetVersion{}
}

func waitForDatasetBuildJobByType(t *testing.T, service *DatasetService, projectID, datasetID, versionID, buildType string) domain.DatasetBuildJob {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := service.ListDatasetBuildJobs(projectID, datasetID, versionID)
		if err == nil {
			for _, job := range jobs.Items {
				if job.BuildType == buildType {
					return job
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	jobs, err := service.ListDatasetBuildJobs(projectID, datasetID, versionID)
	if err != nil {
		t.Fatalf("unexpected list dataset build jobs error: %v", err)
	}
	t.Fatalf("expected dataset build job type %s, got %+v", buildType, jobs.Items)
	return domain.DatasetBuildJob{}
}

func waitForDatasetVersionSentimentReady(t *testing.T, service *DatasetService, projectID, datasetID, versionID string) domain.DatasetVersion {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
		if err == nil && version.SentimentStatus == "ready" {
			return version
		}
		time.Sleep(20 * time.Millisecond)
	}
	version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	t.Fatalf("expected dataset version sentiment ready, got %s", version.SentimentStatus)
	return domain.DatasetVersion{}
}

func valuesAsStrings(t *testing.T, value any) []string {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", value)
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, fmt.Sprintf("%v", item))
	}
	return result
}

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		requestedPath = r.URL.Path
		requestedOutputPath = payload["output_path"].(string)
		requestedLLMMode = payload["llm_mode"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"prepare completed"},
			"artifact": map[string]any{
				"skill_name":               "dataset_prepare",
				"prepare_uri":              "/tmp/issues.prepared.parquet",
				"prepared_ref":             "/tmp/issues.prepared.parquet",
				"prepare_format":           "parquet",
				"prepare_model":            "claude-haiku-test",
				"prepare_prompt_version":   "dataset-prepare-anthropic-v1",
				"prepared_text_column":     "normalized_text",
				"row_id_column":            "row_id",
				"storage_contract_version": "unstructured-storage-v1",
				"usage": map[string]any{
					"provider":               "anthropic",
					"model":                  "claude-haiku-test",
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
	if requestedLLMMode != "default" {
		t.Fatalf("unexpected prepare llm mode: %s", requestedLLMMode)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected prepare output path: %s", requestedOutputPath)
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

	joiner := "\n\n"
	result, err := service.BuildPrepare(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{
		TextColumns: []string{"제목", "본문"},
		TextJoiner:  &joiner,
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
	})
	if err != nil {
		t.Fatalf("unexpected create first dataset version error: %v", err)
	}
	if !first.IsActive {
		t.Fatalf("expected first version to be active: %+v", first)
	}

	second, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues-v2.csv",
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
	})
	if err != nil {
		t.Fatalf("unexpected create first version error: %v", err)
	}
	second, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:       "/tmp/issues-v2.csv",
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

func TestCreateDatasetVersionEnqueuesEagerPrepareJobWhenWorkerConfigured(t *testing.T) {
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

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":          "/tmp/issues.prepared.parquet",
				"prepared_ref":         "/tmp/issues.prepared.parquet",
				"prepare_format":       "parquet",
				"prepared_text_column": "normalized_text",
				"summary": map[string]any{
					"output_row_count": 3,
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

	jobs, err := service.ListDatasetBuildJobs(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected list dataset build jobs error: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one eager prepare job, got %+v", jobs.Items)
	}
	job := waitForDatasetBuildJobStatus(t, service, project.ProjectID, jobs.Items[0].JobID, "completed")
	if job.BuildType != "prepare" {
		t.Fatalf("unexpected build type: %+v", job)
	}
	version = waitForDatasetVersionPrepareReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if callCount != 1 {
		t.Fatalf("expected eager prepare worker call once, got %d", callCount)
	}
	if version.PrepareURI == nil || *version.PrepareURI != "/tmp/issues.prepared.parquet" {
		t.Fatalf("unexpected prepare uri: %+v", version.PrepareURI)
	}
}

func TestCreateDatasetVersionAutoCreatesSentimentJobAfterPrepare(t *testing.T) {
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

	var mu sync.Mutex
	requestPaths := make([]string, 0, 2)
	sentimentDatasetName := ""
	sentimentTextColumn := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}

		mu.Lock()
		requestPaths = append(requestPaths, r.URL.Path)
		mu.Unlock()

		switch r.URL.Path {
		case "/tasks/dataset_prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"prepare_uri":          "/tmp/issues.prepared.parquet",
					"prepared_ref":         "/tmp/issues.prepared.parquet",
					"prepare_format":       "parquet",
					"prepared_text_column": "normalized_text",
					"summary": map[string]any{
						"output_row_count": 3,
					},
				},
			})
		case "/tasks/sentiment_label":
			mu.Lock()
			sentimentDatasetName = stringValue(payload["dataset_name"])
			sentimentTextColumn = stringValue(payload["text_column"])
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"sentiment_uri":               "/tmp/issues.prepared.parquet.sentiment.parquet",
					"sentiment_ref":               "/tmp/issues.prepared.parquet.sentiment.parquet",
					"sentiment_format":            "parquet",
					"sentiment_label_column":      "sentiment_label",
					"sentiment_confidence_column": "sentiment_confidence",
					"sentiment_reason_column":     "sentiment_reason",
				},
			})
		default:
			t.Fatalf("unexpected worker path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI:        "/tmp/issues.csv",
		DataType:          datasetStringPtr("unstructured"),
		SentimentRequired: datasetBoolPtr(true),
		PrepareRequired:   datasetBoolPtr(true),
		SentimentModel:    datasetStringPtr("claude-haiku-test"),
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	prepareJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "prepare")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, prepareJob.JobID, "completed")

	sentimentJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "sentiment")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, sentimentJob.JobID, "completed")

	version = waitForDatasetVersionSentimentReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)

	mu.Lock()
	defer mu.Unlock()
	if len(requestPaths) != 2 {
		t.Fatalf("expected prepare and sentiment worker calls, got %+v", requestPaths)
	}
	if requestPaths[0] != "/tasks/dataset_prepare" || requestPaths[1] != "/tasks/sentiment_label" {
		t.Fatalf("unexpected worker call order: %+v", requestPaths)
	}
	if sentimentDatasetName != "/tmp/issues.prepared.parquet" {
		t.Fatalf("expected sentiment to use prepared dataset, got %s", sentimentDatasetName)
	}
	if sentimentTextColumn != "normalized_text" {
		t.Fatalf("expected sentiment to use prepared text column, got %s", sentimentTextColumn)
	}
	if version.SentimentURI == nil || *version.SentimentURI != "/tmp/issues.prepared.parquet.sentiment.parquet" {
		t.Fatalf("unexpected sentiment uri: %+v", version.SentimentURI)
	}
}

func TestCreatePrepareJobCompletesAndStoresStatus(t *testing.T) {
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
		DatasetVersionID: "version-1",
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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":          "/tmp/issues.prepared.parquet",
				"prepared_ref":         "/tmp/issues.prepared.parquet",
				"prepare_format":       "parquet",
				"prepared_text_column": "normalized_text",
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	job, err := service.CreatePrepareJob(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{}, "test")
	if err != nil {
		t.Fatalf("unexpected create prepare job error: %v", err)
	}
	if job.Status != "queued" {
		t.Fatalf("unexpected initial job status: %+v", job)
	}

	job = waitForDatasetBuildJobStatus(t, service, project.ProjectID, job.JobID, "completed")
	if job.StartedAt == nil || job.CompletedAt == nil {
		t.Fatalf("expected started/completed timestamps: %+v", job)
	}
	if job.TriggeredBy != "test" {
		t.Fatalf("unexpected triggered_by: %+v", job)
	}
	version = waitForDatasetVersionPrepareReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if version.PrepareURI == nil || *version.PrepareURI == "" {
		t.Fatalf("expected prepared uri after job completion: %+v", version)
	}
}

func TestCreatePrepareJobStartsTemporalWorkflowWhenStarterConfigured(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())
	starter := &fakeDatasetBuildStarter{}
	service.SetBuildJobStarter(starter)

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
		Metadata:         map[string]any{},
		PrepareStatus:    "queued",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	job, err := service.CreatePrepareJob(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{}, "test")
	if err != nil {
		t.Fatalf("unexpected create prepare job error: %v", err)
	}
	if len(starter.startCalls) != 1 {
		t.Fatalf("expected temporal workflow start call, got %+v", starter.startCalls)
	}
	if starter.startCalls[0].JobID != job.JobID || starter.startCalls[0].BuildType != "prepare" {
		t.Fatalf("unexpected start input: %+v", starter.startCalls[0])
	}
	stored, err := service.GetDatasetBuildJob(project.ProjectID, job.JobID)
	if err != nil {
		t.Fatalf("unexpected get build job error: %v", err)
	}
	if stored.Status != "queued" {
		t.Fatalf("expected queued build job before workflow pickup, got %+v", stored)
	}
	if stored.WorkflowID == nil || *stored.WorkflowID != "dataset-build-"+job.JobID {
		t.Fatalf("expected workflow id on stored build job, got %+v", stored.WorkflowID)
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
		Metadata:         map[string]any{},
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
		Metadata:         map[string]any{},
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
		Metadata:         map[string]any{},
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
		Metadata:         map[string]any{},
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

func TestBuildClustersStoresSummaryAndMembershipRefs(t *testing.T) {
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
		DatasetVersionID: "version-cluster",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       datasetStringPtr("/tmp/issues.prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"embedding_index_source_ref": "/tmp/issues.embeddings.index.parquet",
			"chunk_ref":                  "/tmp/issues.embeddings.chunks.parquet",
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		if payload["output_path"] == "" {
			t.Fatalf("expected output_path in cluster build payload")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"cluster_ref":               "/tmp/issues.clusters.summary.json",
				"cluster_format":            "json",
				"cluster_summary_ref":       "/tmp/issues.clusters.summary.json",
				"cluster_summary_format":    "json",
				"cluster_membership_ref":    "/tmp/issues.clusters.memberships.parquet",
				"cluster_membership_format": "parquet",
				"cluster_algorithm":         "dense-hybrid-v1",
				"summary": map[string]any{
					"cluster_count":                3,
					"cluster_similarity_threshold": 0.2,
					"top_n":                        3,
					"sample_n":                     2,
					"cluster_membership_row_count": 6,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildClusters(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetClusterBuildRequest{
		SimilarityThreshold: datasetFloat64Ptr(0.2),
		TopN:                datasetIntPtr(3),
		SampleN:             datasetIntPtr(2),
	})
	if err != nil {
		t.Fatalf("unexpected build clusters error: %v", err)
	}

	if got := metadataString(result.Metadata, "cluster_ref", ""); got != "/tmp/issues.clusters.summary.json" {
		t.Fatalf("unexpected cluster ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_summary_ref", ""); got != "/tmp/issues.clusters.summary.json" {
		t.Fatalf("unexpected cluster summary ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_membership_ref", ""); got != "/tmp/issues.clusters.memberships.parquet" {
		t.Fatalf("unexpected cluster membership ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_membership_format", ""); got != "parquet" {
		t.Fatalf("unexpected cluster membership format: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_status", ""); got != "ready" {
		t.Fatalf("unexpected cluster status: %s", got)
	}
}

func TestGetClusterMembersLoadsSummaryAndMembership(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-cluster-members", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-cluster-members",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	summaryPath := filepath.Join(t.TempDir(), "clusters.json")
	membershipPath := filepath.Join(t.TempDir(), "clusters.memberships.parquet")
	if err := os.WriteFile(summaryPath, []byte(`{
		"clusters":[
			{"cluster_id":"cluster-01","document_count":3,"top_terms":[{"term":"결제","count":3}]},
			{"cluster_id":"cluster-02","document_count":1,"top_terms":[{"term":"배송","count":1}]}
		]
	}`), 0o644); err != nil {
		t.Fatalf("unexpected summary write error: %v", err)
	}
	writeClusterMembershipParquet(t, membershipPath, []map[string]any{
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           0,
			"row_id":                 "row-0",
			"chunk_id":               "row-0:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 오류가 반복 발생했습니다",
			"is_sample":              true,
		},
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           1,
			"row_id":                 "row-1",
			"chunk_id":               "row-1:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 승인 오류가 다시 발생했습니다",
			"is_sample":              true,
		},
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           5,
			"row_id":                 "row-5",
			"chunk_id":               "row-5:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 오류 문의가 접수됐습니다",
			"is_sample":              false,
		},
	})

	version := domain.DatasetVersion{
		DatasetVersionID: "version-cluster-members",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"cluster_status":            "ready",
			"cluster_ref":               summaryPath,
			"cluster_summary_ref":       summaryPath,
			"cluster_membership_ref":    membershipPath,
			"cluster_membership_format": "parquet",
		},
		PrepareStatus:   "ready",
		SentimentStatus: "not_requested",
		EmbeddingStatus: "ready",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	limit := 2
	samplesOnly := true
	response, err := service.GetClusterMembers(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		"cluster-01",
		domain.DatasetClusterMembersQuery{
			Limit:       &limit,
			SamplesOnly: &samplesOnly,
		},
	)
	if err != nil {
		t.Fatalf("unexpected get cluster members error: %v", err)
	}
	if response.TotalCount != 3 {
		t.Fatalf("unexpected total_count: %d", response.TotalCount)
	}
	if response.SampleCount != 2 {
		t.Fatalf("unexpected sample_count: %d", response.SampleCount)
	}
	if len(response.Items) != 2 {
		t.Fatalf("unexpected item count: %d", len(response.Items))
	}
	if !response.Items[0].IsSample || !response.Items[1].IsSample {
		t.Fatalf("expected samples_only response, got %+v", response.Items)
	}
	if got := stringValue(response.Cluster["cluster_id"]); got != "cluster-01" {
		t.Fatalf("unexpected cluster summary payload: %+v", response.Cluster)
	}
	if response.ClusterMembershipRef != membershipPath {
		t.Fatalf("unexpected cluster membership ref: %s", response.ClusterMembershipRef)
	}
}

func TestGetDatasetVersionIncludesSourceSummary(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-source-summary", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-source-summary",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	sourcePath := filepath.Join(t.TempDir(), "issues.csv")
	if err := os.WriteFile(sourcePath, []byte("title,body,count\n결제 오류,카드 결제가 실패합니다,3\n로그인 오류,로그인이 실패합니다,2\n"), 0o644); err != nil {
		t.Fatalf("unexpected write source file error: %v", err)
	}
	version := domain.DatasetVersion{
		DatasetVersionID: "version-source-summary",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       sourcePath,
		DataType:         "unstructured",
		Metadata:         map[string]any{},
		PrepareStatus:    "not_requested",
		SentimentStatus:  "not_requested",
		EmbeddingStatus:  "not_requested",
		CreatedAt:        time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}
	completedAt := time.Now().UTC()
	if err := repository.SaveDatasetBuildJob(domain.DatasetBuildJob{
		JobID:            "job-prepare-source-summary",
		ProjectID:        project.ProjectID,
		DatasetID:        dataset.DatasetID,
		DatasetVersionID: version.DatasetVersionID,
		BuildType:        "prepare",
		Status:           "completed",
		TriggeredBy:      "test",
		Attempt:          1,
		CreatedAt:        completedAt.Add(-time.Minute),
		CompletedAt:      &completedAt,
	}); err != nil {
		t.Fatalf("unexpected save dataset build job error: %v", err)
	}

	response, err := service.GetDatasetVersion(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	if response.SourceSummary == nil || !response.SourceSummary.Available {
		t.Fatalf("expected available source summary: %+v", response.SourceSummary)
	}
	if response.SourceSummary.Format != "csv" {
		t.Fatalf("unexpected source format: %s", response.SourceSummary.Format)
	}
	if response.SourceSummary.Status != "ready" {
		t.Fatalf("unexpected source summary status: %s", response.SourceSummary.Status)
	}
	if response.SourceSummary.RowCount == nil || *response.SourceSummary.RowCount != 2 {
		t.Fatalf("unexpected source row_count: %+v", response.SourceSummary.RowCount)
	}
	if response.SourceSummary.ColumnCount != 3 {
		t.Fatalf("unexpected source column_count: %d", response.SourceSummary.ColumnCount)
	}
	columnNames := make([]string, 0, len(response.SourceSummary.Columns))
	for _, column := range response.SourceSummary.Columns {
		columnNames = append(columnNames, column.Name)
	}
	if !reflect.DeepEqual(columnNames, []string{"title", "body", "count"}) {
		t.Fatalf("unexpected source columns: %+v", response.SourceSummary.Columns)
	}
	if len(response.SourceSummary.SampleRows) != 2 {
		t.Fatalf("unexpected source sample rows: %+v", response.SourceSummary.SampleRows)
	}
	if response.SourceSummary.SampleRows[0]["title"] != "결제 오류" {
		t.Fatalf("unexpected first sample row: %+v", response.SourceSummary.SampleRows[0])
	}
	if len(response.BuildJobs) != 1 {
		t.Fatalf("unexpected build jobs: %+v", response.BuildJobs)
	}
	if response.BuildJobs[0].BuildType != "prepare" || response.BuildJobs[0].Status != "completed" {
		t.Fatalf("unexpected build job status: %+v", response.BuildJobs[0])
	}
}

func TestGetPreparePreviewBuildsSummaryAndWarningPanel(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-prepare-preview", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-prepare-preview",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	preparedPath := filepath.Join(t.TempDir(), "prepared.parquet")
	writePreparedPreviewParquet(t, preparedPath, []map[string]any{
		{
			"source_row_index":    0,
			"row_id":              "row-0",
			"raw_text":            "결제 오류가 반복 발생했습니다!!!",
			"normalized_text":     "결제 오류가 반복 발생했습니다.",
			"prepare_disposition": "keep",
			"prepare_reason":      "noise removed",
		},
		{
			"source_row_index":    2,
			"row_id":              "row-2",
			"raw_text":            "로그인이 자주 실패하고 오류가 보입니다",
			"normalized_text":     "로그인이 자주 실패하고 오류가 보입니다.",
			"prepare_disposition": "review",
			"prepare_reason":      "needs review",
		},
	})

	preparedAt := time.Now().UTC()
	version := domain.DatasetVersion{
		DatasetVersionID: "version-prepare-preview",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"prepared_ref":         preparedPath,
			"prepared_format":      "parquet",
			"raw_text_column":      "text",
			"prepared_text_column": "normalized_text",
			"row_id_column":        "row_id",
			"prepare_summary": map[string]any{
				"input_row_count":  3,
				"output_row_count": 2,
				"kept_count":       1,
				"review_count":     1,
				"dropped_count":    1,
				"prepare_regex_rule_hits": map[string]any{
					"html_artifact": 1,
					"url_cleanup":   1,
				},
			},
		},
		PrepareStatus:   "ready",
		PrepareURI:      datasetStringPtr(preparedPath),
		PreparedAt:      &preparedAt,
		SentimentStatus: "not_requested",
		EmbeddingStatus: "not_requested",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	loadedVersion, err := service.GetDatasetVersion(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	if loadedVersion.PrepareSummary == nil {
		t.Fatalf("expected prepare_summary in dataset version: %+v", loadedVersion)
	}
	if loadedVersion.PrepareSummary.ReviewCount != 1 {
		t.Fatalf("unexpected review_count: %+v", loadedVersion.PrepareSummary)
	}
	if loadedVersion.PrepareSummary.PrepareRegexRuleHits["html_artifact"] != 1 {
		t.Fatalf("unexpected prepare_regex_rule_hits: %+v", loadedVersion.PrepareSummary.PrepareRegexRuleHits)
	}

	limit := 2
	response, err := service.GetPreparePreview(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetPreparePreviewQuery{Limit: &limit},
	)
	if err != nil {
		t.Fatalf("unexpected get prepare preview error: %v", err)
	}
	if response.PreparedRef != preparedPath {
		t.Fatalf("unexpected prepared_ref: %s", response.PreparedRef)
	}
	if response.SampleLimit != 2 {
		t.Fatalf("unexpected sample_limit: %d", response.SampleLimit)
	}
	if response.Summary == nil || response.Summary.DroppedCount != 1 {
		t.Fatalf("unexpected summary payload: %+v", response.Summary)
	}
	if len(response.Samples) != 2 {
		t.Fatalf("unexpected samples: %+v", response.Samples)
	}
	if response.Samples[0].RawText != "결제 오류가 반복 발생했습니다!!!" {
		t.Fatalf("unexpected first sample: %+v", response.Samples[0])
	}
	if response.WarningPanel == nil {
		t.Fatalf("expected warning_panel for review rows: %+v", response)
	}
	if response.WarningPanel.ReviewCount != 1 || len(response.WarningPanel.Samples) != 1 {
		t.Fatalf("unexpected warning_panel payload: %+v", response.WarningPanel)
	}
	if response.WarningPanel.Samples[0].PrepareDisposition != "review" {
		t.Fatalf("unexpected warning sample: %+v", response.WarningPanel.Samples[0])
	}
}

func TestGetSentimentPreviewBuildsSummary(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-sentiment-preview", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-sentiment-preview",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	sentimentPath := filepath.Join(t.TempDir(), "sentiment.parquet")
	writeSentimentPreviewParquet(t, sentimentPath, []map[string]any{
		{
			"source_row_index":         0,
			"row_id":                   "row-0",
			"sentiment_label":          "negative",
			"sentiment_confidence":     0.91,
			"sentiment_reason":         "결제 실패와 오류 반복 언급",
			"sentiment_prompt_version": "sentiment-anthropic-v2",
		},
		{
			"source_row_index":         2,
			"row_id":                   "row-2",
			"sentiment_label":          "neutral",
			"sentiment_confidence":     0.64,
			"sentiment_reason":         "상태 설명 중심",
			"sentiment_prompt_version": "sentiment-anthropic-v2",
		},
	})

	labeledAt := time.Now().UTC()
	version := domain.DatasetVersion{
		DatasetVersionID: "version-sentiment-preview",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"sentiment_ref":               sentimentPath,
			"sentiment_format":            "parquet",
			"sentiment_text_column":       "normalized_text",
			"sentiment_label_column":      "sentiment_label",
			"sentiment_confidence_column": "sentiment_confidence",
			"sentiment_reason_column":     "sentiment_reason",
			"row_id_column":               "row_id",
			"sentiment_summary": map[string]any{
				"input_row_count":      3,
				"labeled_row_count":    2,
				"text_column":          "normalized_text",
				"sentiment_batch_size": 8,
				"label_counts": map[string]any{
					"negative": 1,
					"neutral":  1,
				},
			},
		},
		PrepareStatus:      "ready",
		SentimentStatus:    "ready",
		SentimentURI:       datasetStringPtr(sentimentPath),
		SentimentLabeledAt: &labeledAt,
		EmbeddingStatus:    "not_requested",
		CreatedAt:          time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	limit := 2
	response, err := service.GetSentimentPreview(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetSentimentPreviewQuery{Limit: &limit},
	)
	if err != nil {
		t.Fatalf("unexpected get sentiment preview error: %v", err)
	}
	if response.SentimentRef != sentimentPath {
		t.Fatalf("unexpected sentiment_ref: %s", response.SentimentRef)
	}
	if response.SampleLimit != 2 {
		t.Fatalf("unexpected sample_limit: %d", response.SampleLimit)
	}
	if response.Summary == nil || response.Summary.LabeledRowCount != 2 {
		t.Fatalf("unexpected summary payload: %+v", response.Summary)
	}
	if response.Summary.LabelCounts["negative"] != 1 {
		t.Fatalf("unexpected label_counts payload: %+v", response.Summary.LabelCounts)
	}
	if len(response.Samples) != 2 {
		t.Fatalf("unexpected samples: %+v", response.Samples)
	}
	if response.Samples[0].SentimentLabel != "negative" {
		t.Fatalf("unexpected first sample: %+v", response.Samples[0])
	}
	if response.Samples[1].SentimentConfidence != 0.64 {
		t.Fatalf("unexpected second sample confidence: %+v", response.Samples[1])
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
					"text_joiner":          " ",
					"sentiment_batch_size": 8,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	joiner := " "
	result, err := service.BuildSentiment(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{
		TextColumns: []string{"제목", "본문"},
		TextJoiner:  &joiner,
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
	if requestedTextJoiner != " " {
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

func datasetStringPtr(value string) *string {
	return &value
}

func datasetBoolPtr(value bool) *bool {
	return &value
}

func datasetIntPtr(value int) *int {
	return &value
}

func datasetFloat64Ptr(value float64) *float64 {
	return &value
}
