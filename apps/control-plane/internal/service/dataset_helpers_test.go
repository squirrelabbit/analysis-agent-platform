package service

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
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

func datasetVersionArtifactByType(items []domain.DatasetVersionArtifact, artifactType string) (domain.DatasetVersionArtifact, bool) {
	for _, item := range items {
		if item.ArtifactType == artifactType {
			return item, true
		}
	}
	return domain.DatasetVersionArtifact{}, false
}

func datasetVersionBuildStageByName(items []domain.DatasetVersionBuildStage, stage string) (domain.DatasetVersionBuildStage, bool) {
	for _, item := range items {
		if item.Stage == stage {
			return item, true
		}
	}
	return domain.DatasetVersionBuildStage{}, false
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

func waitForDatasetVersionCleanReady(t *testing.T, service *DatasetService, projectID, datasetID, versionID string) domain.DatasetVersion {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
		if err == nil && version.CleanStatus == "ready" {
			return version
		}
		time.Sleep(20 * time.Millisecond)
	}
	version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	t.Fatalf("expected dataset version clean ready, got %s", version.CleanStatus)
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

func waitForDatasetVersionEmbeddingReady(t *testing.T, service *DatasetService, projectID, datasetID, versionID string) domain.DatasetVersion {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
		if err == nil && version.EmbeddingStatus == "ready" {
			return version
		}
		time.Sleep(20 * time.Millisecond)
	}
	version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		t.Fatalf("unexpected get dataset version error: %v", err)
	}
	t.Fatalf("expected dataset version embedding ready, got %s", version.EmbeddingStatus)
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
