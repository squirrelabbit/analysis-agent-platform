package service

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

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
	sourceArtifact, ok := datasetVersionArtifactByType(response.Artifacts, "source")
	if !ok {
		t.Fatalf("expected source artifact: %+v", response.Artifacts)
	}
	if sourceArtifact.Stage != "source" || sourceArtifact.Status != "ready" || sourceArtifact.URI != sourcePath || sourceArtifact.Format != "csv" {
		t.Fatalf("unexpected source artifact: %+v", sourceArtifact)
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
	if len(response.Columns) != 6 || response.Columns[2].Key != "raw_text" || response.Columns[3].Key != "normalized_text" {
		t.Fatalf("unexpected prepare preview columns: %+v", response.Columns)
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
	if len(response.Columns) != 5 || response.Columns[2].Key != "sentiment_label" || response.Columns[3].Key != "sentiment_confidence" {
		t.Fatalf("unexpected sentiment preview columns: %+v", response.Columns)
	}
	if response.Samples[0].SentimentLabel != "negative" {
		t.Fatalf("unexpected first sample: %+v", response.Samples[0])
	}
	if response.Samples[1].SentimentConfidence != 0.64 {
		t.Fatalf("unexpected second sample confidence: %+v", response.Samples[1])
	}
}
