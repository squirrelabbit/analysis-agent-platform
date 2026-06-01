package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestCreateDatasetVersionEnqueuesEagerCleanJobWhenWorkerConfigured(t *testing.T) {
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
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		progressPath := stringValue(payload["progress_path"])
		if progressPath != "" {
			progressPayload := `{"percent":50,"processed_rows":5,"total_rows":10,"elapsed_seconds":12.5,"eta_seconds":12.5,"message":"clean processing rows","updated_at":"2026-04-22T01:00:00Z"}`
			if err := os.WriteFile(progressPath, []byte(progressPayload), 0o644); err != nil {
				t.Fatalf("unexpected write progress error: %v", err)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"clean_uri":           "/tmp/issues.cleaned.parquet",
				"cleaned_ref":         "/tmp/issues.cleaned.parquet",
				"clean_format":        "parquet",
				"progress_ref":        progressPath,
				"cleaned_text_column": "cleaned_text",
				"row_id_column":       "row_id",
				"summary": map[string]any{
					"input_row_count":  10,
					"output_row_count": 3,
					"kept_count":       3,
					"dropped_count":    7,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	version, err := service.CreateDatasetVersion(project.ProjectID, dataset.DatasetID, domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/issues.csv",
		DataType:   datasetStringPtr("unstructured"),
		Metadata:   map[string]any{"text_columns": []string{"text"}},
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	jobs, err := service.ListDatasetBuildJobs(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("unexpected list dataset build jobs error: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one eager clean job, got %+v", jobs.Items)
	}
	job := waitForDatasetBuildJobStatus(t, service, project.ProjectID, jobs.Items[0].JobID, "completed")
	if job.BuildType != "clean" {
		t.Fatalf("unexpected build type: %+v", job)
	}
	if job.Diagnostics.Progress == nil || job.Diagnostics.Progress.Percent != 50 {
		t.Fatalf("unexpected progress diagnostics: %+v", job.Diagnostics)
	}
	version = waitForDatasetVersionCleanReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if callCount != 1 {
		t.Fatalf("expected eager clean worker call once, got %d", callCount)
	}
	if version.CleanedRef == nil || *version.CleanedRef != "/tmp/issues.cleaned.parquet" {
		t.Fatalf("unexpected cleaned ref: %+v", version.CleanedRef)
	}
	if version.CleanURI == nil || *version.CleanURI != "/tmp/issues.cleaned.parquet" {
		t.Fatalf("unexpected clean uri: %+v", version.CleanURI)
	}
	if version.CleanedAt == nil {
		t.Fatalf("expected cleaned_at to be set")
	}
	if version.CleanSummary == nil || version.CleanSummary.OutputRowCount != 3 {
		t.Fatalf("unexpected clean summary: %+v", version.CleanSummary)
	}
}

