package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
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
				"preprocess_options": map[string]any{
					"remove_english":       false,
					"remove_numbers":       false,
					"remove_special":       false,
					"remove_monosyllables": false,
				},
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
		case "/tasks/dataset_clean":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"clean_uri":           "/tmp/issues.cleaned.parquet",
					"cleaned_ref":         "/tmp/issues.cleaned.parquet",
					"clean_format":        "parquet",
					"cleaned_text_column": "cleaned_text",
					"row_id_column":       "row_id",
					"summary": map[string]any{
						"input_row_count":  3,
						"output_row_count": 3,
						"kept_count":       3,
					},
				},
			})
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
		Metadata:          map[string]any{"text_columns": []string{"text"}},
		SentimentRequired: datasetBoolPtr(true),
		PrepareRequired:   datasetBoolPtr(true),
		SentimentModel:    datasetStringPtr("claude-haiku-test"),
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}

	cleanJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "clean")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, cleanJob.JobID, "completed")
	waitForDatasetVersionCleanReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)

	prepareJobCreated, err := service.CreatePrepareJob(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{}, "test")
	if err != nil {
		t.Fatalf("unexpected create prepare job error: %v", err)
	}
	if prepareJobCreated.BuildType != "prepare" {
		t.Fatalf("unexpected prepare job: %+v", prepareJobCreated)
	}

	prepareJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "prepare")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, prepareJob.JobID, "completed")

	sentimentJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "sentiment")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, sentimentJob.JobID, "completed")

	version = waitForDatasetVersionSentimentReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)

	mu.Lock()
	defer mu.Unlock()
	if len(requestPaths) != 3 {
		t.Fatalf("expected clean, prepare, and sentiment worker calls, got %+v", requestPaths)
	}
	if requestPaths[0] != "/tasks/dataset_clean" || requestPaths[1] != "/tasks/dataset_prepare" || requestPaths[2] != "/tasks/sentiment_label" {
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

func TestCreateDatasetVersionAutoCreatesEmbeddingJobAfterPrepare(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-auto-embedding", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-auto-embedding",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	var mu sync.Mutex
	requestPaths := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestPaths = append(requestPaths, r.URL.Path)
		mu.Unlock()

		switch r.URL.Path {
		case "/tasks/dataset_clean":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"clean_uri":           "/tmp/issues.cleaned.parquet",
					"cleaned_ref":         "/tmp/issues.cleaned.parquet",
					"clean_format":        "parquet",
					"cleaned_text_column": "cleaned_text",
					"row_id_column":       "row_id",
					"summary": map[string]any{
						"input_row_count":  3,
						"output_row_count": 3,
						"kept_count":       3,
					},
				},
			})
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
		case "/tasks/embedding":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"embedding_index_source_ref":    "/tmp/issues.embedding.parquet",
					"embedding_index_source_format": "parquet",
					"chunk_ref":                     "/tmp/issues.chunks.parquet",
					"chunk_format":                  "parquet",
					"document_count":                3,
					"chunk_count":                   3,
					"embedding_model":               "test-embedding-model",
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
		Metadata:          map[string]any{"text_columns": []string{"text"}},
		PrepareRequired:   datasetBoolPtr(true),
		EmbeddingRequired: datasetBoolPtr(true),
		EmbeddingModel:    datasetStringPtr("test-embedding-model"),
	})
	if err != nil {
		t.Fatalf("unexpected create dataset version error: %v", err)
	}
	if !metadataBool(version.Metadata, "embedding_required") {
		t.Fatalf("expected embedding_required metadata: %+v", version.Metadata)
	}

	cleanJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "clean")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, cleanJob.JobID, "completed")
	waitForDatasetVersionCleanReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)

	prepareJobCreated, err := service.CreatePrepareJob(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{}, "test")
	if err != nil {
		t.Fatalf("unexpected create prepare job error: %v", err)
	}
	if prepareJobCreated.BuildType != "prepare" {
		t.Fatalf("unexpected prepare job: %+v", prepareJobCreated)
	}

	prepareJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "prepare")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, prepareJob.JobID, "completed")

	embeddingJob := waitForDatasetBuildJobByType(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID, "embedding")
	waitForDatasetBuildJobStatus(t, service, project.ProjectID, embeddingJob.JobID, "completed")

	version = waitForDatasetVersionEmbeddingReady(t, service, project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	embeddingStage, ok := datasetVersionBuildStageByName(version.BuildStages, "embedding")
	if !ok || !embeddingStage.Ready || embeddingStage.RunGroup != "post_prepare" || len(embeddingStage.DependsOn) != 1 || embeddingStage.DependsOn[0] != "prepare" {
		t.Fatalf("unexpected embedding stage: %+v", embeddingStage)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requestPaths) != 3 {
		t.Fatalf("expected clean, prepare, and embedding worker calls, got %+v", requestPaths)
	}
	if requestPaths[0] != "/tasks/dataset_clean" || requestPaths[1] != "/tasks/dataset_prepare" || requestPaths[2] != "/tasks/embedding" {
		t.Fatalf("unexpected worker call order: %+v", requestPaths)
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
		Metadata:         map[string]any{"text_columns": []string{"text"}},
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
		Metadata:         map[string]any{"text_columns": []string{"text"}},
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
