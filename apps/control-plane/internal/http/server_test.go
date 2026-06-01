package http

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/config"

	_ "github.com/marcboeker/go-duckdb"
)

func TestProjectPromptEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"prompt-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	created := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-evidence-v1",
		  "operation":"issue_evidence_summary",
		  "change_reason":"test create",
		  "content":"---\ntitle: 프로젝트 evidence\noperation: issue_evidence_summary\nstatus: experimental\nsummary: 프로젝트 전용 evidence\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n"
		}`,
		http.StatusCreated,
		&created,
	)
	if created["version"] != "project-evidence-v1" || created["operation"] != "issue_evidence_summary" {
		t.Fatalf("unexpected created project prompt: %+v", created)
	}
	if created["title"] != "프로젝트 evidence" || created["status"] != "experimental" {
		t.Fatalf("expected front matter metadata in response: %+v", created)
	}

	duplicateError := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-evidence-v1",
		  "operation":"issue_evidence_summary",
		  "change_reason":"test duplicate",
		  "content":"---\ntitle: 프로젝트 evidence 2\noperation: issue_evidence_summary\nstatus: experimental\nsummary: 덮어쓰기 시도\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n"
		}`,
		http.StatusConflict,
		&duplicateError,
	)
	if duplicateError["detail"] == "" {
		t.Fatalf("expected duplicate prompt error body: %+v", duplicateError)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-evidence-v2",
		  "operation":"issue_evidence_summary",
		  "change_reason":"test create v2",
		  "content":"---\ntitle: 프로젝트 evidence v2\noperation: issue_evidence_summary\nstatus: active\nsummary: 프로젝트 v2\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n"
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	list := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompts", "", http.StatusOK, &list)
	items, ok := list["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected two project prompts: %+v", list)
	}
	item := items[0].(map[string]any)
	if item["content_hash"] == "" {
		t.Fatalf("expected prompt content hash: %+v", item)
	}

	defaults := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompt_defaults", "", http.StatusOK, &defaults)
	if defaults["project_id"] != projectID {
		t.Fatalf("unexpected prompt defaults response: %+v", defaults)
	}
	if _, ok := defaults["issue_evidence_summary_prompt_version"]; ok {
		t.Fatalf("expected empty defaults before update: %+v", defaults)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodPut,
		"/projects/"+projectID+"/prompt_defaults",
		`{
		  "issue_evidence_summary_prompt_version":"project-evidence-v1"
		}`,
		http.StatusOK,
		&defaults,
	)
	if defaults["issue_evidence_summary_prompt_version"] != "project-evidence-v1" {
		t.Fatalf("unexpected updated prompt defaults: %+v", defaults)
	}

	defaults = map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompt_defaults", "", http.StatusOK, &defaults)
	if defaults["issue_evidence_summary_prompt_version"] != "project-evidence-v1" {
		t.Fatalf("unexpected loaded prompt defaults: %+v", defaults)
	}
}


func TestResponsesRenderTimestampsInKST(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects",
		`{"name":"timezone-project"}`,
		http.StatusCreated,
		&project,
	)

	createdAt, ok := project["created_at"].(string)
	if !ok {
		t.Fatalf("expected created_at string, got %#v", project["created_at"])
	}
	if !strings.HasSuffix(createdAt, "+09:00") {
		t.Fatalf("expected KST timestamp, got %s", createdAt)
	}
}

func TestUploadDatasetCreatesStoredVersion(t *testing.T) {
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		UploadRoot:     uploadRoot,
		ArtifactRoot:   artifactRoot,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"upload-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"upload-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("data_type", "unstructured"); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	fileWriter, err := writer.CreateFormFile("file", "issues.csv")
	if err != nil {
		t.Fatalf("unexpected create form file error: %v", err)
	}
	if _, err := io.WriteString(fileWriter, "text\n결제 오류가 반복 발생했습니다\n"); err != nil {
		t.Fatalf("unexpected file write error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/projects/"+projectID+"/datasets/"+datasetID+"/uploads", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("unexpected upload status: %d body=%s", recorder.Code, recorder.Body.String())
	}

	var uploadResp map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &uploadResp); err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	versionID, _ := uploadResp["dataset_version_id"].(string)
	if versionID == "" {
		t.Fatalf("expected dataset_version_id in upload response: %#v", uploadResp)
	}
	if _, ok := uploadResp["row_count"]; !ok {
		t.Fatalf("expected row_count in upload response: %#v", uploadResp)
	}
	if _, ok := uploadResp["column_count"]; !ok {
		t.Fatalf("expected column_count in upload response: %#v", uploadResp)
	}
	if _, ok := uploadResp["columns"]; !ok {
		t.Fatalf("expected columns in upload response: %#v", uploadResp)
	}
	byteSize, ok := uploadResp["byte_size"].(float64)
	if !ok || byteSize <= 0 {
		t.Fatalf("expected positive byte_size in upload response: %#v", uploadResp)
	}
	columnsAny, _ := uploadResp["columns"].([]any)
	gotColumns := make([]string, 0, len(columnsAny))
	for _, c := range columnsAny {
		gotColumns = append(gotColumns, c.(string))
	}
	if len(gotColumns) != 1 || gotColumns[0] != "text" {
		t.Fatalf("expected columns=[text], got: %#v", gotColumns)
	}
	if rc, _ := uploadResp["row_count"].(float64); rc != 1 {
		t.Fatalf("expected row_count=1, got: %#v", uploadResp["row_count"])
	}
	if cc, _ := uploadResp["column_count"].(float64); cc != 1 {
		t.Fatalf("expected column_count=1, got: %#v", uploadResp["column_count"])
	}

	version := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID, "", http.StatusOK, &version)

	expectedStoragePath := filepath.Join(uploadRoot, "projects", projectID, "datasets", datasetID, "versions", versionID, "source", "issues.csv")
	if _, err := os.Stat(expectedStoragePath); err != nil {
		t.Fatalf("expected uploaded file on disk at %s: %v", expectedStoragePath, err)
	}
	if version["is_active"] != true {
		t.Fatalf("expected uploaded dataset version to be active: %+v", version)
	}
	for _, key := range []string{"storage_uri", "metadata", "artifacts", "build_jobs", "prepare_status", "sentiment_status", "embedding_status"} {
		if _, present := version[key]; present {
			t.Fatalf("expected detail to omit %q (β2/(γ) cleanup), got: %#v", key, version)
		}
	}
	for _, stage := range []string{"clean", "doc_genuineness", "clause_label"} {
		s, ok := version[stage].(map[string]any)
		if !ok {
			t.Fatalf("expected stage %q in detail: %#v", stage, version)
		}
		if _, ok := s["status"].(string); !ok {
			t.Fatalf("expected status on stage %q: %#v", stage, s)
		}
	}
	if dataset["active_dataset_version_id"] != nil {
		t.Fatalf("expected dataset create response to omit active version before upload: %+v", dataset)
	}
	loadedDataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID, "", http.StatusOK, &loadedDataset)
	if loadedDataset["active_dataset_version_id"] != versionID {
		t.Fatalf("expected uploaded version to become active: %+v", loadedDataset)
	}

	req = httptest.NewRequest(http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/source_download", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected source download status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	// silverone 2026-06-01 (다운로드 파일명 타임스탬프) — 파일명에 `_YYYYMMDD_
	// HHMMSS` 접미가 붙는다 (예: `issues_20260601_134523.csv`). 확장자와 base는
	// 유지되므로 두 토큰만 확인.
	if disposition := recorder.Header().Get("Content-Disposition"); !strings.Contains(disposition, "attachment; filename=") || !strings.Contains(disposition, "issues_") || !strings.Contains(disposition, ".csv") {
		t.Fatalf("unexpected content-disposition: %s", disposition)
	}
	if got := recorder.Body.String(); got != "text\n결제 오류가 반복 발생했습니다\n" {
		t.Fatalf("unexpected source download body: %q", got)
	}
}

func TestDatasetActiveVersionEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		UploadRoot:     t.TempDir(),
		ArtifactRoot:   t.TempDir(),
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"active-version-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"active-version-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	firstVersion := uploadDatasetVersionFixture(t, handler, projectID, datasetID, "issues-v1.csv", "unstructured", "text\nv1\n", "")
	secondVersion := uploadDatasetVersionFixture(t, handler, projectID, datasetID, "issues-v2.csv", "unstructured", "text\nv2\n", "false")
	if secondVersion["is_active"] != false {
		t.Fatalf("expected second version to stay inactive: %+v", secondVersion)
	}

	updatedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPut,
		"/projects/"+projectID+"/datasets/"+datasetID+"/active_version",
		fmt.Sprintf(`{"dataset_version_id":"%s"}`, secondVersion["dataset_version_id"]),
		http.StatusOK,
		&updatedDataset,
	)
	if updatedDataset["active_dataset_version_id"] != secondVersion["dataset_version_id"] {
		t.Fatalf("expected second version to become active: %+v", updatedDataset)
	}

	versionList := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		"",
		http.StatusOK,
		&versionList,
	)
	items := versionList["items"].([]any)
	for _, item := range items {
		versionItem := item.(map[string]any)
		switch versionItem["dataset_version_id"] {
		case firstVersion["dataset_version_id"]:
			if versionItem["is_active"] != false {
				t.Fatalf("expected first version to be inactive: %+v", versionItem)
			}
		case secondVersion["dataset_version_id"]:
			if versionItem["is_active"] != true {
				t.Fatalf("expected second version to be active: %+v", versionItem)
			}
		}
	}

	deactivatedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPut,
		"/projects/"+projectID+"/datasets/"+datasetID+"/active_version",
		`{"dataset_version_id":""}`,
		http.StatusOK,
		&deactivatedDataset,
	)
	if _, ok := deactivatedDataset["active_dataset_version_id"]; ok {
		t.Fatalf("expected active_dataset_version_id to be omitted after deactivation: %+v", deactivatedDataset)
	}
}

func TestDeleteDatasetVersionEndpoint(t *testing.T) {
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		UploadRoot:     uploadRoot,
		ArtifactRoot:   artifactRoot,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"delete-version-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"delete-version-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	version := uploadDatasetVersionFixture(t, handler, projectID, datasetID, "delete-version-source.jsonl", "unstructured", "{\"text\":\"v1\"}\n", "")
	versionID := version["dataset_version_id"].(string)

	uploadVersionPath := filepath.Join(uploadRoot, "projects", projectID, "datasets", datasetID, "versions", versionID)
	artifactVersionPath := filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID, "versions", versionID)
	if err := os.MkdirAll(filepath.Join(uploadVersionPath, "source"), 0o755); err != nil {
		t.Fatalf("failed to seed version upload path: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(artifactVersionPath, "prepare"), 0o755); err != nil {
		t.Fatalf("failed to seed version artifact path: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedVersion := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID,
		"",
		http.StatusNotFound,
		&deletedVersion,
	)
	loadedDataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID, "", http.StatusOK, &loadedDataset)
	if _, ok := loadedDataset["active_dataset_version_id"]; ok {
		t.Fatalf("expected active version to be cleared after deleting active version: %+v", loadedDataset)
	}
	if _, err := os.Stat(uploadVersionPath); !os.IsNotExist(err) {
		t.Fatalf("expected upload version path to be removed: %v", err)
	}
	if _, err := os.Stat(artifactVersionPath); !os.IsNotExist(err) {
		t.Fatalf("expected artifact version path to be removed: %v", err)
	}
}

func TestDeleteProjectAndDatasetEndpoints(t *testing.T) {
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		UploadRoot:     uploadRoot,
		ArtifactRoot:   artifactRoot,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"delete-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"delete-dataset","data_type":"unstructured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	_ = uploadDatasetVersionFixture(t, handler, projectID, datasetID, "delete-source.jsonl", "unstructured", "{\"text\":\"hello\"}\n", "")

	datasetPath := filepath.Join(uploadRoot, "projects", projectID, "datasets", datasetID)
	artifactPath := filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID)
	if err := os.MkdirAll(filepath.Join(datasetPath, "stub"), 0o755); err != nil {
		t.Fatalf("failed to seed dataset upload path: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(artifactPath, "stub"), 0o755); err != nil {
		t.Fatalf("failed to seed dataset artifact path: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID+"/datasets/"+datasetID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID,
		"",
		http.StatusNotFound,
		&deletedDataset,
	)
	if _, err := os.Stat(datasetPath); !os.IsNotExist(err) {
		t.Fatalf("expected upload dataset path to be removed: %v", err)
	}
	if _, err := os.Stat(artifactPath); !os.IsNotExist(err) {
		t.Fatalf("expected artifact dataset path to be removed: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedProject := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID,
		"",
		http.StatusNotFound,
		&deletedProject,
	)
	projectPath := filepath.Join(uploadRoot, "projects", projectID)
	projectArtifactPath := filepath.Join(artifactRoot, "projects", projectID)
	if _, err := os.Stat(projectPath); !os.IsNotExist(err) {
		t.Fatalf("expected project upload path to be removed: %v", err)
	}
	if _, err := os.Stat(projectArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("expected project artifact path to be removed: %v", err)
	}
}

func TestOpenAPIDocumentAndSwaggerUI(t *testing.T) {
	openapiDir := t.TempDir()
	openapiPath := filepath.Join(openapiDir, "openapi.yaml")
	frontendOpenAPIPath := filepath.Join(openapiDir, "openapi.frontend.yaml")
	if err := os.WriteFile(openapiPath, []byte("openapi: 3.1.0\ninfo:\n  title: test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected openapi write error: %v", err)
	}
	if err := os.WriteFile(frontendOpenAPIPath, []byte("openapi: 3.1.0\ninfo:\n  title: frontend-test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected frontend openapi write error: %v", err)
	}

	server := NewServer(config.Config{
		BindAddr:            ":0",
		StoreBackend:        "memory",
		WorkflowEngine:      "noop",
		OpenAPIPath:         openapiPath,
		FrontendOpenAPIPath: frontendOpenAPIPath,
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected openapi status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "openapi: 3.1.0") {
		t.Fatalf("unexpected openapi body: %s", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/openapi.frontend.yaml", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected frontend openapi status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "frontend-test") {
		t.Fatalf("unexpected frontend openapi body: %s", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/swagger", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected swagger status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "SwaggerUIBundle") {
		t.Fatalf("swagger html missing bundle bootstrap")
	}
	if !strings.Contains(body, "/openapi.yaml") {
		t.Fatalf("swagger html missing openapi url")
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/swagger/frontend", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected frontend swagger status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	body = recorder.Body.String()
	if !strings.Contains(body, "SwaggerUIBundle") {
		t.Fatalf("frontend swagger html missing bundle bootstrap")
	}
	if !strings.Contains(body, "/openapi.frontend.yaml") {
		t.Fatalf("frontend swagger html missing frontend openapi url")
	}
}

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:           ":0",
		StoreBackend:       "memory",
		WorkflowEngine:     "noop",
		CORSAllowedOrigins: []string{"http://127.0.0.1:4173"},
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/projects", nil)
	req.Header.Set("Origin", "http://127.0.0.1:4173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type,authorization")
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected preflight status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:4173" {
		t.Fatalf("unexpected allow-origin header: %q", got)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodPost) {
		t.Fatalf("unexpected allow-methods header: %q", got)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Headers"); got != "content-type,authorization" {
		t.Fatalf("unexpected allow-headers header: %q", got)
	}
}

func TestCORSDoesNotExposeHeadersForUnknownOrigin(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:           ":0",
		StoreBackend:       "memory",
		WorkflowEngine:     "noop",
		CORSAllowedOrigins: []string{"http://127.0.0.1:4173"},
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected health status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected allow-origin header for unknown origin: %q", got)
	}
}

// uploadDatasetVersionFixture — POST /uploads로 dataset version을 만들고 그
// version 객체(GET 결과)를 돌려준다. POST /versions endpoint가 사라진 뒤
// test fixture가 multipart 보일러플레이트를 한 곳에 모으도록 도입.
// activateOnCreate가 "" 이면 form field 자체를 생략(default 동작).
func uploadDatasetVersionFixture(
	t *testing.T,
	handler http.Handler,
	projectID, datasetID, filename, dataType, fileBody, activateOnCreate string,
) map[string]any {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if dataType != "" {
		if err := writer.WriteField("data_type", dataType); err != nil {
			t.Fatalf("write data_type: %v", err)
		}
	}
	if activateOnCreate != "" {
		if err := writer.WriteField("activate_on_create", activateOnCreate); err != nil {
			t.Fatalf("write activate_on_create: %v", err)
		}
	}
	fileWriter, err := writer.CreateFormFile("file", filename)
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := io.WriteString(fileWriter, fileBody); err != nil {
		t.Fatalf("write file body: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/projects/"+projectID+"/datasets/"+datasetID+"/uploads", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("upload fixture failed: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode upload resp: %v", err)
	}
	versionID, _ := resp["dataset_version_id"].(string)
	if versionID == "" {
		t.Fatalf("upload fixture: missing dataset_version_id in %#v", resp)
	}
	var version map[string]any
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID, "", http.StatusOK, &version)
	return version
}

func readJSONResponse(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
	expectedStatus int,
	dest any,
) {
	t.Helper()

	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}

	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != expectedStatus {
		t.Fatalf("unexpected status for %s %s: got=%d body=%s", method, path, recorder.Code, recorder.Body.String())
	}
	if dest == nil {
		return
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), dest); err != nil {
		t.Fatalf("failed to decode response for %s %s: %v", method, path, err)
	}
}

func waitForBuildJobStatusHTTP(t *testing.T, handler http.Handler, projectID, jobID, expectedStatus string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var job map[string]any
		readJSONResponse(
			t,
			handler,
			http.MethodGet,
			"/projects/"+projectID+"/dataset_build_jobs/"+jobID,
			"",
			http.StatusOK,
			&job,
		)
		if job["status"] == expectedStatus {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	var job map[string]any
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/dataset_build_jobs/"+jobID,
		"",
		http.StatusOK,
		&job,
	)
	t.Fatalf("expected build job %s status %s, got %+v", jobID, expectedStatus, job)
	return nil
}

func writeCleanedParquetForHTTP(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "clean-preview-http.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_row_index, '%s' AS row_id, '%s' AS raw_text, '%s' AS cleaned_text, '%s' AS clean_disposition, '%s' AS clean_reason`,
			row["source_row_index"].(int),
			escapeDuckDBLiteralForHTTP(row["row_id"].(string)),
			escapeDuckDBLiteralForHTTP(row["raw_text"].(string)),
			escapeDuckDBLiteralForHTTP(row["cleaned_text"].(string)),
			escapeDuckDBLiteralForHTTP(row["clean_disposition"].(string)),
			escapeDuckDBLiteralForHTTP(row["clean_reason"].(string)),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteralForHTTP(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func escapeDuckDBLiteralForHTTP(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}
