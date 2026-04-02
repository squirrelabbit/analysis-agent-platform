package http

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/config"
)

func TestControlPlaneFlow(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	createProjectBody := `{"name":"demo-project","description":"test"}`
	readJSONResponse(t, handler, http.MethodPost, "/projects", createProjectBody, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"sales-dataset","data_type":"structured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"sales_daily","data_type":"structured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	analysis := map[string]any{}
	submitBody := `{
	  "dataset_version_id":"` + versionID + `",
	  "data_type":"structured",
	  "goal":"Why did sales drop?",
	  "constraints":["compare recent trend"],
	  "context":{"channel":"test"}
	}`
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/analysis_requests",
		submitBody,
		http.StatusCreated,
		&analysis,
	)
	plan := analysis["plan"].(map[string]any)
	planID := plan["plan_id"].(string)

	executionResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/plans/"+planID+"/execute",
		"",
		http.StatusAccepted,
		&executionResponse,
	)
	execution := executionResponse["execution"].(map[string]any)
	executionID := execution["execution_id"].(string)

	result := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions/"+executionID+"/result",
		"",
		http.StatusOK,
		&result,
	)
	if result["execution_id"] != executionID {
		t.Fatalf("unexpected execution_id in result: %v", result["execution_id"])
	}
	resultV1, ok := result["result_v1"].(map[string]any)
	if !ok {
		t.Fatalf("expected result_v1 payload: %+v", result)
	}
	if resultV1["schema_version"] != "execution-result-v1" {
		t.Fatalf("unexpected result_v1 schema version: %+v", resultV1)
	}

	rerun := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/executions/"+executionID+"/rerun",
		`{"mode":"strict_repro","triggered_by":"test"}`,
		http.StatusAccepted,
		&rerun,
	)
	rerunExecution := rerun["execution"].(map[string]any)
	rerunExecutionID := rerunExecution["execution_id"].(string)

	diff := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions/diff?from="+executionID+"&to="+rerunExecutionID,
		"",
		http.StatusOK,
		&diff,
	)
	if diff["changed_steps"].(float64) != 0 {
		t.Fatalf("expected unchanged diff, got %v", diff["changed_steps"])
	}
}

func TestExecutionListAndReportDraftEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"draft-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"issues","data_type":"structured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues.csv","data_type":"structured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	analysis := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/analysis_requests",
		`{"dataset_version_id":"`+versionID+`","data_type":"structured","goal":"이슈를 정리해줘"}`,
		http.StatusCreated,
		&analysis,
	)
	planID := analysis["plan"].(map[string]any)["plan_id"].(string)

	executionResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/plans/"+planID+"/execute",
		"",
		http.StatusAccepted,
		&executionResponse,
	)
	executionID := executionResponse["execution"].(map[string]any)["execution_id"].(string)

	listResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions",
		"",
		http.StatusOK,
		&listResponse,
	)
	items, ok := listResponse["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one execution list item: %+v", listResponse)
	}

	reportDraft := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/report_drafts",
		`{"title":"테스트 초안","execution_ids":["`+executionID+`"]}`,
		http.StatusCreated,
		&reportDraft,
	)
	draftID := reportDraft["draft_id"].(string)
	content := reportDraft["content"].(map[string]any)
	if content["schema_version"] != "report-draft-v1" {
		t.Fatalf("unexpected report draft schema: %+v", content)
	}

	loadedDraft := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/report_drafts/"+draftID,
		"",
		http.StatusOK,
		&loadedDraft,
	)
	if loadedDraft["draft_id"] != draftID {
		t.Fatalf("unexpected loaded draft: %+v", loadedDraft)
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
	if err := writer.WriteField("prepare_required", "true"); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	if err := writer.WriteField("metadata", `{"text_column":"text"}`); err != nil {
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

	var version map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &version); err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	storageURI, _ := version["storage_uri"].(string)
	if storageURI == "" {
		t.Fatalf("expected storage_uri in upload response")
	}
	if !strings.HasPrefix(storageURI, uploadRoot) {
		t.Fatalf("unexpected upload path: %s", storageURI)
	}
	if _, err := os.Stat(storageURI); err != nil {
		t.Fatalf("expected uploaded file on disk: %v", err)
	}
	metadata := version["metadata"].(map[string]any)
	upload := metadata["upload"].(map[string]any)
	if upload["stored_filename"] != "issues.csv" {
		t.Fatalf("unexpected stored filename: %#v", upload["stored_filename"])
	}
	if got := filepath.Base(storageURI); got != "issues.csv" {
		t.Fatalf("unexpected storage basename: %s", got)
	}
}

func TestOpenAPIDocumentAndSwaggerUI(t *testing.T) {
	openapiDir := t.TempDir()
	openapiPath := filepath.Join(openapiDir, "openapi.yaml")
	if err := os.WriteFile(openapiPath, []byte("openapi: 3.1.0\ninfo:\n  title: test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected openapi write error: %v", err)
	}

	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		OpenAPIPath:    openapiPath,
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
