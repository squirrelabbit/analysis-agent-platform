package http

import (
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/planner"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/service"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

type Server struct {
	cfg             config.Config
	mux             *stdhttp.ServeMux
	projectService  *service.ProjectService
	datasetService  *service.DatasetService
	analysisService *service.AnalysisService
}

func NewServer(cfg config.Config) *Server {
	mux := stdhttp.NewServeMux()
	repository, err := store.NewRepository(cfg)
	if err != nil {
		panic(err)
	}
	starter, err := workflows.NewStarter(cfg)
	if err != nil {
		panic(err)
	}
	planGenerator, err := planner.New(cfg)
	if err != nil {
		panic(err)
	}
	server := &Server{
		cfg:             cfg,
		mux:             mux,
		projectService:  service.NewProjectService(repository),
		datasetService:  service.NewDatasetService(repository, cfg.PythonAIWorkerURL, cfg.UploadRoot, cfg.ArtifactRoot),
		analysisService: service.NewAnalysisService(repository, starter, planGenerator),
	}
	server.routes()
	return server
}

func (s *Server) Handler() stdhttp.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		writeJSON(w, stdhttp.StatusOK, map[string]any{
			"status": "ok",
			"stack":  "go-control-plane-scaffold",
		})
	})
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("GET /swagger", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /swagger/", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /skills", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		writeJSON(w, stdhttp.StatusOK, registry.SupportedSkills())
	})
	s.mux.HandleFunc("POST /projects", s.handleCreateProject)
	s.mux.HandleFunc("GET /projects/{project_id}", s.handleGetProject)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets", s.handleCreateDataset)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}", s.handleGetDataset)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/uploads", s.handleUploadDataset)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions", s.handleCreateDatasetVersion)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}", s.handleGetDatasetVersion)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/prepare", s.handleBuildPrepare)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/sentiment", s.handleBuildSentiment)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/embeddings", s.handleBuildEmbeddings)
	s.mux.HandleFunc("POST /projects/{project_id}/analysis_requests", s.handleSubmitAnalysis)
	s.mux.HandleFunc("GET /projects/{project_id}/analysis_requests/{request_id}", s.handleGetRequest)
	s.mux.HandleFunc("GET /projects/{project_id}/plans/{plan_id}", s.handleGetPlan)
	s.mux.HandleFunc("POST /projects/{project_id}/plans/{plan_id}/execute", s.handleExecutePlan)
	s.mux.HandleFunc("GET /projects/{project_id}/executions/{execution_id}", s.handleGetExecution)
	s.mux.HandleFunc("GET /projects/{project_id}/executions/{execution_id}/result", s.handleGetExecutionResult)
	s.mux.HandleFunc("POST /projects/{project_id}/executions/{execution_id}/resume", s.handleResumeExecution)
	s.mux.HandleFunc("POST /projects/{project_id}/executions/{execution_id}/rerun", s.handleRerunExecution)
	s.mux.HandleFunc("GET /projects/{project_id}/executions/diff", s.handleDiffExecutions)
}

func writeJSON(w stdhttp.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	_ = encoder.Encode(displaytime.NormalizeForJSON(payload))
}

func (s *Server) handleCreateProject(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ProjectCreateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	project, err := s.projectService.CreateProject(payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, project)
}

func (s *Server) handleOpenAPI(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	path := strings.TrimSpace(s.cfg.OpenAPIPath)
	if path == "" {
		writeError(w, stdhttp.StatusInternalServerError, "openapi path is not configured")
		return
	}
	content, err := os.ReadFile(path)
	if err != nil {
		writeError(w, stdhttp.StatusInternalServerError, "failed to read openapi document")
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = w.Write(content)
}

func (s *Server) handleSwaggerUI(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = io.WriteString(w, swaggerUIHTML(r))
}

func (s *Server) handleGetProject(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	projectID := r.PathValue("project_id")
	project, err := s.projectService.GetProject(projectID)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, project)
}

func (s *Server) handleCreateDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetCreateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateDataset(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleGetDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetDataset(r.PathValue("project_id"), r.PathValue("dataset_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleUploadDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeError(w, stdhttp.StatusBadRequest, "invalid multipart form")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, stdhttp.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	payload, err := datasetVersionCreateRequestFromMultipart(r.MultipartForm)
	if err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}

	response, err := s.datasetService.UploadDatasetVersion(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		payload,
		header.Filename,
		header.Header.Get("Content-Type"),
		file,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleCreateDatasetVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetVersionCreateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateDatasetVersion(r.PathValue("project_id"), r.PathValue("dataset_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleGetDatasetVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetDatasetVersion(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleBuildPrepare(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetPrepareRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.BuildPrepare(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func datasetVersionCreateRequestFromMultipart(form *multipart.Form) (domain.DatasetVersionCreateRequest, error) {
	var payload domain.DatasetVersionCreateRequest
	if form == nil {
		return payload, errors.New("multipart form is required")
	}

	if value := firstFormValue(form, "data_type"); value != "" {
		payload.DataType = stringPtr(value)
	}
	if value := firstFormValue(form, "record_count"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return payload, errors.New("record_count must be an integer")
		}
		payload.RecordCount = &parsed
	}
	if value := firstFormValue(form, "metadata"); value != "" {
		metadata := map[string]any{}
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			return payload, errors.New("metadata must be a JSON object")
		}
		payload.Metadata = metadata
	}
	if value, ok, err := optionalBoolFormValue(form, "prepare_required"); err != nil {
		return payload, err
	} else if ok {
		payload.PrepareRequired = &value
	}
	if value, ok, err := optionalBoolFormValue(form, "sentiment_required"); err != nil {
		return payload, err
	} else if ok {
		payload.SentimentRequired = &value
	}
	if value, ok, err := optionalBoolFormValue(form, "embedding_required"); err != nil {
		return payload, err
	} else if ok {
		payload.EmbeddingRequired = &value
	}
	if value := firstFormValue(form, "prepare_model"); value != "" {
		payload.PrepareModel = stringPtr(value)
	}
	if value := firstFormValue(form, "sentiment_model"); value != "" {
		payload.SentimentModel = stringPtr(value)
	}
	if value := firstFormValue(form, "embedding_model"); value != "" {
		payload.EmbeddingModel = stringPtr(value)
	}
	return payload, nil
}

func (s *Server) handleBuildEmbeddings(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetEmbeddingBuildRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.BuildEmbeddings(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func (s *Server) handleBuildSentiment(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetSentimentBuildRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.BuildSentiment(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func (s *Server) handleSubmitAnalysis(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.AnalysisSubmitRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.analysisService.SubmitAnalysis(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleGetRequest(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.analysisService.GetRequest(r.PathValue("project_id"), r.PathValue("request_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetPlan(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.analysisService.GetPlan(r.PathValue("project_id"), r.PathValue("plan_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleExecutePlan(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.analysisService.ExecutePlan(r.PathValue("project_id"), r.PathValue("plan_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func (s *Server) handleGetExecution(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.analysisService.GetExecution(r.PathValue("project_id"), r.PathValue("execution_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetExecutionResult(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.analysisService.BuildExecutionResult(r.PathValue("project_id"), r.PathValue("execution_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleResumeExecution(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ExecutionResumeRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.analysisService.ResumeExecution(r.PathValue("project_id"), r.PathValue("execution_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func (s *Server) handleRerunExecution(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ExecutionRerunRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.analysisService.RerunExecution(r.PathValue("project_id"), r.PathValue("execution_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusAccepted, response)
}

func (s *Server) handleDiffExecutions(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	fromExecutionID := strings.TrimSpace(r.URL.Query().Get("from"))
	toExecutionID := strings.TrimSpace(r.URL.Query().Get("to"))
	if fromExecutionID == "" || toExecutionID == "" {
		writeError(w, stdhttp.StatusBadRequest, "from and to query parameters are required")
		return
	}
	response, err := s.analysisService.DiffExecutions(r.PathValue("project_id"), fromExecutionID, toExecutionID)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func decodeJSON(r *stdhttp.Request, dest any) error {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		return err
	}
	return nil
}

func decodeJSONAllowEmpty(r *stdhttp.Request, dest any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dest); err != nil {
		if errors.Is(err, stdhttp.ErrBodyNotAllowed) {
			return nil
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func writeError(w stdhttp.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"detail": strings.TrimSpace(message),
	})
}

func (s *Server) writeServiceError(w stdhttp.ResponseWriter, err error) {
	var invalid service.ErrInvalidArgument
	var missing service.ErrNotFound
	switch {
	case errors.As(err, &invalid):
		writeError(w, stdhttp.StatusBadRequest, invalid.Error())
	case errors.As(err, &missing):
		writeError(w, stdhttp.StatusNotFound, missing.Error())
	default:
		writeError(w, stdhttp.StatusInternalServerError, err.Error())
	}
}

func firstFormValue(form *multipart.Form, key string) string {
	if form == nil || form.Value == nil {
		return ""
	}
	values := form.Value[key]
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func optionalBoolFormValue(form *multipart.Form, key string) (bool, bool, error) {
	value := firstFormValue(form, key)
	if value == "" {
		return false, false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, true, errors.New(key + " must be true or false")
	}
	return parsed, true, nil
}

func stringPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func swaggerUIHTML(r *stdhttp.Request) string {
	specURL := "/openapi.yaml"
	if r != nil && r.URL != nil && strings.HasPrefix(r.URL.Path, "/swagger/") {
		specURL = "../openapi.yaml"
	}
	return `<!doctype html>
<html lang="ko">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Analysis Support Platform API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
    <style>
      html { box-sizing: border-box; overflow-y: scroll; }
      *, *:before, *:after { box-sizing: inherit; }
      body { margin: 0; background: #f6f7fb; }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: '` + specURL + `',
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis],
        layout: 'BaseLayout'
      });
    </script>
  </body>
</html>`
}
