package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/metrics"
	"analysis-support-platform/control-plane/internal/obs"
	"analysis-support-platform/control-plane/internal/service"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

type Server struct {
	cfg            config.Config
	mux            *stdhttp.ServeMux
	projectService *service.ProjectService
	datasetService *service.DatasetService
	authService    *service.AuthService
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
	server := &Server{
		cfg:            cfg,
		mux:            mux,
		projectService: service.NewProjectService(repository, cfg.UploadRoot, cfg.ArtifactRoot),
		datasetService: service.NewDatasetService(repository, cfg.PythonAIWorkerURL, cfg.UploadRoot, cfg.ArtifactRoot),
		authService: service.NewAuthService(
			repository,
			service.NewGoogleAuthenticator(cfg.AuthGoogleClientID, cfg.AuthGoogleSecret, cfg.AuthRedirectURL),
			service.AuthConfig{
				ClientID:      cfg.AuthGoogleClientID,
				RedirectURL:   cfg.AuthRedirectURL,
				AllowedDomain: cfg.AuthAllowedDomain,
				AdminEmails:   cfg.AuthAdminEmails,
				SessionTTL:    time.Duration(cfg.AuthSessionTTLHours) * time.Hour,
			},
		),
	}
	if err := server.datasetService.SetDatasetProfilesPath(cfg.DatasetProfilesPath); err != nil {
		panic(err)
	}
	server.datasetService.SetPromptTemplatesDir(cfg.PromptTemplatesDir)
	server.datasetService.SetBuildJobStarter(starter)
	server.datasetService.SetPythonAITaskTimeout(time.Duration(cfg.PythonAIWorkerHTTPTimeoutSec) * time.Second)
	server.datasetService.SetLLOAModelDisplay(cfg.LLOAModel, cfg.LLOAModelDisplayName)
	if err := server.datasetService.SetLLOAModelsPath(cfg.LLOAModelsPath); err != nil {
		panic(err)
	}
	server.datasetService.SetPlanReuseEnabled(cfg.PlanReuseEnabled)
	server.routes()
	return server
}

func (s *Server) Handler() stdhttp.Handler {
	return obs.Middleware(s.withCORS(s.authMiddleware(s.mux)))
}

// ReconcileStartup — silverone 2026-05-27 (Codex adversarial review fix-2).
// listening 전에 호출. in-flight analysis_runs / dataset_build_jobs를 모두
// 단말 상태로 마감해 재기동 후 active job lookup이 막히지 않게 한다.
func (s *Server) ReconcileStartup(ctx context.Context) (service.ReconcileReport, error) {
	return s.datasetService.ReconcileStartup(ctx)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		writeJSON(w, stdhttp.StatusOK, map[string]any{
			"status": "ok",
			"stack":  "go-control-plane-scaffold",
		})
	})
	s.mux.HandleFunc("GET /runtime_status", s.handleRuntimeStatus)
	// silverone 2026-06-04 (metrics 1차) — Prometheus text exposition. plain text.
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	// 전역 read-only prompt 선택지. task-folder prompt(doc_genuineness /
	// clause_label)의 version/default/label을 Python worker로 proxy해 반환.
	s.mux.HandleFunc("GET /prompt_options", s.handlePromptOptions)
	// 전역 read-only 전처리 모델 선택지 (LLOA_MODELS env allowlist). 빌드 재실행
	// 다이얼로그의 모델 select용. (silverone 2026-06-12)
	s.mux.HandleFunc("GET /lloa_model_options", s.handleLLOAModelOptions)
	// 전역 read-only taxonomy 정의. aspect key→한글 label 매핑 등을 Python
	// worker로 proxy해 반환 (프론트 표시용).
	s.mux.HandleFunc("GET /taxonomy", s.handleTaxonomy)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("GET /openapi.frontend.yaml", s.handleFrontendOpenAPI)
	s.mux.HandleFunc("GET /swagger", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /swagger/", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /swagger/frontend", s.handleFrontendSwaggerUI)
	s.mux.HandleFunc("GET /swagger/frontend/", s.handleFrontendSwaggerUI)
	// 5/6 화면기획서 B안 채택 (vault prompt_저장_정책.md): 전역 prompt
	// 라이브러리(/prompts) 화면 안 만들기로 결정. 글로벌 prompt는 .md 코드
	// 계약, 프로젝트별만 DB(project_prompts). 옛 전역 라우트 5개 + handler +
	// service + store 제거. 운영자는 ``/projects/{X}/prompts`` (B5 화면)로
	// project-scoped prompt만 관리.
	// δ-4 (5/21) — /skills route 제거. analyze가 planner + executor로
	// LLM이 plan_v2를 직접 생성하므로 고정 skill catalog 노출이 의미를 잃었다.
	// plan_v2 8 skill catalog는 planner/schema.py의 SKILL_CATALOG로 잠금.
	// 인증/RBAC (ADR-025). config/google/* 는 public, me/logout은 세션 필요.
	s.mux.HandleFunc("GET /auth/config", s.handleAuthConfig)
	s.mux.HandleFunc("GET /auth/google/start", s.handleAuthGoogleStart)
	s.mux.HandleFunc("GET /auth/google/callback", s.handleAuthGoogleCallback)
	s.mux.HandleFunc("GET /auth/me", s.handleAuthMe)
	s.mux.HandleFunc("POST /auth/logout", s.handleAuthLogout)
	// 프로젝트 멤버(RBAC) 관리 — admin/owner. 권한 부여/회수/조회.
	s.mux.HandleFunc("GET /projects/{project_id}/members", s.handleListProjectMembers)
	s.mux.HandleFunc("PUT /projects/{project_id}/members/{user_id}", s.handleUpsertProjectMember)
	s.mux.HandleFunc("DELETE /projects/{project_id}/members/{user_id}", s.handleDeleteProjectMember)
	s.mux.HandleFunc("POST /projects", s.handleCreateProject)
	s.mux.HandleFunc("GET /projects", s.handleListProjects)
	s.mux.HandleFunc("GET /projects/{project_id}", s.handleGetProject)
	s.mux.HandleFunc("DELETE /projects/{project_id}", s.handleDeleteProject)
	s.mux.HandleFunc("GET /projects/{project_id}/prompts", s.handleListProjectPrompts)
	s.mux.HandleFunc("POST /projects/{project_id}/prompts", s.handleSaveProjectPrompt)
	s.mux.HandleFunc("GET /projects/{project_id}/prompt_defaults", s.handleGetProjectPromptDefaults)
	s.mux.HandleFunc("PUT /projects/{project_id}/prompt_defaults", s.handleUpdateProjectPromptDefaults)
	// ADR-015 §C audit endpoints.
	s.mux.HandleFunc("GET /projects/{project_id}/prompt_history", s.handleListProjectPromptHistory)
	s.mux.HandleFunc("POST /projects/{project_id}/prompts/{operation}/revert", s.handleRevertProjectPrompt)
	s.mux.HandleFunc("GET /projects/{project_id}/prompts/{operation}/diff", s.handleProjectPromptDiff)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets", s.handleCreateDataset)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets", s.handleListDatasets)
	// silverone 2026-05-22 (옵션 α1) — dataset-level 설정 갱신. body는
	// `{"metadata": {...}}` 또는 `{...}` 둘 다 허용.
	s.mux.HandleFunc("PATCH /projects/{project_id}/datasets/{dataset_id}/metadata", s.handleUpdateDatasetMetadata)
	s.mux.HandleFunc("PATCH /projects/{project_id}/datasets/{dataset_id}", s.handleUpdateDatasetInfo)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}", s.handleGetDataset)
	s.mux.HandleFunc("DELETE /projects/{project_id}/datasets/{dataset_id}", s.handleDeleteDataset)
	s.mux.HandleFunc("PUT /projects/{project_id}/datasets/{dataset_id}/active_version", s.handleActivateDatasetVersion)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/uploads", s.handleUploadDataset)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions", s.handleListDatasetVersions)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}", s.handleGetDatasetVersion)
	s.mux.HandleFunc("DELETE /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}", s.handleDeleteDatasetVersion)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/source_download", s.handleDownloadSourceDataset)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clean", s.handleCreateCleanJob)
	// 화면 polling용 GET — POST와 같은 path에 method routing.
	// status + progress + summary를 한 번에 반환해 build job endpoint 직접
	// polling이 필요 없다. doc_genuineness / clause_label도 같은 패턴.
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clean", s.handleGetCleanView)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clean_download", s.handleDownloadCleanedDataset)
	// dataset_build endpoint — clean / clause_label / doc_genuineness
	// 3 task만 유지. 옛 prepare/sentiment/embeddings/cluster/segment/
	// embedding_cluster/keyword_index 7 task + document_cluster_profile은
	// (β2) 결정으로 제거 (2026-05-19).
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_label", s.handleCreateClauseLabelJob)
	// 2026-05-21 — 화면 polling용 GET. status + applied + summary + items 페이지 반환.
	// POST와 같은 path에 method routing.
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_label", s.handleGetClauseLabelView)
	// 절 라벨링 aspect/sentiment 수동 보정 (silverone 2026-06-11).
	s.mux.HandleFunc("PATCH /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_label/{clause_id}", s.handleSetClauseLabelOverride)
	s.mux.HandleFunc("DELETE /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_label/{clause_id}/override", s.handleDeleteClauseLabelOverride)
	// 2026-05-21 — clause_label / doc_genuineness 산출물 CSV 다운로드. jsonl
	// artifact를 DuckDB로 즉시 변환해 UTF-8 BOM CSV로 스트림.
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_label_download", s.handleDownloadClauseLabelDataset)
	// silverone 2026-06-10 — 수동 keyword build endpoint. precondition clause_label ready.
	// 운영자 API/script 실행용 (UI 버튼은 보고서/탭 작업 때 추가).
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_keywords", s.handleCreateClauseKeywordsJob)
	// 같은 path GET — clause_keywords 대시보드/조회 (summary + 필터·페이징 item table).
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clause_keywords", s.handleGetClauseKeywordsView)
	// ADR-017 / 5/19 결정 — clean 직후 doc-level 3-tier 진성 분류 endpoint.
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/doc_genuineness", s.handleCreateDocGenuinenessJob)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/doc_genuineness", s.handleGetDocGenuinenessView)
	// 진성 분류 모델 비교 (silverone 2026-06-15) — 같은 원본을 두 모델로 빌드한
	// 두 버전을 ?version_a=&version_b=로 받아 doc_id 1:1 비교 리포트 반환.
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/doc_genuineness/compare", s.handleCompareDocGenuineness)
	// 진성 라벨 수동 보정 (silverone 2026-06-11) — PATCH로 set, DELETE override로 되돌리기.
	s.mux.HandleFunc("PATCH /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/doc_genuineness/{doc_id}", s.handleSetDocGenuinenessOverride)
	s.mux.HandleFunc("DELETE /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/doc_genuineness/{doc_id}/override", s.handleDeleteDocGenuinenessOverride)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/doc_genuineness_download", s.handleDownloadDocGenuinenessDataset)
	// Phase 3 (2026-05-22) — /versions/{vid}/build_jobs list 제거. 화면이
	// build job 이력을 직접 조회할 필요가 사라졌다 (view endpoint가 최신 job의
	// status/progress/error_message를 묶어서 반환). retry 이력 trace가
	// 필요하면 Temporal UI 또는 DB 직접 조회로.
	// plan_v2 + executor sync debug endpoint. body는 {plan} 또는
	// {user_question} 둘 중 하나. Go가 version의 artifact path를 resolve해서
	// python-ai worker에 inline inject. wire contract `plan_version: "v2"`는
	// response body에서 유지하지만 endpoint는 정식 이름 /analyze만 노출.
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/analyze", s.handleAnalyze)
	// dataset의 active version을 자동 resolve해서 위 version endpoint와
	// 동일한 흐름을 저장형 analysis thread의 첫 메시지로 실행한다. 이어질문은
	// /analysis_threads/{thread_id}/messages를 사용.
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/analyze", s.handleAnalyzeOnActiveVersion)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/analysis_threads", s.handleCreateAnalysisThread)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/analysis_threads", s.handleListAnalysisThreads)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/analysis_threads/{thread_id}", s.handleGetAnalysisThread)
	s.mux.HandleFunc("DELETE /projects/{project_id}/datasets/{dataset_id}/analysis_threads/{thread_id}", s.handleDeleteAnalysisThread)
	s.mux.HandleFunc("POST /projects/{project_id}/datasets/{dataset_id}/analysis_threads/{thread_id}/messages", s.handlePostAnalysisThreadMessage)
	s.mux.HandleFunc("GET /projects/{project_id}/datasets/{dataset_id}/analysis_runs/{run_id}", s.handleGetAnalysisRun)
	s.mux.HandleFunc("GET /projects/{project_id}/dataset_build_jobs/{job_id}", s.handleGetDatasetBuildJob)
	// 보고서 보관함 (silverone 2026-06-10) — project 스코프.
	s.mux.HandleFunc("POST /projects/{project_id}/saved_results", s.handleCreateSavedResult)
	s.mux.HandleFunc("GET /projects/{project_id}/saved_results", s.handleListSavedResults)
	s.mux.HandleFunc("DELETE /projects/{project_id}/saved_results/{result_id}", s.handleDeleteSavedResult)
	// 보고서 문서 CRUD (silverone 2026-06-11) — project 스코프.
	s.mux.HandleFunc("POST /projects/{project_id}/reports", s.handleCreateReport)
	s.mux.HandleFunc("GET /projects/{project_id}/reports", s.handleListReports)
	s.mux.HandleFunc("GET /projects/{project_id}/reports/{report_id}", s.handleGetReport)
	s.mux.HandleFunc("PUT /projects/{project_id}/reports/{report_id}", s.handleUpdateReport)
	s.mux.HandleFunc("DELETE /projects/{project_id}/reports/{report_id}", s.handleDeleteReport)
}

func (s *Server) withCORS(next stdhttp.Handler) stdhttp.Handler {
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		appendVary(w.Header(), "Origin")
		appendVary(w.Header(), "Access-Control-Request-Method")
		appendVary(w.Header(), "Access-Control-Request-Headers")

		allowedOrigin, ok := s.allowedOrigin(r.Header.Get("Origin"))
		if ok {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Max-Age", "600")

			allowedHeaders := strings.TrimSpace(r.Header.Get("Access-Control-Request-Headers"))
			if allowedHeaders == "" {
				allowedHeaders = "Accept, Authorization, Content-Type"
			}
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
		}

		if r.Method == stdhttp.MethodOptions && strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")) != "" {
			if ok {
				w.WriteHeader(stdhttp.StatusNoContent)
				return
			}
			w.WriteHeader(stdhttp.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowedOrigin(origin string) (string, bool) {
	candidate := strings.TrimSpace(origin)
	if candidate == "" {
		return "", false
	}
	for _, allowed := range s.cfg.CORSAllowedOrigins {
		value := strings.TrimSpace(allowed)
		if value == "" {
			continue
		}
		if value == "*" {
			return "*", true
		}
		if strings.EqualFold(value, candidate) {
			return candidate, true
		}
	}
	return "", false
}

func writeJSON(w stdhttp.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	_ = encoder.Encode(displaytime.NormalizeForJSON(payload))
}

func (s *Server) handleMetrics(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = io.WriteString(w, metrics.Render())
}

func (s *Server) handleRuntimeStatus(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	response := domain.RuntimeStatusResponse{
		Status:         "ok",
		Stack:          "go-control-plane-scaffold",
		WorkflowEngine: strings.TrimSpace(s.cfg.WorkflowEngine),
		StoreBackend:   strings.TrimSpace(s.cfg.StoreBackend),
		PlannerBackend: strings.TrimSpace(s.cfg.PlannerBackend),
	}
	if strings.TrimSpace(s.cfg.WorkflowEngine) == "temporal" {
		response.Temporal = &domain.RuntimeStatusTemporal{
			Address:         strings.TrimSpace(s.cfg.TemporalAddress),
			Namespace:       strings.TrimSpace(s.cfg.TemporalNamespace),
			TaskQueue:       strings.TrimSpace(s.cfg.TemporalTaskQueue),
			BuildTaskQueue:  strings.TrimSpace(s.cfg.TemporalBuildTaskQueue),
			PersistenceMode: strings.TrimSpace(s.cfg.TemporalPersistenceMode),
			RetentionMode:   strings.TrimSpace(s.cfg.TemporalRetentionMode),
			RecoveryMode:    strings.TrimSpace(s.cfg.TemporalRecoveryMode),
		}
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// handlePromptOptions — GET /prompt_options?task=<task>. task-folder prompt의
// version/default/label을 Python worker proxy로 반환한다. worker 응답 JSON을
// 그대로 전달 (Go는 파일 미접근). invalid task는 ErrInvalidArgument → 400.
func (s *Server) handlePromptOptions(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	task := strings.TrimSpace(r.URL.Query().Get("task"))
	raw, err := s.datasetService.GetPromptOptions(r.Context(), task)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = w.Write(raw)
}

// handleTaxonomy — GET /taxonomy?taxonomy_id=<id>. aspect/sentiment taxonomy
// 정의(key/label/description)를 Python worker proxy로 반환한다. taxonomy_id
// 미지정 시 worker default(festival-v2). worker 응답 JSON을 그대로 전달
// (Go는 파일 미접근). unknown id는 ErrInvalidArgument → 400.
func (s *Server) handleTaxonomy(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	taxonomyID := strings.TrimSpace(r.URL.Query().Get("taxonomy_id"))
	raw, err := s.datasetService.GetTaxonomy(r.Context(), taxonomyID)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = w.Write(raw)
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
	s.handleOpenAPIFile(w, strings.TrimSpace(s.cfg.OpenAPIPath))
}

func (s *Server) handleFrontendOpenAPI(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	s.handleOpenAPIFile(w, strings.TrimSpace(s.cfg.FrontendOpenAPIPath))
}

func (s *Server) handleOpenAPIFile(w stdhttp.ResponseWriter, path string) {
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
	_, _ = io.WriteString(w, swaggerUIHTML("/openapi.yaml"))
}

func (s *Server) handleFrontendSwaggerUI(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = io.WriteString(w, swaggerUIHTML("/openapi.frontend.yaml"))
}

func appendVary(header stdhttp.Header, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	for _, existing := range header.Values("Vary") {
		for _, part := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(part), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
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

func (s *Server) handleListProjects(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.projectService.ListProjects()
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	// 인증 on이면 내가 멤버인 프로젝트만(admin은 전체). off면 그대로 전체.
	if s.cfg.AuthEnabled {
		if user, ok := userFromContext(r.Context()); ok && user.GlobalRole != "admin" {
			roles, rErr := s.authService.ProjectRolesForUser(user.UserID)
			if rErr != nil {
				s.writeServiceError(w, rErr)
				return
			}
			filtered := make([]domain.Project, 0, len(response.Items))
			for _, p := range response.Items {
				if _, member := roles[p.ProjectID]; member {
					filtered = append(filtered, p)
				}
			}
			response.Items = filtered
		}
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleDeleteProject(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.projectService.DeleteProject(r.PathValue("project_id")); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

// 5/6 화면기획서 B안 채택: handleListPrompts/handleCreatePrompt/handleGetPrompt/
// handleUpdatePrompt/handleDeletePrompt 5개 제거. 글로벌 prompt는 .md 코드
// 계약, 프로젝트별만 DB. 운영자 hot-edit은 ``handleListProjectPrompts``+
// ``handleSaveProjectPrompt`` 흐름에서.

func (s *Server) handleListProjectPrompts(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListProjectPrompts(r.PathValue("project_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleSaveProjectPrompt(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ProjectPromptUpsertRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	// ADR-015 §D1: soft enforcement of operator-only operations until
	// auth lands. Caller must echo the X-Operator-Mode: 1 header to
	// edit planner / planner_meta prompts.
	payload.CallerIsOperator = isOperatorRequest(r)
	response, err := s.datasetService.SaveProjectPrompt(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func isOperatorRequest(r *stdhttp.Request) bool {
	value := strings.TrimSpace(r.Header.Get("X-Operator-Mode"))
	return value == "1" || strings.EqualFold(value, "true")
}

func (s *Server) handleGetProjectPromptDefaults(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetProjectPromptDefaults(r.PathValue("project_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleUpdateProjectPromptDefaults(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ProjectPromptDefaultsUpdateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.UpdateProjectPromptDefaults(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// ADR-015 §C audit handlers.
//
// GET /projects/{project_id}/prompt_history?operation=<op> — list audit
// rows oldest-first. “operation“ is optional (omitted → all operations).
func (s *Server) handleListProjectPromptHistory(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	operation := strings.TrimSpace(r.URL.Query().Get("operation"))
	response, err := s.datasetService.ListProjectPromptHistory(r.PathValue("project_id"), operation)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// POST /projects/{project_id}/prompts/{operation}/revert — clone
// “to_version“ body into “new_version“ and append a “revert“
// audit row. The active prompt's content is never mutated in place
// (Codex review §Q4).
func (s *Server) handleRevertProjectPrompt(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ProjectPromptRevertRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	payload.CallerIsOperator = isOperatorRequest(r)
	response, err := s.datasetService.RevertProjectPrompt(
		r.PathValue("project_id"),
		r.PathValue("operation"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

// GET /projects/{project_id}/prompts/{operation}/diff?base=<v>&head=<v>
// — server-side line diff between two stored prompt versions for the UI's
// edit/history view.
func (s *Server) handleProjectPromptDiff(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	base := r.URL.Query().Get("base")
	head := r.URL.Query().Get("head")
	response, err := s.datasetService.DiffProjectPromptVersions(
		r.PathValue("project_id"),
		r.PathValue("operation"),
		base,
		head,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
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

func (s *Server) handleListDatasets(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListDatasets(r.PathValue("project_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleDeleteDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteDataset(r.PathValue("project_id"), r.PathValue("dataset_id")); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

// handleUpdateDatasetMetadata — PATCH /projects/{pid}/datasets/{did}/metadata.
// silverone 2026-05-22 (옵션 α1). request body는 `{"metadata": {...}}` 또는
// `{...}` 둘 다 받는다 (운영자가 굳이 wrapper를 안 적어도 동작). top-level
// key 단위 merge — service.UpdateDatasetMetadata 시맨틱 그대로.
func (s *Server) handleUpdateDatasetMetadata(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var raw map[string]any
	if err := decodeJSON(r, &raw); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	patch := raw
	if wrapper, ok := raw["metadata"].(map[string]any); ok && len(raw) == 1 {
		patch = wrapper
	}
	response, err := s.datasetService.UpdateDatasetMetadata(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		patch,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// handleUpdateDatasetInfo — PATCH /projects/{pid}/datasets/{did}. 이름/설명 수정.
// silverone 2026-06-05 — non-nil 필드만 반영. (metadata 수정은 /metadata 별도)
func (s *Server) handleUpdateDatasetInfo(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetInfoUpdateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.UpdateDatasetInfo(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleActivateDatasetVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetActiveVersionUpdateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	var response domain.Dataset
	var err error
	if strings.TrimSpace(payload.DatasetVersionID) == "" {
		response, err = s.datasetService.DeactivateDatasetVersion(
			r.PathValue("project_id"),
			r.PathValue("dataset_id"),
		)
	} else {
		response, err = s.datasetService.ActivateDatasetVersion(
			r.PathValue("project_id"),
			r.PathValue("dataset_id"),
			payload.DatasetVersionID,
		)
	}
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
	writeJSON(w, stdhttp.StatusCreated, buildUploadResponse(response))
}

// buildUploadResponse — POST /uploads 응답을 최소 셋(식별자 + 파일 형태 요약)만
// 포함하는 평탄한 객체로 만든다. SourceSummary/metadata.upload를 손에 들고
// row/column/byte_size를 끌어온다. 값이 비어 있으면 0 또는 빈 슬라이스를
// 그대로 노출해 caller가 분기할 필요를 줄인다.
func buildUploadResponse(version domain.DatasetVersion) map[string]any {
	columns := []string{}
	var rowCount, columnCount int
	if version.SourceSummary != nil {
		if version.SourceSummary.RowCount != nil {
			rowCount = *version.SourceSummary.RowCount
		}
		columnCount = version.SourceSummary.ColumnCount
		for _, col := range version.SourceSummary.Columns {
			columns = append(columns, col.Name)
		}
	}
	var byteSize int64
	if upload, ok := version.Metadata["upload"].(map[string]any); ok {
		switch b := upload["byte_size"].(type) {
		case int64:
			byteSize = b
		case int:
			byteSize = int64(b)
		case float64:
			byteSize = int64(b)
		}
	}
	return map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"row_count":          rowCount,
		"column_count":       columnCount,
		"columns":            columns,
		"byte_size":          byteSize,
	}
}

func (s *Server) handleGetDatasetVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetDatasetVersionDetail(
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

func (s *Server) handleDeleteDatasetVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteDatasetVersion(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) handleDownloadSourceDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	sourcePath, filename, contentType, err := s.datasetService.ResolveSourceDownload(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	handle, err := os.Open(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.writeServiceError(w, service.ErrNotFound{Resource: "source file"})
			return
		}
		s.writeServiceError(w, err)
		return
	}
	defer handle.Close()

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filename))
	w.WriteHeader(stdhttp.StatusOK)
	_, _ = io.Copy(w, handle)
}

func (s *Server) handleDownloadCleanedDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	s.streamArtifactCSV(w, r, "clean", s.datasetService.ResolveCleanDownload)
}

// 2026-05-21 — doc_genuineness / clause_label 다운로드. jsonl artifact를
// DuckDB로 CSV 변환 후 UTF-8 BOM 붙여 스트림. clean_download와 같은 출력
// 패턴 (Content-Type: text/csv, Content-Disposition: attachment).
func (s *Server) handleDownloadDocGenuinenessDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	s.streamArtifactCSV(w, r, "doc_genuineness", s.datasetService.ResolveDocGenuinenessDownload)
}

func (s *Server) handleDownloadClauseLabelDataset(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	s.streamArtifactCSV(w, r, "clause_label", s.datasetService.ResolveClauseLabelDownload)
}

// streamArtifactCSV — 3 download endpoint(clean/doc_genuineness/clause_label)
// 공통 CSV streaming + BOM prefix + 임시 파일 cleanup 흐름.
func (s *Server) streamArtifactCSV(
	w stdhttp.ResponseWriter,
	r *stdhttp.Request,
	kind string,
	resolve func(projectID, datasetID, datasetVersionID string) (string, string, error),
) {
	artifactPath, filename, err := resolve(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	defer os.Remove(artifactPath)
	handle, err := os.Open(artifactPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.writeServiceError(w, service.ErrNotFound{Resource: kind + " artifact"})
			return
		}
		s.writeServiceError(w, err)
		return
	}
	defer handle.Close()

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filename))
	w.WriteHeader(stdhttp.StatusOK)
	if _, err := w.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return
	}
	_, _ = io.Copy(w, handle)
}

func (s *Server) handleListDatasetVersions(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListDatasetVersions(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// 5/6 endpoint 통일: handleBuildPrepare/handleBuildSentiment/handleBuildEmbeddings
// HTTP handler 3개는 제거됨. 라우팅을 ``handleCreatePrepareJob`` 등으로
// 위임 (jobs row 생성 + 비동기). ``BuildPrepare/Sentiment/Embeddings``
// 함수 자체는 ``dispatchDatasetBuildJob``의 fallback runner로 그대로 유지.

func (s *Server) handleCreateCleanJob(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetCleanRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateCleanJob(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		payload,
		"api",
		obs.RequestIDFromContext(r.Context()),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	// 2026-05-21 — POST 응답은 slim accepted shape으로. 상세는
	// GET /dataset_build_jobs/{job_id} 또는 /versions/{version_id}/build_jobs.
	writeJSON(w, stdhttp.StatusAccepted, response.AsAccepted())
}

// dataset_build deprecated endpoint helper. 호출처(dataset_build_v2.go 등)가
// 다시 도입할 수 있으니 setDeprecatedBuildEndpointHeaders는 유지.
const _deprecatedSunsetDate = "Mon, 15 Sep 2025 00:00:00 GMT"

func setDeprecatedBuildEndpointHeaders(w stdhttp.ResponseWriter, replacementPath string) {
	w.Header().Set("Deprecation", "true")
	w.Header().Set("Sunset", _deprecatedSunsetDate)
	if replacementPath != "" {
		w.Header().Set("Link", `<`+replacementPath+`>; rel="successor-version"`)
	}
}

func datasetVersionCreateRequestFromMultipart(form *multipart.Form) (domain.DatasetVersionCreateRequest, error) {
	var payload domain.DatasetVersionCreateRequest
	if form == nil {
		return payload, errors.New("multipart form is required")
	}

	if value := firstFormValue(form, "data_type"); value != "" {
		payload.DataType = stringPtr(value)
	}
	if value := firstFormValue(form, "profile"); value != "" {
		var profile domain.DatasetProfile
		if err := json.Unmarshal([]byte(value), &profile); err != nil {
			return payload, errors.New("profile must be a JSON object")
		}
		payload.Profile = &profile
	}
	if value, ok, err := optionalBoolFormValue(form, "activate_on_create"); err != nil {
		return payload, err
	} else if ok {
		payload.ActivateOnCreate = &value
	}
	// ADR-018 β2 — prepare/sentiment/embedding task 삭제로 관련 8 form field
	// (prepare_required / prepare_llm_mode / prepare_model / sentiment_required
	// / sentiment_llm_mode / sentiment_model / embedding_required / embedding_model)
	// 제거. DatasetVersionCreateRequest struct field는 audit/DB 호환 보존하되
	// upload form에서는 더 이상 받지 않음.
	return payload, nil
}

// document_cluster_profile 4 handler는 β2 (5/19) 결정으로 제거.

// handleAnalyze — version-specific path. plan 디버깅/replay 전용. user_question
// 필드는 받지 않는다 (K-안, 2026-05-22). 화면 분석은 active version path
// (handleAnalyzeOnActiveVersion)를 사용.
func (s *Server) handleAnalyze(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload service.AnalyzeDebugRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.ExecuteAnalyze(
		r.Context(),
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		service.AnalyzeRequest{Plan: payload.Plan},
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

// handleAnalyzeOnActiveVersion — 화면 분석 path. user_question만 받는다.
// dataset의 active version을 자동 resolve해서 처리.
func (s *Server) handleAnalyzeOnActiveVersion(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload service.AnalyzeUserQuestionRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.AnalyzeDatasetAsNewThread(
		r.Context(),
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		service.AnalyzeRequest{UserQuestion: payload.UserQuestion},
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleCreateAnalysisThread(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.AnalysisThreadCreateRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateAnalysisThread(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleListAnalysisThreads(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListAnalysisThreads(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetAnalysisThread(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetAnalysisThread(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("thread_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleDeleteAnalysisThread(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteAnalysisThread(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("thread_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

// 보고서 보관함 (silverone 2026-06-10). 채팅 분석 결과를 스냅샷 저장/조회/삭제.
// project 스코프 — 보고서 탭이 project 단위(/projects/{id}/reports)이기 때문.
func (s *Server) handleCreateSavedResult(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ReportSavedResultCreateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateSavedResult(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleListSavedResults(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListSavedResults(
		r.PathValue("project_id"),
		r.URL.Query().Get("dataset_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleDeleteSavedResult(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteSavedResult(
		r.PathValue("project_id"),
		r.PathValue("result_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

// 보고서 문서 CRUD (silverone 2026-06-11). project 스코프.
func (s *Server) handleCreateReport(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ReportCreateRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateReport(r.PathValue("project_id"), payload)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusCreated, response)
}

func (s *Server) handleListReports(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.ListReports(r.PathValue("project_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetReport(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetReport(
		r.PathValue("project_id"),
		r.PathValue("report_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleUpdateReport(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ReportUpdateRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.UpdateReport(
		r.PathValue("project_id"),
		r.PathValue("report_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleDeleteReport(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteReport(
		r.PathValue("project_id"),
		r.PathValue("report_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) handlePostAnalysisThreadMessage(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.AnalysisThreadMessageRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.PostAnalysisThreadMessage(
		r.Context(),
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("thread_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetAnalysisRun(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetAnalysisRun(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("run_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, response)
}

func (s *Server) handleGetDatasetBuildJob(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	response, err := s.datasetService.GetDatasetBuildJob(
		r.PathValue("project_id"),
		r.PathValue("job_id"),
	)
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

func hasTextColumns(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func writeError(w stdhttp.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"detail": strings.TrimSpace(message),
	})
}

func (s *Server) writeServiceError(w stdhttp.ResponseWriter, err error) {
	var invalid service.ErrInvalidArgument
	var conflict service.ErrConflict
	var missing service.ErrNotFound
	var unauthorized service.ErrUnauthorized
	var forbidden service.ErrForbidden
	switch {
	case errors.As(err, &invalid):
		writeError(w, stdhttp.StatusBadRequest, invalid.Error())
	case errors.As(err, &unauthorized):
		writeError(w, stdhttp.StatusUnauthorized, unauthorized.Error())
	case errors.As(err, &forbidden):
		writeError(w, stdhttp.StatusForbidden, forbidden.Error())
	case errors.As(err, &conflict):
		writeError(w, stdhttp.StatusConflict, conflict.Error())
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

func swaggerUIHTML(specURL string) string {
	if strings.TrimSpace(specURL) == "" {
		specURL = "/openapi.yaml"
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
