package http

import (
	stdhttp "net/http"
	"strconv"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/obs"
)

// dataset_build endpoint handler. ADR-017 / 5/19 — clean 직후 doc-level 3-tier
// 진성 분류 + 절·라벨링 두 build job handler.

// ADR-017 / 5/19 결정 — clean 직후 doc-level 3-tier 진성 분류 build job handler.
func (s *Server) handleCreateDocGenuinenessJob(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetDocGenuinenessBuildRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateDocGenuinenessJob(
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

// 화면 polling용 GET handler — clean / doc_genuineness / clause_label 3종.
// POST와 같은 path. status / progress / error_message / (단계별) summary
// items + applied를 한 응답으로 반환해 화면이 build job API를 직접
// polling하지 않아도 되게 한다.
func (s *Server) handleGetCleanView(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	view, err := s.datasetService.GetCleanView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
}

func (s *Server) handleGetDocGenuinenessView(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	limit, offset := parseArtifactPagination(r)
	genuineness := strings.TrimSpace(r.URL.Query().Get("genuineness"))
	view, err := s.datasetService.GetDocGenuinenessView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		genuineness,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
}

func (s *Server) handleGetClauseLabelView(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	limit, offset := parseArtifactPagination(r)
	aspect := strings.TrimSpace(r.URL.Query().Get("aspect"))
	sentiment := strings.TrimSpace(r.URL.Query().Get("sentiment"))
	view, err := s.datasetService.GetClauseLabelView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		aspect, sentiment,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
}

// parseArtifactPagination — ?limit=&offset= query 파싱. 잘못된 값은 default로
// fallback (service 쪽 normalize와 이중 안전).
func parseArtifactPagination(r *stdhttp.Request) (int, int) {
	limit := 100
	offset := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	return limit, offset
}

func (s *Server) handleCreateClauseLabelJob(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetClauseLabelBuildRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateClauseLabelJob(
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

// silverone 2026-06-10 — 수동 keyword build. precondition clause_label ready.
func (s *Server) handleCreateClauseKeywordsJob(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DatasetClauseKeywordsBuildRequest
	if err := decodeJSONAllowEmpty(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	response, err := s.datasetService.CreateClauseKeywordsJob(
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
	writeJSON(w, stdhttp.StatusAccepted, response.AsAccepted())
}
