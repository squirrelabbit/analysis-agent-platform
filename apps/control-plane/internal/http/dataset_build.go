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

// handleLLOAModelOptions — 전처리 빌드(doc_genuineness/clause_label) 모델 선택지.
// LLOA_MODELS env allowlist 기반, default는 LLOA_MODEL(worker default) 일치 항목.
// 빈 allowlist면 items: []로 응답 — 프론트는 select 자체를 숨긴다. (2026-06-12)
func (s *Server) handleLLOAModelOptions(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	options := s.datasetService.LLOAModelOptions()
	if options == nil {
		options = []domain.LLOAModelOption{}
	}
	writeJSON(w, stdhttp.StatusOK, map[string]any{"items": options})
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

// silverone 2026-06-11 — 진성 라벨 수동 보정. PUT은 set(원본과 같으면 해제),
// DELETE는 되돌리기. effective label은 GET doc_genuineness 응답에서 합성된다.
func (s *Server) handleSetDocGenuinenessOverride(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.DocGenuinenessOverrideRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	override, err := s.datasetService.SetDocGenuinenessOverride(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		r.PathValue("doc_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, override)
}

func (s *Server) handleDeleteDocGenuinenessOverride(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteDocGenuinenessOverride(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		r.PathValue("doc_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
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

// silverone 2026-06-11 — 절 라벨링 aspect/sentiment 수동 보정. PATCH로 set,
// DELETE override로 되돌리기. effective는 GET clause_label 응답에서 합성된다.
func (s *Server) handleSetClauseLabelOverride(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.ClauseLabelOverrideRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	override, err := s.datasetService.SetClauseLabelOverride(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		r.PathValue("clause_id"),
		payload,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, override)
}

func (s *Server) handleDeleteClauseLabelOverride(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if err := s.datasetService.DeleteClauseLabelOverride(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		r.PathValue("clause_id"),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
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

// silverone 2026-06-10 — clause_keywords 대시보드/조회 (화면 polling용).
// summary(KPI/aspect/sentiment/top5) + 필터(aspect/sentiment/q)·페이징된 item table.
func (s *Server) handleGetClauseKeywordsView(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	limit, offset := parseArtifactPagination(r)
	aspect := strings.TrimSpace(r.URL.Query().Get("aspect"))
	sentiment := strings.TrimSpace(r.URL.Query().Get("sentiment"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	view, err := s.datasetService.GetClauseKeywordsView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		aspect, sentiment, q,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
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
