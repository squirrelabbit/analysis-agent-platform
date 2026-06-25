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

// handleListDocGenuinenessRuns — 한 버전에 보관된 모델별 진성 분류 결과 목록
// (비교 화면 dropdown용, silverone 2026-06-15).
func (s *Server) handleListDocGenuinenessRuns(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	resp, err := s.datasetService.GetDocGenuinenessRuns(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, resp)
}

// handleCompareDocGenuineness — 진성 분류 모델 비교 (silverone 2026-06-15).
// 한 버전에 보관된 두 모델 결과(model_a/model_b)를 doc_id 기준 1:1 비교한다.
// limit/offset은 불일치 목록에만 적용.
func (s *Server) handleCompareDocGenuineness(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	versionID := strings.TrimSpace(r.URL.Query().Get("version_id"))
	modelA := strings.TrimSpace(r.URL.Query().Get("model_a"))
	modelB := strings.TrimSpace(r.URL.Query().Get("model_b"))
	if versionID == "" || modelA == "" || modelB == "" {
		writeError(w, stdhttp.StatusBadRequest, "version_id, model_a, model_b query params are required")
		return
	}
	limit, offset := parseArtifactPagination(r)
	view, err := s.datasetService.CompareDocGenuineness(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		versionID, modelA, modelB,
		limit, offset,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
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
	// 교차검증(verify) 검토 큐 필터 (ADR-026) — verify artifact에만 의미.
	disagreementOnly := r.URL.Query().Get("disagreement") == "true"
	needsReviewOnly := r.URL.Query().Get("needs_review") == "true"
	view, err := s.datasetService.GetDocGenuinenessView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		genuineness,
		disagreementOnly, needsReviewOnly,
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
	// 교차검증(verify) 검토 큐 필터 (ADR-028) — verify artifact에만 의미.
	disagreementOnly := r.URL.Query().Get("disagreement") == "true"
	needsReviewOnly := r.URL.Query().Get("needs_review") == "true"
	view, err := s.datasetService.GetClauseLabelView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		aspect, sentiment,
		disagreementOnly, needsReviewOnly,
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
	group := strings.TrimSpace(r.URL.Query().Get("group"))
	view, err := s.datasetService.GetClauseKeywordsView(
		r.PathValue("project_id"),
		r.PathValue("dataset_id"),
		r.PathValue("version_id"),
		limit, offset,
		aspect, sentiment, q, group,
	)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, view)
}

// ── 키워드 정제 사전 (silverone 2026-06-25) ────────────────────────────────
// dataset 단위 block(제외)/synonym(병합) 규칙 + append-only 이력. 키워드 뷰가
// 조회 시 overlay로 적용한다(원본 artifact 불변).

func keywordDictActor(r *stdhttp.Request) string {
	if u, ok := userFromContext(r.Context()); ok {
		return u.UserID
	}
	return ""
}

func (s *Server) handleListKeywordDictionary(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	// ?include_inactive=1 이면 비활성 규칙도 포함(이력 화면용). 기본은 활성만.
	activeOnly := strings.TrimSpace(r.URL.Query().Get("include_inactive")) == ""
	rules, err := s.datasetService.ListKeywordDictionaryRules(
		r.PathValue("project_id"), r.PathValue("dataset_id"), activeOnly)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, domain.KeywordDictionaryRuleListResponse{Items: rules})
}

func (s *Server) handleSetKeywordDictionaryRule(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var payload domain.KeywordDictionaryRuleRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, stdhttp.StatusBadRequest, err.Error())
		return
	}
	rule, err := s.datasetService.SetKeywordDictionaryRule(
		r.PathValue("project_id"), r.PathValue("dataset_id"), payload, keywordDictActor(r))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, rule)
}

type keywordRuleActiveRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) setKeywordRuleActive(w stdhttp.ResponseWriter, r *stdhttp.Request, active bool) {
	var payload keywordRuleActiveRequest
	_ = decodeJSONAllowEmpty(r, &payload)
	rule, err := s.datasetService.SetKeywordDictionaryRuleActive(
		r.PathValue("project_id"), r.PathValue("dataset_id"), r.PathValue("rule_id"),
		active, payload.Reason, keywordDictActor(r))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, rule)
}

func (s *Server) handleDeactivateKeywordDictionaryRule(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	s.setKeywordRuleActive(w, r, false)
}

func (s *Server) handleReactivateKeywordDictionaryRule(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	s.setKeywordRuleActive(w, r, true)
}

func (s *Server) handleDeleteKeywordDictionaryRule(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	// 사유는 query(?reason=) 또는 body {reason} 둘 다 허용.
	reason := strings.TrimSpace(r.URL.Query().Get("reason"))
	if reason == "" {
		var payload keywordRuleActiveRequest
		_ = decodeJSONAllowEmpty(r, &payload)
		reason = payload.Reason
	}
	if err := s.datasetService.DeleteKeywordDictionaryRule(
		r.PathValue("project_id"), r.PathValue("dataset_id"), r.PathValue("rule_id"),
		reason, keywordDictActor(r),
	); err != nil {
		s.writeServiceError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) handleListKeywordDictionaryHistory(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	events, err := s.datasetService.ListKeywordDictionaryEvents(
		r.PathValue("project_id"), r.PathValue("dataset_id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, domain.KeywordDictionaryEventListResponse{Items: events})
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
