package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/metrics"
)

// defaultPythonAITaskTimeout — SetPythonAITaskTimeout 미주입 시 fallback.
// config default(PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC=120)와 동일하게 둬서
// wiring을 안 거치는 test 경로도 기존과 같은 동작을 갖게 한다.
const defaultPythonAITaskTimeout = 120 * time.Second

// analysis API 모델 (silverone 2026-05-26, vault: analysis_api_model_2026-05-26)
//
// 중심 객체는 `/analyze` endpoint가 아니라 analysis_thread + analysis_message +
// analysis_run 3개다. `/analyze`는 (create thread + first user message + start run)
// shortcut. 자세한 모델/정책은 vault 문서 참조.
//
// 이 파일이 담당하는 두 흐름:
//
//   - ExecuteAnalyze (/versions/{vid}/analyze, debug/replay)
//     thread/message/run 저장 없음. plan만 받아서 validator → executor.
//     direct-plan smoke / version 고정 디버깅용. 응답은 단순 result shape.
//
//   - ExecuteAnalyzeOnActiveVersion (내부)
//     active version을 resolve해 ExecuteAnalyze에 위임. analysis_threads.go의
//     stateful 흐름이 이걸 첫 turn에 호출한다.
//
// 두 흐름 모두 Temporal workflow 미사용 — sync HTTP. Python `/tasks/analyze`
// 응답을 passthrough하되 (project_id, dataset_id, version_id, mode) wrapper로
// 감싼다. wire contract `plan_version: "v2"`는 유지.

// analyze 데이터 계약 타입(AnalyzeRequest / AnalyzeResponse / analyzeArtifactPaths 등)은
// analyze_types.go로 분리했다 (silverone 2026-06-04, 구조 정리). 이 파일은 로직만 담는다.

// ExecuteAnalyzeOnActiveVersion — analysis_thread 흐름의 첫 turn 내부 진입점.
// dataset의 active version을 1회 resolve해 ExecuteAnalyze에 위임한다. caller는
// analysis_threads.go::AnalyzeDatasetAsNewThread (thread 생성 + first user
// message + run save).
//
// 화면이 직접 호출하는 외부 endpoint는 `POST /datasets/{did}/analyze`이고,
// 그 handler가 AnalyzeDatasetAsNewThread를 부른다. 즉 이 함수는 stateful 흐름
// 안에서 active version → artifact path → Python worker 호출만 담당.
//
// active version이 없으면 ErrInvalidArgument.
func (a *AnalyzeService) ExecuteAnalyzeOnActiveVersion(
	ctx context.Context,
	projectID, datasetID string,
	req AnalyzeRequest,
) (AnalyzeResponse, error) {
	dataset, err := a.versions.GetDataset(projectID, datasetID)
	if err != nil {
		return AnalyzeResponse{}, err
	}
	versionID := ""
	if dataset.ActiveDatasetVersionID != nil {
		versionID = strings.TrimSpace(*dataset.ActiveDatasetVersionID)
	}
	if versionID == "" {
		return AnalyzeResponse{}, ErrInvalidArgument{
			Message: "dataset has no active version — upload a dataset version first, or use the explicit /versions/{version_id}/analyze endpoint",
		}
	}
	return a.ExecuteAnalyze(ctx, projectID, datasetID, versionID, req)
}

// ExecuteAnalyze — version-specific 진입점. 외부 endpoint는
// `POST /versions/{vid}/analyze` (debug/replay). thread/message/run 저장은 없다.
//
// 내부적으로 두 caller가 이 함수를 공유한다:
//   - handleAnalyze (HTTP, debug — plan만 받음)
//   - analysis_threads.go (stateful 흐름 — user_question 또는 reuse patched plan)
//
// project/dataset/version은 caller가 path에서 추출, body는 raw payload 그대로.
func (a *AnalyzeService) ExecuteAnalyze(
	ctx context.Context,
	projectID, datasetID, versionID string,
	req AnalyzeRequest,
) (AnalyzeResponse, error) {
	hasPlan := isRawMessageSet(req.Plan)
	hasUQ := strings.TrimSpace(req.UserQuestion) != ""
	if hasPlan && hasUQ {
		return AnalyzeResponse{}, ErrInvalidArgument{
			Message: "analyze payload must include exactly one of 'plan' or 'user_question', not both",
		}
	}
	if !hasPlan && !hasUQ {
		return AnalyzeResponse{}, ErrInvalidArgument{
			Message: "analyze payload requires either 'user_question' (POST /datasets/{did}/analyze) or 'plan' (POST /versions/{vid}/analyze)",
		}
	}

	version, err := a.versions.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		return AnalyzeResponse{}, err
	}
	paths, err := a.resolveAnalyzeArtifactPaths(version)
	if err != nil {
		return AnalyzeResponse{}, err
	}

	mode := "plan"
	if hasUQ {
		mode = "user_question"
	}

	workerPayload := map[string]any{
		"dataset_version_id": versionID,
		"artifact_paths":     paths.asPayload(),
	}
	if len(req.ReuseMetadata) > 0 {
		// silverone 2026-05-26 (ADR-020 PR-A) — worker composer가 reuse_applied
		// 템플릿을 선택할 수 있게 hint 전달.
		workerPayload["reuse_metadata"] = req.ReuseMetadata
	}
	if hasPlan {
		workerPayload["plan"] = req.Plan
	} else {
		workerPayload["user_question"] = req.UserQuestion
		if len(req.ConversationContext) > 0 {
			workerPayload["conversation_context"] = req.ConversationContext
		}
		// 2026-05-22 — dataset-specific docs 컬럼을 SourceSummary에서 자동
		// derive해서 worker payload에 inject한다. 화면이 컬럼 메타를 들고
		// 다시 보낼 필요 없음. SourceSummary가 비어 있으면 inject 생략
		// (planner는 standard docs 컬럼만 보고 답한다).
		if extra := deriveDocsExtraColumns(version); len(extra) > 0 {
			workerPayload["docs_extra_columns"] = extra
		}
	}

	// taxonomy-driven config Phase 3-B wire (silverone 2026-05-27) —
	// clause_label artifact metadata에서 taxonomy_id / taxonomy_hash를 추출해
	// worker가 정합성 체크할 수 있게 inject. 옛 artifact는 metadata가 없어
	// nil이 반환되고 worker는 legacy_missing 분기로 떨어진다.
	if taxonomyMeta := deriveClauseLabelTaxonomyMetadata(version); len(taxonomyMeta) > 0 {
		workerPayload["clause_label_metadata"] = taxonomyMeta
	}

	// silverone 2026-06-01 (rename PR A) — canonical worker URL은 /tasks/analyze.
	// 옛 /tasks/analyze_v2는 worker 측 alias로 유지되지만 새 호출은 canonical
	// path만 쓴다.
	rawResult, err := a.postPythonAITask(ctx, "/tasks/analyze", workerPayload)
	if err != nil {
		return AnalyzeResponse{}, err
	}

	return AnalyzeResponse{
		ProjectID: projectID,
		DatasetID: datasetID,
		VersionID: versionID,
		Mode:      mode,
		Result:    rawResult,
	}, nil
}

// resolveAnalyzeArtifactPaths — version metadata에서 3 artifact path를
// 가져온다. 누락되거나 disk에 없으면 ErrInvalidArgument.
func (a *AnalyzeService) resolveAnalyzeArtifactPaths(version domain.DatasetVersion) (analyzeArtifactPaths, error) {
	docs := cleanArtifactRef(version)
	clauses := strings.TrimSpace(metadataString(version.Metadata, "clause_label_ref", ""))
	if clauses == "" {
		clauses = strings.TrimSpace(metadataString(version.Metadata, "clause_label_uri", ""))
	}
	genuineness := strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_ref", ""))
	if genuineness == "" {
		genuineness = strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_uri", ""))
	}
	// silverone 2026-06-10 — clause_keywords는 optional artifact(키워드 build이 돈 버전에만
	// 존재). 없으면 required 검증/주입에서 빠지고, 있으면 readable 검증 후 worker에 inject.
	clauseKeywords := strings.TrimSpace(metadataString(version.Metadata, "clause_keywords_ref", ""))
	if clauseKeywords == "" {
		clauseKeywords = strings.TrimSpace(metadataString(version.Metadata, "clause_keywords_uri", ""))
	}

	missing := make([]string, 0, 3)
	if docs == "" {
		missing = append(missing, "docs (cleaned.parquet)")
	}
	if clauses == "" {
		missing = append(missing, "clauses (clause_label.jsonl)")
	}
	if genuineness == "" {
		missing = append(missing, "genuineness (doc_genuineness.jsonl)")
	}
	if len(missing) > 0 {
		return analyzeArtifactPaths{}, ErrInvalidArgument{
			Message: fmt.Sprintf("analyze requires the following artifacts on this version: %s", strings.Join(missing, ", ")),
		}
	}

	// silverone 2026-06-04 — worker로 넘기기 전에 artifact가 실제로 읽을 수 있는지
	// 검증(존재/regular file/size>0/format framing). 깨진 artifact를 worker가 읽다
	// 실패하기 전에 운영자-친화 에러로 차단. 고정 순서(docs→clauses→genuineness).
	for _, it := range []struct {
		label  string
		path   string
		format artifactFormat
	}{
		{"docs", docs, artifactParquet},
		{"clauses", clauses, artifactJSONL},
		{"genuineness", genuineness, artifactJSONL},
	} {
		if err := validateArtifactReadable(it.label, it.path, it.format); err != nil {
			return analyzeArtifactPaths{}, err
		}
	}

	// optional clause_keywords — ref가 있으면 readable 검증, 깨졌으면 차단(없는 것과 구분).
	if clauseKeywords != "" {
		if err := validateArtifactReadable("clause_keywords", clauseKeywords, artifactJSONL); err != nil {
			return analyzeArtifactPaths{}, err
		}
	}

	return analyzeArtifactPaths{
		Docs:           docs,
		Clauses:        clauses,
		Genuineness:    genuineness,
		ClauseKeywords: clauseKeywords,
	}, nil
}

// postPythonAITask — Python worker에 task payload를 POST하고 응답 body를 raw로
// 돌려준다. PythonAIWorkerURL이 비어 있으면 ErrInvalidArgument.
func (a *AnalyzeService) postPythonAITask(ctx context.Context, path string, payload map[string]any) (json.RawMessage, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(a.workerURL), "/")
	if baseURL == "" {
		return nil, ErrInvalidArgument{Message: "python-ai worker is not configured"}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("analyze marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("analyze request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	timeout := a.workerTimeout
	if timeout <= 0 {
		timeout = defaultPythonAITaskTimeout
	}
	client := &http.Client{Timeout: timeout}

	// silverone 2026-06-04 (metrics 1차) — 실제 worker 호출만 계측. timer 등록 이후의
	// 모든 return은 status="error"가 기본이고 성공 경로에서만 "ok"로 바꾼다. URL 미설정/
	// marshal/request build 등 호출 이전 실패는 worker call 메트릭에 포함하지 않는다.
	start := time.Now()
	status := "error"
	defer func() {
		metrics.RecordAnalysisWorkerCall(status, time.Since(start).Milliseconds())
	}()

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("analyze worker call: %w", err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("analyze worker read: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("analyze worker %s returned %d: %s", path, resp.StatusCode, buf.String())
	}
	status = "ok"
	return json.RawMessage(buf.Bytes()), nil
}

func isRawMessageSet(raw json.RawMessage) bool {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return false
	}
	return true
}

// docsExtraColumnsFromAnalysis — clean_summary.analysis_columns(=clean이 materialize한
// typed 분석 컬럼)를 docs_extra_columns payload로 변환한다. name=parquet alias(SQL용),
// type=advertise/parquet type, label/source_column=원본 CSV 컬럼명. clean_summary나
// analysis_columns가 없으면(옛 데이터셋) nil을 반환해 caller가 fallback하게 한다.
// silverone 2026-06-08 (파일럿).
func docsExtraColumnsFromAnalysis(version domain.DatasetVersion) []map[string]any {
	summaryRaw, ok := version.Metadata["clean_summary"].(map[string]any)
	if !ok {
		return nil
	}
	colsRaw, ok := summaryRaw["analysis_columns"].([]any)
	if !ok || len(colsRaw) == 0 {
		return nil
	}
	asString := func(v any) string {
		s, _ := v.(string)
		return strings.TrimSpace(s)
	}
	result := make([]map[string]any, 0, len(colsRaw))
	for _, item := range colsRaw {
		col, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := asString(col["name"])
		if name == "" {
			continue
		}
		colType := asString(col["type"])
		if colType == "" {
			colType = "string"
		}
		label := asString(col["label"])
		source := asString(col["source_column"])
		if source == "" {
			source = label
		}
		result = append(result, map[string]any{
			"name":          name,
			"type":          colType,
			"description":   label,
			"label":         label,
			"source_column": source,
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// deriveDocsExtraColumns — dataset의 원본 컬럼 중 plan_v2 docs table의 표준
// 컬럼이 아닌 것들을 추려 planner prompt에 inject할 수 있는 형태로 반환한다.
// 화면이 매번 컬럼 메타를 들고 보낼 필요 없도록 서버가 SourceSummary를 읽어
// 자동 derive한다 (J-안, 2026-05-22).
//
// 표준 docs 컬럼은 planner/schema.py의 TABLE_SCHEMAS["docs"] 기준:
//   - doc_id / row_id / raw_text / cleaned_text / created_at
//
// 그 외 클린 단계 메타 컬럼(source_row_index 등)도 dataset-specific으로 노출할
// 가치가 없어 함께 필터링한다. SourceSummary가 비어 있으면 nil 반환.
func deriveDocsExtraColumns(version domain.DatasetVersion) []map[string]any {
	// silverone 2026-06-08 (파일럿) — clean이 materialize한 analysis_columns가 있으면
	// 우선 사용한다. 이건 실제 parquet에 적재된 SQL-safe alias + typed 컬럼이라
	// advertised type == executable parquet type을 보장한다. (옛 source-summary
	// 경로는 원본 컬럼명을 advertise했지만 parquet에 없어 executor에서 걸러졌다.)
	if cols := docsExtraColumnsFromAnalysis(version); len(cols) > 0 {
		return cols
	}
	summary := loadDatasetSourceSummary(version.StorageURI, 0)
	if summary == nil || len(summary.Columns) == 0 {
		return nil
	}
	// silverone 2026-05-28 (clean 정식화) — clean output 표준 9 컬럼.
	// 옛 키: clean_disposition / clean_flags / clean_regex_applied_rules.
	// 새 schema: clean_status로 rename, flags / regex_applied_rules는
	// summary 통계로만 집계되고 row-level 컬럼에서 제거. source 원본은
	// source_json(JSON string)으로 보존.
	standard := map[string]bool{
		"doc_id":           true,
		"row_id":           true,
		"raw_text":         true,
		"cleaned_text":     true,
		"created_at":       true,
		"source_row_index": true,
		"clean_status":     true,
		"clean_reason":     true,
		"source_json":      true,
	}
	result := make([]map[string]any, 0, len(summary.Columns))
	seen := map[string]bool{}
	for _, col := range summary.Columns {
		name := strings.TrimSpace(col.Name)
		if name == "" || standard[name] || seen[name] {
			continue
		}
		seen[name] = true
		colType := strings.TrimSpace(col.Type)
		if colType == "" {
			colType = "string"
		}
		result = append(result, map[string]any{
			"name":        name,
			"type":        colType,
			"description": "",
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// deriveClauseLabelTaxonomyMetadata — clause_label artifact summary에서
// taxonomy_id / taxonomy_hash를 추출해 worker payload에 inject할 dict로
// 변환한다.
//
// silverone 2026-05-27 (taxonomy-driven config Phase 3-B wire). Python worker
// 측은 “clause_label_metadata“ payload 필드를 받아 “check_taxonomy_
// compatibility“로 정합성 체크한다. 옛 artifact는 summary에 taxonomy_id가
// 없어 nil이 반환되고, worker는 “legacy_missing“ 분기로 떨어진다.
//
// 우선순위:
//  1. version.Metadata["clause_label_summary"]에 taxonomy_id 있으면 사용
//  2. 없으면 nil (legacy_missing 허용)
func deriveClauseLabelTaxonomyMetadata(version domain.DatasetVersion) map[string]any {
	summary, ok := version.Metadata["clause_label_summary"].(map[string]any)
	if !ok {
		return nil
	}
	taxonomyID := strings.TrimSpace(stringFromAny(summary["taxonomy_id"]))
	taxonomyHash := strings.TrimSpace(stringFromAny(summary["taxonomy_hash"]))
	if taxonomyID == "" && taxonomyHash == "" {
		return nil
	}
	out := map[string]any{}
	if taxonomyID != "" {
		out["taxonomy_id"] = taxonomyID
	}
	if taxonomyHash != "" {
		out["taxonomy_hash"] = taxonomyHash
	}
	return out
}

// stringFromAny — interface{} 값을 string으로 안전 변환. string이 아닌 값은
// "" 반환 (number/bool/nil 모두).
func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

var _ = errors.New // keep errors import for future use
