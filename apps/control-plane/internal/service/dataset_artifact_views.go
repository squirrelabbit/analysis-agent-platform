package service

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/obs"

	_ "github.com/marcboeker/go-duckdb"
)

// 2026-05-21 — 화면 polling용 GET endpoint 응답 빌더.
// /versions/{vid}/doc_genuineness, /versions/{vid}/clause_label 두 곳이
// 공유. DuckDB on-demand query로 summary + items + total을 즉시 집계한다
// (festival 50 docs / 268 clauses 기준 sub-second). 1만+ clause에서 성능
// 문제 시 build 완료 시점에 artifact metadata로 캐시 옮기는 것 별도 작업.

const (
	docGenuinenessBuildType = "doc_genuineness"
	clauseLabelBuildType    = "clause_label"
)

// enrichViewWithJob — view 공통 필드(started_at / completed_at / error_message
// / progress / job_id)를 latest job + metadata에서 채워 넣는다. clean /
// doc_genuineness / clause_label 3 view가 공유.
func enrichViewWithJob(view *domain.DatasetArtifactView, job *domain.DatasetBuildJob, metadata map[string]any, buildType string) {
	if job != nil {
		jobID := job.JobID
		view.JobID = &jobID
		view.StartedAt = job.StartedAt
		view.CompletedAt = job.CompletedAt
		view.ErrorMessage = job.ErrorMessage
		view.DurationSeconds = computeDurationSeconds(job.StartedAt, job.CompletedAt)
	}
	prefix := buildJobMetadataPrefix(buildType)
	if prefix == "" {
		return
	}
	if rawProgress := loadBuildJobProgress(metadata, prefix); rawProgress != nil {
		view.Progress = &domain.ArtifactProgress{
			Percent:       rawProgress.Percent,
			ProcessedRows: rawProgress.ProcessedRows,
			TotalRows:     rawProgress.TotalRows,
			ETASeconds:    rawProgress.ETASeconds,
			Message:       rawProgress.Message,
			UpdatedAt:     rawProgress.UpdatedAt,
		}
	}
}

// computeDurationSeconds — view.duration_seconds 계산.
//   - started == nil → nil (queued까지 가지 않은 케이스)
//   - completed != nil → completed - started (확정값)
//   - completed == nil (running) → now - started (진행 중 실시간)
// 음수가 나오면(시계 어긋남 등) 0으로 clamp해 의미 없는 값이 화면에 노출되는 걸 막는다.
func computeDurationSeconds(started, completed *time.Time) *float64 {
	if started == nil {
		return nil
	}
	end := time.Now().UTC()
	if completed != nil {
		end = *completed
	}
	seconds := end.Sub(*started).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	return &seconds
}

// cleanSummaryToMap — domain.DatasetCleanSummary를 view summary map으로 변환.
// 화면이 build_type별로 다른 summary shape을 그대로 받도록 한다.
func cleanSummaryToMap(summary *domain.DatasetCleanSummary) map[string]any {
	if summary == nil {
		return nil
	}
	result := map[string]any{
		"input_row_count":  summary.InputRowCount,
		"output_row_count": summary.OutputRowCount,
		"kept_count":       summary.KeptCount,
		"dropped_count":    summary.DroppedCount,
	}
	if summary.SkippedRowCount > 0 {
		result["skipped_row_count"] = summary.SkippedRowCount
	}
	if summary.TextColumn != "" {
		result["text_column"] = summary.TextColumn
	}
	if len(summary.TextColumns) > 0 {
		result["text_columns"] = summary.TextColumns
	}
	if summary.SourceInputCharCount > 0 {
		result["source_input_char_count"] = summary.SourceInputCharCount
	}
	if summary.CleanedInputCharCount > 0 {
		result["cleaned_input_char_count"] = summary.CleanedInputCharCount
	}
	if summary.CleanReducedCharCount > 0 {
		result["clean_reduced_char_count"] = summary.CleanReducedCharCount
	}
	if len(summary.CleanRegexRuleHits) > 0 {
		result["clean_regex_rule_hits"] = summary.CleanRegexRuleHits
	}
	return result
}

// GetCleanView — clean artifact 화면 polling용 응답. items / pagination /
// applied는 채우지 않는다 (clean은 deterministic 단계라 prompt 개념 없음 +
// 결과 raw row는 별도 download endpoint로 제공).
func (s *DatasetService) GetCleanView(projectID, datasetID, datasetVersionID string) (domain.DatasetArtifactView, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}

	view := domain.DatasetArtifactView{BuildType: datasetBuildTypeClean}

	latestJob := latestJobForBuildType(s, projectID, version.DatasetVersionID, datasetBuildTypeClean)
	ref := strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "clean_uri", ""))
	}
	view.Status = resolveArtifactStatus(ref, latestJob, version.CleanStatus)
	enrichViewWithJob(&view, latestJob, version.Metadata, datasetBuildTypeClean)

	// clean summary는 build 완료 시 metadata에 캐시돼 있어 그대로 노출. ready가
	// 아니면 비워두고 status로 판단.
	if version.CleanSummary != nil && view.Status == "completed" {
		view.Summary = cleanSummaryToMap(version.CleanSummary)
	}
	return view, nil
}

// GetDocGenuinenessView — doc_genuineness artifact 화면 polling용 응답.
func (s *DatasetService) GetDocGenuinenessView(
	projectID, datasetID, datasetVersionID string,
	limit, offset int,
) (domain.DatasetArtifactView, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	limit, offset = normalizeArtifactPagination(limit, offset)

	view := domain.DatasetArtifactView{
		BuildType: docGenuinenessBuildType,
		Items:     []map[string]any{},
		Pagination: &domain.ArtifactPagination{
			Limit:  limit,
			Offset: offset,
		},
	}

	latestJob := latestJobForBuildType(s, projectID, version.DatasetVersionID, docGenuinenessBuildType)
	ref := strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_uri", ""))
	}
	view.Status = resolveArtifactStatus(ref, latestJob, metadataString(version.Metadata, "doc_genuineness_status", ""))
	enrichViewWithJob(&view, latestJob, version.Metadata, docGenuinenessBuildType)

	if !artifactReadyForView(ref) {
		return view, nil
	}
	if _, err := os.Stat(ref); err != nil {
		if os.IsNotExist(err) {
			return view, nil
		}
		return domain.DatasetArtifactView{}, err
	}

	// silverone 2026-05-28 (옵션 A) — cleaned.parquet의 cleaned_text를 doc_id
	// 기준 LEFT JOIN해 응답에 포함. 운영자가 reason 외에 본문 자체를 보고
	// 판별 적절성을 확인할 수 있게 한다. clean artifact가 없거나 cleanRef가
	// 비어 있으면 본문 없이 기존 schema(doc_id/genuineness/reason/source)로
	// degrade. row_count(pagination.total)는 doc_genuineness 기준 유지.
	cleanRef := strings.TrimSpace(metadataString(version.Metadata, "clean_uri", ""))
	if cleanRef == "" {
		cleanRef = strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
	}
	if cleanRef != "" {
		if _, statErr := os.Stat(cleanRef); statErr != nil {
			cleanRef = "" // file 없으면 join 생략
		}
	}
	summary, prompt, total, items, err := loadDocGenuinenessArtifact(ref, cleanRef, limit, offset, version.DatasetVersionID)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	view.Summary = summary
	if prompt != "" {
		view.Applied = map[string]any{"prompt_version": prompt}
	}
	view.Items = items
	view.Pagination.Total = total
	return view, nil
}

// GetClauseLabelView — clause_label artifact 화면 polling용 응답.
func (s *DatasetService) GetClauseLabelView(
	projectID, datasetID, datasetVersionID string,
	limit, offset int,
) (domain.DatasetArtifactView, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	limit, offset = normalizeArtifactPagination(limit, offset)

	view := domain.DatasetArtifactView{
		BuildType: clauseLabelBuildType,
		Items:     []map[string]any{},
		Pagination: &domain.ArtifactPagination{
			Limit:  limit,
			Offset: offset,
		},
	}

	latestJob := latestJobForBuildType(s, projectID, version.DatasetVersionID, clauseLabelBuildType)
	ref := strings.TrimSpace(metadataString(version.Metadata, "clause_label_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "clause_label_uri", ""))
	}
	view.Status = resolveArtifactStatus(ref, latestJob, metadataString(version.Metadata, "clause_label_status", ""))
	enrichViewWithJob(&view, latestJob, version.Metadata, clauseLabelBuildType)

	if !artifactReadyForView(ref) {
		return view, nil
	}
	if _, err := os.Stat(ref); err != nil {
		if os.IsNotExist(err) {
			return view, nil
		}
		return domain.DatasetArtifactView{}, err
	}

	// clause_label_prompt_version은 build 완료 시 metadata에 저장되므로 먼저 본다.
	// 없으면 DuckDB로 첫 행 prompt_version을 회수한다 (Applied source single).
	prompt := strings.TrimSpace(metadataString(version.Metadata, "clause_label_prompt_version", ""))
	summary, fallbackPrompt, total, items, err := loadClauseLabelArtifact(ref, limit, offset)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	view.Summary = summary
	if prompt == "" {
		prompt = fallbackPrompt
	}
	if prompt != "" {
		view.Applied = map[string]any{"prompt_version": prompt}
	}
	view.Items = items
	view.Pagination.Total = total
	return view, nil
}

// ===== helpers =====

func normalizeArtifactPagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// resolveArtifactStatus — 결정 4 (status 매핑) 그대로 구현.
//   - artifact 없음 + job 없음 → not_started
//   - 최근 job queued/running/failed → 그 status
//   - artifact ref 있음 + 최근 job completed → completed
func resolveArtifactStatus(ref string, latestJob *domain.DatasetBuildJob, metadataStatus string) string {
	hasArtifact := strings.TrimSpace(ref) != ""
	if latestJob == nil {
		if hasArtifact {
			// 옛 dataset에서 job row 없이 artifact만 있는 케이스 — completed로 본다.
			return "completed"
		}
		return "not_started"
	}
	switch latestJob.Status {
	case "queued", "running", "failed":
		return latestJob.Status
	case "completed":
		if hasArtifact {
			return "completed"
		}
		// 완료라고 표시됐지만 artifact 안 남은 경우 — 보수적으로 failed 처리.
		return "failed"
	default:
		// 옛 상태 값 또는 unknown — metadata fallback.
		if strings.TrimSpace(metadataStatus) != "" {
			return metadataStatus
		}
		return latestJob.Status
	}
}

func artifactReadyForView(ref string) bool {
	return strings.TrimSpace(ref) != ""
}

// latestJobForBuildType — 같은 dataset_version의 build job 중 해당 build_type
// 최신 1건. created_at DESC 순.
func latestJobForBuildType(s *DatasetService, projectID, datasetVersionID, buildType string) *domain.DatasetBuildJob {
	items, err := s.store.ListDatasetBuildJobs(projectID, datasetVersionID)
	if err != nil || len(items) == 0 {
		return nil
	}
	for index := range items {
		if items[index].BuildType == buildType {
			job := items[index]
			return &job
		}
	}
	return nil
}

// loadDocGenuinenessArtifact — DuckDB on-demand로 summary/total/items + 첫 행
// prompt_version 회수. silverone 2026-05-28 (옵션 A) — cleanRef가 주어지면
// cleaned.parquet의 cleaned_text를 doc_id 기준 LEFT JOIN해 items 응답에
// 포함한다. join miss는 본문 null로 두고 obs warning으로 카운트 노출.
func loadDocGenuinenessArtifact(ref, cleanRef string, limit, offset int, datasetVersionID string) (map[string]any, string, int, []map[string]any, error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, "", 0, nil, err
	}
	defer cleanup()

	source := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))

	// total + by-genuineness 집계.
	total, byGenuineness, err := aggregateGroupedCounts(db, source, "genuineness")
	if err != nil {
		return nil, "", 0, nil, err
	}
	summary := map[string]any{
		"total":       total,
		"genuineness": byGenuineness,
	}

	prompt, err := firstStringValue(db, source, "prompt_version")
	if err != nil {
		return nil, "", 0, nil, err
	}

	if cleanRef != "" {
		// cleaned.parquet의 row_id 컬럼이 doc_genuineness.jsonl의 doc_id와
		// 동일 값 ({version_id}:row:N). LEFT JOIN으로 본문 누락 시에도
		// item은 그대로 유지하고 cleaned_text만 null.
		cleanSource := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
		itemQuery := fmt.Sprintf(
			`SELECT dg.doc_id, dg.genuineness, dg.reason, dg.source, c.cleaned_text
			 FROM %s AS dg
			 LEFT JOIN %s AS c ON dg.doc_id = c.row_id
			 ORDER BY dg.doc_id
			 LIMIT %d OFFSET %d`,
			source, cleanSource, limit, offset,
		)
		items, err := scanArtifactRows(db, itemQuery, []string{"doc_id", "genuineness", "reason", "source", "cleaned_text"})
		if err != nil {
			// JOIN 실패 시(예: cleaned.parquet에 row_id 컬럼 없음) 본문 없이
			// 기존 schema로 fallback. 운영자 진단용 obs warning.
			obs.Logger.Warn("dataset.doc_genuineness.view.cleaned_text_join_failed",
				"event", "dataset.doc_genuineness.view.cleaned_text_join_failed",
				"dataset_version_id", datasetVersionID,
				"clean_ref", cleanRef,
				"error", err.Error(),
			)
			return loadDocGenuinenessArtifactWithoutBody(db, source, summary, prompt, total, limit, offset)
		}

		// join miss 카운트(전체 base — 페이징 무관) — 운영자가 본문 누락
		// 비율을 인지할 수 있도록.
		missQuery := fmt.Sprintf(
			`SELECT COUNT(*)
			 FROM %s AS dg
			 LEFT JOIN %s AS c ON dg.doc_id = c.row_id
			 WHERE c.row_id IS NULL`,
			source, cleanSource,
		)
		var missCount int
		if scanErr := db.QueryRow(missQuery).Scan(&missCount); scanErr == nil && missCount > 0 {
			obs.Logger.Warn("dataset.doc_genuineness.view.cleaned_text_join_miss",
				"event", "dataset.doc_genuineness.view.cleaned_text_join_miss",
				"dataset_version_id", datasetVersionID,
				"miss_count", missCount,
				"total", total,
			)
		}
		return summary, prompt, total, items, nil
	}

	return loadDocGenuinenessArtifactWithoutBody(db, source, summary, prompt, total, limit, offset)
}

// loadDocGenuinenessArtifactWithoutBody — cleanRef 없거나 join 실패 시 본문
// 컬럼 없이 기존 schema(doc_id/genuineness/reason/source)로 items 반환.
func loadDocGenuinenessArtifactWithoutBody(db *sql.DB, source string, summary map[string]any, prompt string, total, limit, offset int) (map[string]any, string, int, []map[string]any, error) {
	itemQuery := fmt.Sprintf(
		`SELECT doc_id, genuineness, reason, source
		 FROM %s
		 ORDER BY doc_id
		 LIMIT %d OFFSET %d`,
		source, limit, offset,
	)
	items, err := scanArtifactRows(db, itemQuery, []string{"doc_id", "genuineness", "reason", "source"})
	if err != nil {
		return nil, "", 0, nil, err
	}
	return summary, prompt, total, items, nil
}

// loadClauseLabelArtifact — DuckDB on-demand로 summary/total/items + 첫 행
// prompt_version 회수. clause_id는 `{doc_id}-{partition_row_index}`로 즉시 생성.
func loadClauseLabelArtifact(ref string, limit, offset int) (map[string]any, string, int, []map[string]any, error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, "", 0, nil, err
	}
	defer cleanup()

	source := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))

	// total + 2 grouping (sentiment, aspect).
	total, bySentiment, err := aggregateGroupedCounts(db, source, "sentiment")
	if err != nil {
		return nil, "", 0, nil, err
	}
	_, byAspect, err := aggregateGroupedCounts(db, source, "aspect")
	if err != nil {
		return nil, "", 0, nil, err
	}
	summary := map[string]any{
		"total":     total,
		"sentiment": bySentiment,
		"aspect":    byAspect,
	}

	prompt, err := firstStringValue(db, source, "prompt_version")
	if err != nil {
		return nil, "", 0, nil, err
	}

	// clause_id는 doc_id 내 ROW_NUMBER에서 1을 빼 0-base index로 만든다.
	// ROW_NUMBER OVER 외부에 ORDER BY가 없으면 jsonl scan 순서를 따른다 — file
	// 작성 순서가 곧 표시 순서.
	itemQuery := fmt.Sprintf(
		`WITH ordered AS (
		    SELECT *, ROW_NUMBER() OVER () AS _rn
		    FROM %s
		 )
		 SELECT
		    doc_id,
		    doc_id || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY _rn) - 1 AS VARCHAR) AS clause_id,
		    clause, sentiment, aspect, source
		 FROM ordered
		 ORDER BY _rn
		 LIMIT %d OFFSET %d`,
		source, limit, offset,
	)
	items, err := scanArtifactRows(db, itemQuery, []string{"doc_id", "clause_id", "clause", "sentiment", "aspect", "source"})
	if err != nil {
		return nil, "", 0, nil, err
	}
	return summary, prompt, total, items, nil
}

// openTempDuckDB — clean_download / artifact view 공통 패턴. temp duckdb file
// 생성 + 사용 후 cleanup callback 반환.
func openTempDuckDB() (*sql.DB, func(), error) {
	tempHandle, err := os.CreateTemp("", "artifact-view-*.duckdb")
	if err != nil {
		return nil, nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		_ = os.Remove(dbPath)
		return nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	}
	return db, cleanup, nil
}

// aggregateGroupedCounts — total + group_by 컬럼별 count map 반환.
// total은 전체 행 수 (NULL group 포함). map[group_key]count.
func aggregateGroupedCounts(db *sql.DB, source, groupColumn string) (int, map[string]int, error) {
	rows, err := db.Query(fmt.Sprintf(
		`SELECT %s, COUNT(*) AS cnt FROM %s GROUP BY %s`,
		groupColumn, source, groupColumn,
	))
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	result := map[string]int{}
	total := 0
	for rows.Next() {
		var keyRaw sql.NullString
		var cnt int
		if err := rows.Scan(&keyRaw, &cnt); err != nil {
			return 0, nil, err
		}
		key := "unknown"
		if keyRaw.Valid {
			trimmed := strings.TrimSpace(keyRaw.String)
			if trimmed != "" {
				key = trimmed
			}
		}
		result[key] += cnt
		total += cnt
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return total, result, nil
}

// firstStringValue — 첫 행에서 column 값 1개 추출. prompt_version 회수용.
func firstStringValue(db *sql.DB, source, column string) (string, error) {
	row := db.QueryRow(fmt.Sprintf(`SELECT %s FROM %s LIMIT 1`, column, source))
	var raw sql.NullString
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	if !raw.Valid {
		return "", nil
	}
	return strings.TrimSpace(raw.String), nil
}

// scanArtifactRows — query 결과를 []map[string]any로 변환. 컬럼 순서는 호출자가 지정.
// NULL 컬럼은 omit 대신 nil 값으로 둔다 (json marshal 시 null로 직렬화).
func scanArtifactRows(db *sql.DB, query string, columns []string) ([]map[string]any, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		scanTargets := make([]any, len(columns))
		holders := make([]sql.NullString, len(columns))
		for index := range columns {
			scanTargets[index] = &holders[index]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for index, col := range columns {
			if holders[index].Valid {
				row[col] = holders[index].String
			} else {
				row[col] = nil
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
