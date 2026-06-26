package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xuri/excelize/v2"
)

// sourceSummaryMetaKey — 버전 metadata에 캐시되는 source 프리뷰(컬럼/행수/샘플) 키.
// source 파일은 버전당 immutable이라 생성/업로드 시 1회 계산해 캐시하면 매 조회마다
// CSV를 전체 스캔(COUNT/DESCRIBE)하던 비용을 없앤다. (silverone 2026-06-26)
const sourceSummaryMetaKey = "source_summary"

// buildSourceSummaryCache — 생성 시점에 source 프리뷰를 계산해 metadata 캐시용 map으로
// 돌려준다. ready가 아니면(파일 없음/미지원) nil → 캐시 생략(조회 시 재계산 fallback).
func buildSourceSummaryCache(storageURI string) map[string]any {
	summary := loadDatasetSourceSummary(storageURI, defaultDatasetSourceSummarySampleLimit)
	if summary == nil || summary.Status != "ready" {
		return nil
	}
	b, err := json.Marshal(summary)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

// cachedSourceSummary — metadata에 캐시된 source 프리뷰가 있으면 복원(없으면 nil).
func cachedSourceSummary(metadata map[string]any) *domain.DatasetSourceSummary {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata[sourceSummaryMetaKey]
	if !ok || raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var summary domain.DatasetSourceSummary
	if err := json.Unmarshal(b, &summary); err != nil {
		return nil
	}
	return &summary
}

func loadDatasetSourceSummary(storageURI string, sampleLimit int) *domain.DatasetSourceSummary {
	storageURI = strings.TrimSpace(storageURI)
	summary := &domain.DatasetSourceSummary{
		Available:   false,
		Status:      "unavailable",
		SampleLimit: sampleLimit,
	}
	if storageURI == "" {
		summary.Status = "missing"
		summary.ErrorMessage = "storage_uri is required"
		return summary
	}

	format := inferDatasetSourceFormat(storageURI)
	if format == "" {
		summary.Status = "unsupported"
		summary.ErrorMessage = "unsupported source format"
		return summary
	}
	summary.Format = format

	info, err := os.Stat(storageURI)
	if err != nil {
		if os.IsNotExist(err) {
			summary.Status = "missing"
			summary.ErrorMessage = "source file not found"
			return summary
		}
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	if info.IsDir() {
		summary.Status = "error"
		summary.ErrorMessage = "source path must be a file"
		return summary
	}

	// 엑셀(.xlsx/.xlsm)은 DuckDB로 못 읽으므로 excelize로 직접 읽어 컬럼/행수/샘플을 채운다.
	// (clean 폼의 text_columns 선택이 source 컬럼 목록에 의존하므로 프리뷰가 필수.)
	// Python worker(runtime.common._read_excel_rows)와 동일하게 첫 시트·헤더·빈행 skip·문자열 값.
	if format == "xlsx" {
		columns, rowCount, sampleRows, xerr := readXlsxSourceSummary(storageURI, sampleLimit)
		if xerr != nil {
			summary.Status = "error"
			summary.ErrorMessage = xerr.Error()
			return summary
		}
		summary.Available = true
		summary.Status = "ready"
		summary.RowCount = &rowCount
		summary.ColumnCount = len(columns)
		summary.Columns = columns
		summary.SampleRows = sampleRows
		return summary
	}

	relation, err := datasetSourceDuckDBRelation(storageURI, format)
	if err != nil {
		summary.Status = "unsupported"
		summary.ErrorMessage = err.Error()
		return summary
	}
	db, cleanup, err := openTemporaryDuckDB("dataset-source-summary-*.duckdb")
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	defer cleanup()

	columns, err := loadDatasetSourceColumns(db, relation)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	rowCount, err := loadDatasetSourceRowCount(db, relation)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	sampleRows, err := loadDatasetSourceSampleRows(db, relation, sampleLimit)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}

	summary.Available = true
	summary.Status = "ready"
	summary.RowCount = &rowCount
	summary.ColumnCount = len(columns)
	summary.Columns = columns
	summary.SampleRows = sampleRows
	summary.ErrorMessage = ""
	return summary
}

// readXlsxSourceSummary는 .xlsx/.xlsm 첫 시트를 읽어 source 프리뷰(컬럼/행수/샘플)를 만든다.
// Python worker(runtime.common._read_excel_rows)와 정합: 첫 행=헤더(빈 칸 무시), 완전히 빈 행 skip,
// 값은 문자열. clean의 text_columns 선택이 이 컬럼 목록을 쓴다.
func readXlsxSourceSummary(path string, sampleLimit int) ([]domain.DatasetSourceColumnSummary, int, []map[string]any, error) {
	file, err := excelize.OpenFile(path)
	if err != nil {
		return nil, 0, nil, err
	}
	defer file.Close()

	sheets := file.GetSheetList()
	if len(sheets) == 0 {
		return []domain.DatasetSourceColumnSummary{}, 0, []map[string]any{}, nil
	}
	rows, err := file.GetRows(sheets[0])
	if err != nil {
		return nil, 0, nil, err
	}
	if len(rows) == 0 {
		return []domain.DatasetSourceColumnSummary{}, 0, []map[string]any{}, nil
	}

	header := rows[0]
	columnIndexes := make([]int, 0, len(header))
	columns := make([]domain.DatasetSourceColumnSummary, 0, len(header))
	for index, raw := range header {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		columnIndexes = append(columnIndexes, index)
		columns = append(columns, domain.DatasetSourceColumnSummary{Name: name, Type: "VARCHAR"})
	}

	rowCount := 0
	sampleRows := make([]map[string]any, 0)
	for _, cells := range rows[1:] {
		allEmpty := true
		for _, cell := range cells {
			if strings.TrimSpace(cell) != "" {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			continue
		}
		rowCount++
		if sampleLimit > 0 && len(sampleRows) < sampleLimit {
			record := make(map[string]any, len(columns))
			for position, index := range columnIndexes {
				value := ""
				if index < len(cells) {
					value = cells[index]
				}
				record[columns[position].Name] = value
			}
			sampleRows = append(sampleRows, record)
		}
	}
	return columns, rowCount, sampleRows, nil
}

func inferDatasetSourceFormat(path string) string {
	normalized := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(normalized, ".parquet"):
		return "parquet"
	case strings.HasSuffix(normalized, ".csv"):
		return "csv"
	case strings.HasSuffix(normalized, ".tsv"):
		return "tsv"
	case strings.HasSuffix(normalized, ".jsonl"), strings.HasSuffix(normalized, ".ndjson"):
		return "jsonl"
	case strings.HasSuffix(normalized, ".xlsx"), strings.HasSuffix(normalized, ".xlsm"):
		return "xlsx"
	default:
		return ""
	}
}

func datasetSourceDuckDBRelation(path string, format string) (string, error) {
	escapedPath := escapeDuckDBLiteral(path)
	switch format {
	case "parquet":
		return fmt.Sprintf("read_parquet('%s')", escapedPath), nil
	case "csv", "tsv":
		// SAMPLE_SIZE=-1(전체 스캔 타입추론)은 프리뷰(컬럼명/행수/샘플)엔 불필요하고 큰
		// 파일에서 매우 느리다(특히 HDD). 기본 샘플링으로 DESCRIBE/샘플을 가볍게.
		// 행수는 COUNT(*)가 어차피 1패스 스캔이라 정확. (silverone 2026-06-26)
		return fmt.Sprintf("read_csv_auto('%s', HEADER=TRUE)", escapedPath), nil
	case "jsonl":
		return fmt.Sprintf("read_json_auto('%s')", escapedPath), nil
	default:
		return "", ErrInvalidArgument{Message: "unsupported source format"}
	}
}

func openTemporaryDuckDB(pattern string) (*sql.DB, func(), error) {
	tempHandle, err := os.CreateTemp("", pattern)
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

func loadDatasetSourceColumns(db *sql.DB, relation string) ([]domain.DatasetSourceColumnSummary, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM %s", relation))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]domain.DatasetSourceColumnSummary, 0)
	for rows.Next() {
		var name sql.NullString
		var columnType sql.NullString
		var nullable sql.NullString
		var key sql.NullString
		var defaultValue sql.NullString
		var extra sql.NullString
		if err := rows.Scan(&name, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
			return nil, err
		}
		columnName := strings.TrimSpace(name.String)
		if columnName == "" {
			continue
		}
		columns = append(columns, domain.DatasetSourceColumnSummary{
			Name: columnName,
			Type: strings.TrimSpace(columnType.String),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func loadDatasetSourceRowCount(db *sql.DB, relation string) (int, error) {
	var rowCount int64
	query := fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s", relation)
	if err := db.QueryRow(query).Scan(&rowCount); err != nil {
		return 0, err
	}
	return int(rowCount), nil
}

func loadDatasetSourceSampleRows(db *sql.DB, relation string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d", relation, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columnNames))
		destinations := make([]any, len(columnNames))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		item := make(map[string]any, len(columnNames))
		for index, column := range columnNames {
			item[column] = sourceSummaryJSONValue(values[index])
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func sourceSummaryJSONValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return typed
	}
}
