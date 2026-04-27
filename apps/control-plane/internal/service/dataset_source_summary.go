package service

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
)

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
		return fmt.Sprintf("read_csv_auto('%s', HEADER=TRUE, SAMPLE_SIZE=-1)", escapedPath), nil
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
