package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
)

type preparePreviewParquetRecord struct {
	SourceRowIndex     int
	RowID              string
	RawText            string
	NormalizedText     string
	PrepareDisposition string
	PrepareReason      string
}

type sentimentPreviewParquetRecord struct {
	SourceRowIndex      int
	RowID               string
	SentimentLabel      string
	SentimentConfidence float64
	SentimentReason     string
}

type clusterMembershipParquetRecord struct {
	ClusterID            string
	ClusterRank          int
	ClusterDocumentCount int
	SourceIndex          int
	RowID                string
	ChunkID              string
	ChunkIndex           int
	Text                 string
	IsSample             bool
}

func loadClusterSummary(summaryRef string, clusterID string) (map[string]any, error) {
	summaryRef = strings.TrimSpace(summaryRef)
	if summaryRef == "" {
		return nil, ErrInvalidArgument{Message: "cluster summary ref is required"}
	}
	content, err := os.ReadFile(summaryRef)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "cluster summary artifact"}
		}
		return nil, err
	}
	var artifact map[string]any
	if err := json.Unmarshal(content, &artifact); err != nil {
		return nil, err
	}
	clusters, ok := artifact["clusters"].([]any)
	if !ok {
		return nil, ErrInvalidArgument{Message: "cluster summary artifact is invalid"}
	}
	for _, raw := range clusters {
		cluster, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if strings.TrimSpace(anyStringValue(cluster["cluster_id"])) != clusterID {
			continue
		}
		return mergeStringAny(nil, cluster), nil
	}
	return nil, ErrNotFound{Resource: "cluster"}
}

func loadPrepareSamplesFromParquet(preparedRef string, limit int, disposition string) ([]domain.DatasetPrepareSample, error) {
	preparedRef = strings.TrimSpace(preparedRef)
	if preparedRef == "" {
		return nil, ErrInvalidArgument{Message: "prepare artifact ref is required"}
	}
	if limit <= 0 {
		return nil, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if _, err := os.Stat(preparedRef); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "prepare artifact"}
		}
		return nil, err
	}
	tempHandle, err := os.CreateTemp("", "prepare-preview-*.duckdb")
	if err != nil {
		return nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	filterClause := ""
	disposition = strings.TrimSpace(disposition)
	if disposition != "" {
		filterClause = fmt.Sprintf("WHERE prepare_disposition = '%s'", escapeDuckDBLiteral(disposition))
	}
	query := fmt.Sprintf(
		`SELECT
			CAST(COALESCE(source_row_index, 0) AS INTEGER) AS source_row_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(raw_text, '') AS raw_text,
			COALESCE(normalized_text, '') AS normalized_text,
			COALESCE(prepare_disposition, '') AS prepare_disposition,
			COALESCE(prepare_reason, '') AS prepare_reason
		FROM read_parquet('%s')
		%s
		ORDER BY source_row_index, row_id
		LIMIT %d`,
		escapeDuckDBLiteral(preparedRef),
		filterClause,
		limit,
	)
	rows, err := db.Query(query)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no files found") {
			return nil, ErrNotFound{Resource: "prepare artifact"}
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetPrepareSample, 0)
	for rows.Next() {
		var row preparePreviewParquetRecord
		if err := rows.Scan(
			&row.SourceRowIndex,
			&row.RowID,
			&row.RawText,
			&row.NormalizedText,
			&row.PrepareDisposition,
			&row.PrepareReason,
		); err != nil {
			return nil, err
		}
		items = append(items, domain.DatasetPrepareSample{
			SourceRowIndex:     row.SourceRowIndex,
			RowID:              strings.TrimSpace(row.RowID),
			RawText:            strings.TrimSpace(row.RawText),
			NormalizedText:     strings.TrimSpace(row.NormalizedText),
			PrepareDisposition: strings.TrimSpace(row.PrepareDisposition),
			PrepareReason:      strings.TrimSpace(row.PrepareReason),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func loadSentimentSamplesFromParquet(sentimentRef string, limit int) ([]domain.DatasetSentimentSample, error) {
	sentimentRef = strings.TrimSpace(sentimentRef)
	if sentimentRef == "" {
		return nil, ErrInvalidArgument{Message: "sentiment artifact ref is required"}
	}
	if limit <= 0 {
		return nil, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if _, err := os.Stat(sentimentRef); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "sentiment artifact"}
		}
		return nil, err
	}
	tempHandle, err := os.CreateTemp("", "sentiment-preview-*.duckdb")
	if err != nil {
		return nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`SELECT
			CAST(COALESCE(source_row_index, 0) AS INTEGER) AS source_row_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(sentiment_label, '') AS sentiment_label,
			CAST(COALESCE(sentiment_confidence, 0) AS DOUBLE) AS sentiment_confidence,
			COALESCE(sentiment_reason, '') AS sentiment_reason
		FROM read_parquet('%s')
		ORDER BY source_row_index, row_id
		LIMIT %d`,
		escapeDuckDBLiteral(sentimentRef),
		limit,
	)
	rows, err := db.Query(query)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no files found") {
			return nil, ErrNotFound{Resource: "sentiment artifact"}
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetSentimentSample, 0)
	for rows.Next() {
		var row sentimentPreviewParquetRecord
		if err := rows.Scan(
			&row.SourceRowIndex,
			&row.RowID,
			&row.SentimentLabel,
			&row.SentimentConfidence,
			&row.SentimentReason,
		); err != nil {
			return nil, err
		}
		items = append(items, domain.DatasetSentimentSample{
			SourceRowIndex:      row.SourceRowIndex,
			RowID:               strings.TrimSpace(row.RowID),
			SentimentLabel:      strings.TrimSpace(row.SentimentLabel),
			SentimentConfidence: row.SentimentConfidence,
			SentimentReason:     strings.TrimSpace(row.SentimentReason),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func exportPrepareCSVFromParquet(preparedRef string) (string, error) {
	preparedRef = strings.TrimSpace(preparedRef)
	if preparedRef == "" {
		return "", ErrInvalidArgument{Message: "prepare artifact ref is required"}
	}
	if _, err := os.Stat(preparedRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "prepare artifact"}
		}
		return "", err
	}

	csvHandle, err := os.CreateTemp("", "prepare-export-*.csv")
	if err != nil {
		return "", err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	tempHandle, err := os.CreateTemp("", "prepare-export-*.duckdb")
	if err != nil {
		return "", err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT * FROM read_parquet('%s')
			ORDER BY source_row_index, row_id
		) TO '%s' (FORMAT CSV, HEADER)`,
		escapeDuckDBLiteral(preparedRef),
		escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

func exportSentimentCSVFromParquet(sentimentRef string) (string, error) {
	sentimentRef = strings.TrimSpace(sentimentRef)
	if sentimentRef == "" {
		return "", ErrInvalidArgument{Message: "sentiment artifact ref is required"}
	}
	if _, err := os.Stat(sentimentRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "sentiment artifact"}
		}
		return "", err
	}

	csvHandle, err := os.CreateTemp("", "sentiment-export-*.csv")
	if err != nil {
		return "", err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	tempHandle, err := os.CreateTemp("", "sentiment-export-*.duckdb")
	if err != nil {
		return "", err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT * FROM read_parquet('%s')
			ORDER BY source_row_index, row_id
		) TO '%s' (FORMAT CSV, HEADER)`,
		escapeDuckDBLiteral(sentimentRef),
		escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

func loadClusterMembersFromParquet(clusterMembershipRef string, clusterID string, limit int, samplesOnly bool) ([]domain.ClusterMember, int, int, error) {
	clusterMembershipRef = strings.TrimSpace(clusterMembershipRef)
	if clusterMembershipRef == "" {
		return nil, 0, 0, ErrInvalidArgument{Message: "cluster membership ref is required"}
	}
	tempHandle, err := os.CreateTemp("", "cluster-members-*.duckdb")
	if err != nil {
		return nil, 0, 0, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, 0, 0, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, 0, 0, err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, 0, 0, err
	}
	defer db.Close()

	countQuery := fmt.Sprintf(
		`SELECT
			CAST(COUNT(*) AS BIGINT) AS total_count,
			CAST(COALESCE(SUM(CASE WHEN is_sample THEN 1 ELSE 0 END), 0) AS BIGINT) AS sample_count
		FROM read_parquet('%s')
		WHERE cluster_id = '%s'`,
		escapeDuckDBLiteral(clusterMembershipRef),
		escapeDuckDBLiteral(clusterID),
	)
	var totalCount int
	var sampleCount int
	if err := db.QueryRow(countQuery).Scan(&totalCount, &sampleCount); err != nil {
		return nil, 0, 0, err
	}

	filterClause := ""
	if samplesOnly {
		filterClause = " AND is_sample = TRUE"
	}
	rowsQuery := fmt.Sprintf(
		`SELECT
			COALESCE(cluster_id, '') AS cluster_id,
			CAST(COALESCE(cluster_rank, 0) AS INTEGER) AS cluster_rank,
			CAST(COALESCE(cluster_document_count, 0) AS INTEGER) AS cluster_document_count,
			CAST(COALESCE(source_index, 0) AS INTEGER) AS source_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(chunk_id, '') AS chunk_id,
			CAST(COALESCE(chunk_index, 0) AS INTEGER) AS chunk_index,
			COALESCE(text, '') AS text,
			CAST(COALESCE(is_sample, FALSE) AS BOOLEAN) AS is_sample
		FROM read_parquet('%s')
		WHERE cluster_id = '%s'%s
		ORDER BY is_sample DESC, source_index, chunk_index, chunk_id
		LIMIT %d`,
		escapeDuckDBLiteral(clusterMembershipRef),
		escapeDuckDBLiteral(clusterID),
		filterClause,
		limit,
	)
	rows, err := db.Query(rowsQuery)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	items := make([]domain.ClusterMember, 0)
	for rows.Next() {
		var row clusterMembershipParquetRecord
		if err := rows.Scan(
			&row.ClusterID,
			&row.ClusterRank,
			&row.ClusterDocumentCount,
			&row.SourceIndex,
			&row.RowID,
			&row.ChunkID,
			&row.ChunkIndex,
			&row.Text,
			&row.IsSample,
		); err != nil {
			return nil, 0, 0, err
		}
		items = append(items, domain.ClusterMember{
			ClusterID:            strings.TrimSpace(row.ClusterID),
			ClusterRank:          row.ClusterRank,
			ClusterDocumentCount: row.ClusterDocumentCount,
			SourceIndex:          row.SourceIndex,
			RowID:                strings.TrimSpace(row.RowID),
			ChunkID:              strings.TrimSpace(row.ChunkID),
			ChunkIndex:           row.ChunkIndex,
			Text:                 strings.TrimSpace(row.Text),
			IsSample:             row.IsSample,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, err
	}
	return items, totalCount, sampleCount, nil
}
