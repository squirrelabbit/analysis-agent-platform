package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
)

const tokenProjectionVectorDim = 64

type embeddingSidecarRecord struct {
	SourceIndex       int64          `json:"source_index"`
	RowID             string         `json:"row_id"`
	ChunkID           string         `json:"chunk_id"`
	ChunkIndex        int            `json:"chunk_index"`
	CharStart         int            `json:"char_start"`
	CharEnd           int            `json:"char_end"`
	TokenCounts       map[string]int `json:"token_counts"`
	Embedding         []float32      `json:"embedding"`
	EmbeddingDim      int            `json:"embedding_dim"`
	EmbeddingProvider string         `json:"embedding_provider"`
}

type embeddingIndexParquetRecord struct {
	SourceIndex       int64
	RowID             string
	ChunkID           string
	ChunkIndex        int
	CharStart         int
	CharEnd           int
	EmbeddingJSON     string
	EmbeddingDim      int
	EmbeddingProvider string
	TokenCountsJSON   string
}

func loadEmbeddingIndexChunks(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	format := inferArtifactFormat(embeddingRef, "jsonl")
	if format == "parquet" {
		return loadEmbeddingIndexChunksFromParquet(datasetVersionID, embeddingRef, chunkRef, embeddingModel)
	}
	return loadEmbeddingIndexChunksFromJSONL(datasetVersionID, embeddingRef, chunkRef, embeddingModel)
}

func loadEmbeddingIndexChunksFromJSONL(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	handle, err := os.Open(embeddingRef)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 1024), 4*1024*1024)
	records := make([]domain.EmbeddingIndexChunk, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var sidecar embeddingSidecarRecord
		if err := json.Unmarshal([]byte(line), &sidecar); err != nil {
			return nil, err
		}
		if strings.TrimSpace(sidecar.ChunkID) == "" {
			continue
		}
		vector := sidecar.Embedding
		if len(vector) == 0 {
			vector = projectTokenCountsToDenseVector(sidecar.TokenCounts, tokenProjectionVectorDim)
		}
		vectorDim := len(vector)
		if vectorDim == 0 {
			continue
		}
		metadata := map[string]any{
			"char_start": sidecar.CharStart,
			"char_end":   sidecar.CharEnd,
		}
		if provider := strings.TrimSpace(sidecar.EmbeddingProvider); provider != "" {
			metadata["embedding_provider"] = provider
		}
		records = append(records, domain.EmbeddingIndexChunk{
			ChunkID:          strings.TrimSpace(sidecar.ChunkID),
			DatasetVersionID: datasetVersionID,
			RowID:            strings.TrimSpace(sidecar.RowID),
			SourceRowIndex:   sidecar.SourceIndex,
			ChunkIndex:       sidecar.ChunkIndex,
			ChunkRef:         strings.TrimSpace(chunkRef),
			EmbeddingModel:   strings.TrimSpace(embeddingModel),
			VectorDim:        vectorDim,
			Embedding:        vector,
			Metadata:         metadata,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func loadEmbeddingIndexChunksFromParquet(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	embeddingRef = strings.TrimSpace(embeddingRef)
	if embeddingRef == "" {
		return nil, errors.New("embedding parquet ref is required")
	}
	tempHandle, err := os.CreateTemp("", "embedding-index-*.duckdb")
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
			CAST(source_index AS BIGINT) AS source_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(chunk_id, '') AS chunk_id,
			CAST(chunk_index AS INTEGER) AS chunk_index,
			CAST(char_start AS INTEGER) AS char_start,
			CAST(char_end AS INTEGER) AS char_end,
			COALESCE(embedding_json, '') AS embedding_json,
			CAST(COALESCE(embedding_dim, 0) AS INTEGER) AS embedding_dim,
			COALESCE(embedding_provider, '') AS embedding_provider,
			COALESCE(token_counts_json, '{}') AS token_counts_json
		FROM read_parquet('%s')
		ORDER BY source_index, chunk_index, chunk_id`,
		escapeDuckDBLiteral(embeddingRef),
	)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]domain.EmbeddingIndexChunk, 0)
	for rows.Next() {
		var row embeddingIndexParquetRecord
		if err := rows.Scan(
			&row.SourceIndex,
			&row.RowID,
			&row.ChunkID,
			&row.ChunkIndex,
			&row.CharStart,
			&row.CharEnd,
			&row.EmbeddingJSON,
			&row.EmbeddingDim,
			&row.EmbeddingProvider,
			&row.TokenCountsJSON,
		); err != nil {
			return nil, err
		}
		record, ok, err := embeddingIndexChunkFromParquetRow(datasetVersionID, chunkRef, embeddingModel, row)
		if err != nil {
			return nil, err
		}
		if ok {
			records = append(records, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func embeddingIndexChunkFromParquetRow(datasetVersionID string, chunkRef string, embeddingModel string, row embeddingIndexParquetRecord) (domain.EmbeddingIndexChunk, bool, error) {
	if strings.TrimSpace(row.ChunkID) == "" {
		return domain.EmbeddingIndexChunk{}, false, nil
	}
	vector, err := parseFloat32JSONVector(row.EmbeddingJSON)
	if err != nil {
		return domain.EmbeddingIndexChunk{}, false, err
	}
	if len(vector) == 0 {
		tokenCounts, err := parseTokenCountsJSON(row.TokenCountsJSON)
		if err != nil {
			return domain.EmbeddingIndexChunk{}, false, err
		}
		vector = projectTokenCountsToDenseVector(tokenCounts, tokenProjectionVectorDim)
	}
	vectorDim := len(vector)
	if row.EmbeddingDim > 0 && len(vector) > 0 {
		vectorDim = row.EmbeddingDim
	}
	if vectorDim == 0 {
		return domain.EmbeddingIndexChunk{}, false, nil
	}
	metadata := map[string]any{
		"char_start": row.CharStart,
		"char_end":   row.CharEnd,
	}
	if provider := strings.TrimSpace(row.EmbeddingProvider); provider != "" {
		metadata["embedding_provider"] = provider
	}
	return domain.EmbeddingIndexChunk{
		ChunkID:          strings.TrimSpace(row.ChunkID),
		DatasetVersionID: datasetVersionID,
		RowID:            strings.TrimSpace(row.RowID),
		SourceRowIndex:   row.SourceIndex,
		ChunkIndex:       row.ChunkIndex,
		ChunkRef:         strings.TrimSpace(chunkRef),
		EmbeddingModel:   strings.TrimSpace(embeddingModel),
		VectorDim:        vectorDim,
		Embedding:        vector,
		Metadata:         metadata,
	}, true, nil
}

func parseFloat32JSONVector(value string) ([]float32, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return []float32{}, nil
	}
	var raw []float64
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, err
	}
	vector := make([]float32, 0, len(raw))
	for _, item := range raw {
		vector = append(vector, float32(item))
	}
	return vector, nil
}

func parseTokenCountsJSON(value string) (map[string]int, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return map[string]int{}, nil
	}
	tokenCounts := map[string]int{}
	if err := json.Unmarshal([]byte(text), &tokenCounts); err != nil {
		return nil, err
	}
	return tokenCounts, nil
}

func escapeDuckDBLiteral(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func projectTokenCountsToDenseVector(tokenCounts map[string]int, dim int) []float32 {
	if dim <= 0 {
		return []float32{}
	}
	vector := make([]float32, dim)
	for token, count := range tokenCounts {
		token = strings.TrimSpace(token)
		if token == "" || count == 0 {
			continue
		}
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(token))
		sum := hash.Sum64()
		index := int(sum % uint64(dim))
		sign := float32(1)
		if (sum>>63)&1 == 1 {
			sign = -1
		}
		vector[index] += sign * float32(count)
	}
	var norm float64
	for _, value := range vector {
		norm += float64(value * value)
	}
	if norm <= 0 {
		return vector
	}
	scale := float32(1 / math.Sqrt(norm))
	for index, value := range vector {
		vector[index] = value * scale
	}
	return vector
}
