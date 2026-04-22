package store

import (
	"path/filepath"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

func deriveDatasetVersionArtifacts(version domain.DatasetVersion, now time.Time) []domain.DatasetVersionArtifact {
	version = normalizeDatasetVersionCleanFields(version)
	items := make([]domain.DatasetVersionArtifact, 0, 10)

	sourceMetadata := map[string]any{
		"data_type": version.DataType,
	}
	if version.RecordCount != nil {
		sourceMetadata["record_count"] = *version.RecordCount
	}
	items = append(items, datasetVersionArtifact(version, "source", "source", "ready", strings.TrimSpace(version.StorageURI), inferDatasetArtifactFormat(version.StorageURI, ""), "", "", nil, sourceMetadata, now))

	cleanStatus := strings.TrimSpace(version.CleanStatus)
	cleanURI := firstNonEmpty(derefString(version.CleanURI), metadataString(version.Metadata, "clean_uri"), metadataString(version.Metadata, "cleaned_ref"))
	if shouldIncludeDatasetArtifact(cleanStatus, cleanURI) {
		cleanFormat := firstNonEmpty(metadataString(version.Metadata, "clean_format"), metadataString(version.Metadata, "cleaned_format"), inferDatasetArtifactFormat(cleanURI, "parquet"))
		items = append(items, datasetVersionArtifact(
			version,
			"clean",
			"clean",
			cleanStatus,
			cleanURI,
			cleanFormat,
			"",
			"",
			artifactSummary(version.Metadata, "clean_summary"),
			artifactMetadata(version.Metadata, []string{
				"clean_progress_ref",
				"cleaned_text_column",
				"raw_text_column",
				"raw_text_columns",
				"text_joiner",
				"clean_preprocess_options",
				"clean_notes",
				"clean_error",
				"row_id_column",
			}),
			now,
		))
	}
	if progressRef := metadataString(version.Metadata, "clean_progress_ref"); progressRef != "" {
		items = append(items, datasetVersionArtifact(version, "clean_progress", "clean", cleanStatus, progressRef, inferDatasetArtifactFormat(progressRef, "json"), "", "", nil, nil, now))
	}

	prepareStatus := strings.TrimSpace(version.PrepareStatus)
	prepareURI := firstNonEmpty(derefString(version.PrepareURI), metadataString(version.Metadata, "prepare_uri"), metadataString(version.Metadata, "prepared_ref"))
	if shouldIncludeDatasetArtifact(prepareStatus, prepareURI) {
		prepareFormat := firstNonEmpty(metadataString(version.Metadata, "prepare_format"), metadataString(version.Metadata, "prepared_format"), inferDatasetArtifactFormat(prepareURI, "parquet"))
		items = append(items, datasetVersionArtifact(
			version,
			"prepare",
			"prepare",
			prepareStatus,
			prepareURI,
			prepareFormat,
			derefString(version.PrepareModel),
			derefString(version.PreparePromptVer),
			artifactSummary(version.Metadata, "prepare_summary"),
			artifactMetadata(version.Metadata, []string{
				"prepare_progress_ref",
				"prepared_text_column",
				"text_column",
				"text_columns",
				"text_joiner",
				"raw_text_column",
				"raw_text_columns",
				"prepare_notes",
				"prepare_error",
				"prepare_max_rows",
				"prepare_usage",
				"row_id_column",
				"storage_contract_version",
			}),
			now,
		))
	}
	if progressRef := metadataString(version.Metadata, "prepare_progress_ref"); progressRef != "" {
		items = append(items, datasetVersionArtifact(version, "prepare_progress", "prepare", prepareStatus, progressRef, inferDatasetArtifactFormat(progressRef, "json"), "", "", nil, nil, now))
	}

	sentimentStatus := strings.TrimSpace(version.SentimentStatus)
	sentimentURI := firstNonEmpty(derefString(version.SentimentURI), metadataString(version.Metadata, "sentiment_uri"), metadataString(version.Metadata, "sentiment_ref"))
	if shouldIncludeDatasetArtifact(sentimentStatus, sentimentURI) {
		sentimentFormat := firstNonEmpty(metadataString(version.Metadata, "sentiment_format"), inferDatasetArtifactFormat(sentimentURI, "parquet"))
		items = append(items, datasetVersionArtifact(
			version,
			"sentiment",
			"sentiment",
			sentimentStatus,
			sentimentURI,
			sentimentFormat,
			derefString(version.SentimentModel),
			derefString(version.SentimentPromptVer),
			artifactSummary(version.Metadata, "sentiment_summary"),
			artifactMetadata(version.Metadata, []string{
				"sentiment_text_column",
				"sentiment_text_columns",
				"sentiment_text_joiner",
				"sentiment_label_column",
				"sentiment_confidence_column",
				"sentiment_reason_column",
				"sentiment_notes",
				"sentiment_error",
				"sentiment_usage",
				"row_id_column",
				"storage_contract_version",
			}),
			now,
		))
	}

	embeddingStatus := strings.TrimSpace(version.EmbeddingStatus)
	embeddingURI := firstNonEmpty(derefString(version.EmbeddingURI), metadataString(version.Metadata, "embedding_uri"), metadataString(version.Metadata, "embedding_ref"))
	if shouldIncludeDatasetArtifact(embeddingStatus, embeddingURI) {
		embeddingFormat := firstNonEmpty(metadataString(version.Metadata, "embedding_format"), inferDatasetArtifactFormat(embeddingURI, "jsonl"))
		items = append(items, datasetVersionArtifact(
			version,
			"embedding",
			"embedding",
			embeddingStatus,
			embeddingURI,
			embeddingFormat,
			derefString(version.EmbeddingModel),
			"",
			artifactSummary(version.Metadata, "embedding_summary"),
			artifactMetadata(version.Metadata, []string{
				"embedding_dataset_name",
				"embedding_notes",
				"embedding_debug_export_jsonl",
				"embedding_provider",
				"embedding_representation",
				"embedding_usage",
				"embedding_error",
				"embedding_vector_dim",
				"text_column",
				"storage_contract_version",
			}),
			now,
		))
	}
	if indexRef := metadataString(version.Metadata, "embedding_index_source_ref"); indexRef != "" {
		indexFormat := firstNonEmpty(metadataString(version.Metadata, "embedding_index_source_format"), inferDatasetArtifactFormat(indexRef, "parquet"))
		items = append(items, datasetVersionArtifact(version, "embedding_index", "embedding", readyStatusForArtifact(embeddingStatus, indexRef), indexRef, indexFormat, derefString(version.EmbeddingModel), "", nil, artifactMetadata(version.Metadata, []string{"document_count", "chunk_count", "source_row_count", "embedding_vector_dim"}), now))
	}
	if chunkRef := metadataString(version.Metadata, "chunk_ref"); chunkRef != "" {
		chunkFormat := firstNonEmpty(metadataString(version.Metadata, "chunk_format"), inferDatasetArtifactFormat(chunkRef, "parquet"))
		items = append(items, datasetVersionArtifact(version, "embedding_chunks", "embedding", readyStatusForArtifact(embeddingStatus, chunkRef), chunkRef, chunkFormat, derefString(version.EmbeddingModel), "", nil, artifactMetadata(version.Metadata, []string{"chunk_id_column", "chunk_index_column", "chunk_text_column", "chunking_strategy", "row_id_column", "chunk_count"}), now))
	}

	clusterStatus := metadataString(version.Metadata, "cluster_status")
	clusterSummaryRef := firstNonEmpty(metadataString(version.Metadata, "cluster_summary_ref"), metadataString(version.Metadata, "cluster_ref"))
	if shouldIncludeDatasetArtifact(clusterStatus, clusterSummaryRef) {
		clusterSummaryFormat := firstNonEmpty(metadataString(version.Metadata, "cluster_summary_format"), metadataString(version.Metadata, "cluster_format"), inferDatasetArtifactFormat(clusterSummaryRef, "json"))
		items = append(items, datasetVersionArtifact(
			version,
			"cluster_summary",
			"cluster",
			readyStatusForArtifact(clusterStatus, clusterSummaryRef),
			clusterSummaryRef,
			clusterSummaryFormat,
			"",
			"",
			artifactSummary(version.Metadata, "cluster_summary"),
			artifactMetadata(version.Metadata, []string{
				"cluster_ref",
				"cluster_membership_ref",
				"cluster_membership_format",
				"cluster_algorithm",
				"cluster_source_embedding_ref",
				"cluster_similarity_threshold",
				"cluster_top_n",
				"cluster_sample_n",
				"cluster_params_hash",
				"cluster_notes",
				"cluster_error",
				"cluster_stale_reason",
			}),
			now,
		))
	}
	if membershipRef := metadataString(version.Metadata, "cluster_membership_ref"); membershipRef != "" {
		membershipFormat := firstNonEmpty(metadataString(version.Metadata, "cluster_membership_format"), inferDatasetArtifactFormat(membershipRef, "parquet"))
		items = append(items, datasetVersionArtifact(version, "cluster_membership", "cluster", readyStatusForArtifact(clusterStatus, membershipRef), membershipRef, membershipFormat, "", "", nil, artifactMetadata(version.Metadata, []string{"cluster_summary_ref", "cluster_algorithm", "cluster_params_hash"}), now))
	}

	sort.Slice(items, func(i, j int) bool {
		if artifactStageOrder(items[i].Stage) == artifactStageOrder(items[j].Stage) {
			return items[i].ArtifactType < items[j].ArtifactType
		}
		return artifactStageOrder(items[i].Stage) < artifactStageOrder(items[j].Stage)
	})
	return items
}

func datasetVersionArtifact(version domain.DatasetVersion, artifactType, stage, status, uri, format, model, promptVersion string, summary, metadata map[string]any, now time.Time) domain.DatasetVersionArtifact {
	status = readyStatusForArtifact(status, uri)
	return domain.DatasetVersionArtifact{
		ArtifactID:       datasetVersionArtifactID(version.DatasetVersionID, artifactType),
		ProjectID:        version.ProjectID,
		DatasetID:        version.DatasetID,
		DatasetVersionID: version.DatasetVersionID,
		ArtifactType:     artifactType,
		Stage:            stage,
		Status:           status,
		URI:              strings.TrimSpace(uri),
		Format:           strings.TrimSpace(format),
		Model:            strings.TrimSpace(model),
		PromptVersion:    strings.TrimSpace(promptVersion),
		Summary:          cloneAnyMap(summary),
		Metadata:         cloneAnyMap(metadata),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func datasetVersionArtifactID(datasetVersionID, artifactType string) string {
	return strings.TrimSpace(datasetVersionID) + ":" + strings.TrimSpace(artifactType)
}

func shouldIncludeDatasetArtifact(status, uri string) bool {
	status = strings.TrimSpace(status)
	if strings.TrimSpace(uri) != "" {
		return true
	}
	return status != "" && status != "not_requested" && status != "not_applicable"
}

func readyStatusForArtifact(status, uri string) string {
	status = strings.TrimSpace(status)
	if status == "" && strings.TrimSpace(uri) != "" {
		return "ready"
	}
	if status == "" {
		return "not_requested"
	}
	return status
}

func artifactSummary(metadata map[string]any, key string) map[string]any {
	value, ok := metadata[key].(map[string]any)
	if !ok || len(value) == 0 {
		return nil
	}
	return cloneAnyMap(value)
}

func artifactMetadata(metadata map[string]any, keys []string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	output := make(map[string]any)
	for _, key := range keys {
		value, ok := metadata[key]
		if !ok || value == nil {
			continue
		}
		output[key] = cloneAnyValue(value)
	}
	if len(output) == 0 {
		return nil
	}
	return output
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func inferDatasetArtifactFormat(uri, fallback string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return strings.TrimSpace(fallback)
	}
	extension := strings.TrimPrefix(strings.ToLower(filepath.Ext(uri)), ".")
	if extension == "" {
		return strings.TrimSpace(fallback)
	}
	if extension == "jsonl" || extension == "json" || extension == "csv" || extension == "parquet" {
		return extension
	}
	return strings.TrimSpace(fallback)
}

func artifactStageOrder(stage string) int {
	switch strings.TrimSpace(stage) {
	case "source":
		return 10
	case "clean":
		return 20
	case "prepare":
		return 30
	case "sentiment":
		return 40
	case "embedding":
		return 50
	case "cluster":
		return 60
	default:
		return 100
	}
}
