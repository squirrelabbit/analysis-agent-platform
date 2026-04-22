package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

func (s *DatasetService) BuildEmbeddings(projectID, datasetID, datasetVersionID string, input domain.DatasetEmbeddingBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embeddings require unstructured or mixed dataset version"}
	}
	if requiresPrepare(version) && !isPrepareReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare must be ready before embeddings"}
	}

	force := input.Force != nil && *input.Force
	if embeddingBuildReady(version) && !force {
		return version, nil
	}

	source := domain.ResolveDatasetSource(version)
	textColumn := source.TextColumn
	if input.TextColumn != nil && strings.TrimSpace(*input.TextColumn) != "" {
		requestedTextColumn := strings.TrimSpace(*input.TextColumn)
		if source.Stage == domain.DatasetSourceStageRaw || !domain.DatasetSourceIsRawTextColumn(version, requestedTextColumn) {
			textColumn = requestedTextColumn
		}
	}
	datasetName := datasetSourceForUnstructured(version)

	version.EmbeddingStatus = "building"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["text_column"] = textColumn
	version.Metadata["embedding_dataset_name"] = datasetName
	invalidateClusterArtifacts(&version, "embedding output changed")
	indexOutputPath := s.deriveEmbeddingIndexSourceURI(version)
	debugExportJSONL := input.DebugExportJSONL != nil && *input.DebugExportJSONL
	outputPath := ""
	if debugExportJSONL {
		outputPath = s.deriveEmbeddingURI(version)
		version.EmbeddingURI = &outputPath
		if err := ensureParentDir(outputPath); err != nil {
			return domain.DatasetVersion{}, err
		}
	} else {
		version.EmbeddingURI = nil
	}
	if err := ensureParentDir(indexOutputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if input.EmbeddingModel != nil {
		requestedModel := strings.TrimSpace(*input.EmbeddingModel)
		if requestedModel == "" {
			version.EmbeddingModel = nil
		} else {
			version.EmbeddingModel = &requestedModel
		}
	}
	if version.EmbeddingModel == nil {
		if version.Profile != nil && version.Profile.EmbeddingModel != nil && strings.TrimSpace(*version.Profile.EmbeddingModel) != "" {
			model := strings.TrimSpace(*version.Profile.EmbeddingModel)
			version.EmbeddingModel = &model
		} else {
			model := DefaultEmbeddingModel
			version.EmbeddingModel = &model
		}
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textColumn,
		"index_output_path":  indexOutputPath,
		"embedding_model":    derefString(version.EmbeddingModel),
	}
	if debugExportJSONL {
		payload["output_path"] = outputPath
	}
	response, err := s.runWorkerTask(context.Background(), "/tasks/embedding", payload)
	if err != nil {
		version.EmbeddingStatus = "failed"
		version.Metadata["embedding_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.EmbeddingStatus = "ready"
	version.ReadyAt = &now
	embeddingRef := artifactString(response.Artifact, "embedding_ref")
	if embeddingRef == "" {
		embeddingRef = artifactString(response.Artifact, "embedding_uri")
	}
	embeddingFormat := artifactString(response.Artifact, "embedding_format")
	if embeddingFormat == "" && embeddingRef != "" {
		embeddingFormat = inferArtifactFormat(embeddingRef, "")
	}
	embeddingIndexSourceRef := artifactString(response.Artifact, "embedding_index_source_ref")
	if embeddingIndexSourceRef == "" {
		embeddingIndexSourceRef = indexOutputPath
	}
	embeddingIndexSourceFormat := artifactString(response.Artifact, "embedding_index_source_format")
	if embeddingIndexSourceFormat == "" && embeddingIndexSourceRef != "" {
		embeddingIndexSourceFormat = inferArtifactFormat(embeddingIndexSourceRef, "parquet")
	}
	embeddingMetadata := map[string]any{
		"text_column":                  textColumn,
		"embedding_notes":              response.Notes,
		"embedding_debug_export_jsonl": debugExportJSONL,
	}
	if embeddingRef != "" {
		embeddingMetadata["embedding_ref"] = embeddingRef
	} else {
		delete(version.Metadata, "embedding_ref")
	}
	if embeddingFormat != "" {
		embeddingMetadata["embedding_format"] = embeddingFormat
	} else {
		delete(version.Metadata, "embedding_format")
	}
	if embeddingIndexSourceRef != "" {
		embeddingMetadata["embedding_index_source_ref"] = embeddingIndexSourceRef
	}
	if embeddingIndexSourceFormat != "" {
		embeddingMetadata["embedding_index_source_format"] = embeddingIndexSourceFormat
	}
	if chunkRef := artifactString(response.Artifact, "chunk_ref"); chunkRef != "" {
		embeddingMetadata["chunk_ref"] = chunkRef
	}
	if chunkFormat := artifactString(response.Artifact, "chunk_format"); chunkFormat != "" {
		embeddingMetadata["chunk_format"] = chunkFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		embeddingMetadata["row_id_column"] = rowIDColumn
	}
	if chunkIDColumn := artifactString(response.Artifact, "chunk_id_column"); chunkIDColumn != "" {
		embeddingMetadata["chunk_id_column"] = chunkIDColumn
	}
	if chunkIndexColumn := artifactString(response.Artifact, "chunk_index_column"); chunkIndexColumn != "" {
		embeddingMetadata["chunk_index_column"] = chunkIndexColumn
	}
	if chunkTextColumn := artifactString(response.Artifact, "chunk_text_column"); chunkTextColumn != "" {
		embeddingMetadata["chunk_text_column"] = chunkTextColumn
	}
	if chunkingStrategy := artifactString(response.Artifact, "chunking_strategy"); chunkingStrategy != "" {
		embeddingMetadata["chunking_strategy"] = chunkingStrategy
	}
	if embeddingProvider := artifactString(response.Artifact, "embedding_provider"); embeddingProvider != "" {
		embeddingMetadata["embedding_provider"] = embeddingProvider
	}
	if embeddingRepresentation := artifactString(response.Artifact, "embedding_representation"); embeddingRepresentation != "" {
		embeddingMetadata["embedding_representation"] = embeddingRepresentation
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		embeddingMetadata["storage_contract_version"] = contractVersion
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		embeddingMetadata["embedding_usage"] = usage
	}
	version.Metadata = mergeStringAny(version.Metadata, embeddingMetadata)
	if value, ok := response.Artifact["document_count"]; ok {
		version.Metadata["document_count"] = value
	}
	if value, ok := response.Artifact["chunk_count"]; ok {
		version.Metadata["chunk_count"] = value
	}
	if value, ok := response.Artifact["source_row_count"]; ok {
		version.Metadata["source_row_count"] = value
	}
	if value, ok := response.Artifact["embedding_vector_dim"]; ok {
		version.Metadata["embedding_vector_dim"] = value
	}
	if value, ok := response.Artifact["embedding_uri"].(string); ok && strings.TrimSpace(value) != "" {
		version.EmbeddingURI = &value
	} else {
		version.EmbeddingURI = nil
	}
	if value, ok := response.Artifact["embedding_model"].(string); ok && strings.TrimSpace(value) != "" {
		version.EmbeddingModel = &value
	}
	indexEmbeddingRef := resolveReadableArtifactRef(embeddingIndexSourceRef, embeddingRef, outputPath)
	if err := s.syncEmbeddingIndex(version, indexEmbeddingRef, artifactString(response.Artifact, "chunk_ref")); err != nil {
		version.EmbeddingStatus = "failed"
		version.Metadata["embedding_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}
	delete(version.Metadata, "embedding_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	enrichDatasetVersionView(&version)
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}
