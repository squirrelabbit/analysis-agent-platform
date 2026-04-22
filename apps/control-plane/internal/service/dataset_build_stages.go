package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
)

func (s *DatasetService) BuildClean(projectID, datasetID, datasetVersionID string, input domain.DatasetCleanRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset clean requires unstructured or mixed dataset version"}
	}

	force := input.Force != nil && *input.Force
	if isCleanReady(version) && !force {
		return version, nil
	}

	textSelection := resolveDatasetBuildTextSelection(version.Metadata, input.TextColumns)
	if len(textSelection.Columns) == 0 {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "text_columns is required for dataset clean"}
	}
	textColumn := textSelection.TextColumn
	textColumns := textSelection.Columns
	textJoiner := textSelection.Joiner
	outputPath := s.deriveCleanURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}
	progressPath := outputPath + ".progress.json"
	preprocessOptions := resolveCleanPreprocessOptions(version.Metadata, input.PreprocessOptions)

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["clean_status"] = "cleaning"
	version.Metadata["cleaned_ref"] = outputPath
	version.Metadata["cleaned_format"] = inferArtifactFormat(outputPath, "parquet")
	version.Metadata["clean_progress_ref"] = progressPath
	version.Metadata["raw_text_column"] = textColumn
	version.Metadata["raw_text_columns"] = append([]string(nil), textColumns...)
	version.Metadata["text_joiner"] = textJoiner
	version.Metadata["clean_preprocess_options"] = cloneStringBoolMap(preprocessOptions)
	invalidateDownstreamArtifactsForClean(&version, "clean output changed")
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       version.StorageURI,
		"text_column":        textColumn,
		"text_columns":       append([]string(nil), textColumns...),
		"text_joiner":        textJoiner,
		"output_path":        outputPath,
		"progress_path":      progressPath,
		"preprocess_options": cloneStringBoolMap(preprocessOptions),
	}
	if version.Profile != nil && len(version.Profile.RegexRuleNames) > 0 {
		payload["regex_rule_names"] = append([]string(nil), version.Profile.RegexRuleNames...)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_clean", payload)
	if err != nil {
		version.Metadata["clean_status"] = "failed"
		version.Metadata["clean_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	cleanedRef := artifactString(response.Artifact, "cleaned_ref")
	if cleanedRef == "" {
		cleanedRef = artifactString(response.Artifact, "clean_uri")
	}
	if cleanedRef == "" {
		cleanedRef = outputPath
	}
	cleanFormat := artifactString(response.Artifact, "clean_format")
	if cleanFormat == "" {
		cleanFormat = inferArtifactFormat(cleanedRef, "parquet")
	}
	cleanMetadata := map[string]any{
		"clean_status":        "ready",
		"cleaned_ref":         cleanedRef,
		"cleaned_format":      cleanFormat,
		"cleaned_at":          now,
		"clean_notes":         response.Notes,
		"raw_text_column":     textColumn,
		"raw_text_columns":    append([]string(nil), textColumns...),
		"text_joiner":         textJoiner,
		"cleaned_text_column": artifactString(response.Artifact, "cleaned_text_column"),
	}
	if cleanMetadata["cleaned_text_column"] == "" {
		cleanMetadata["cleaned_text_column"] = "cleaned_text"
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		cleanMetadata["row_id_column"] = rowIDColumn
	}
	if progressRef := artifactString(response.Artifact, "progress_ref"); progressRef != "" {
		cleanMetadata["clean_progress_ref"] = progressRef
	}
	if artifactPreprocessOptions := artifactBoolMap(response.Artifact, "preprocess_options"); len(artifactPreprocessOptions) > 0 {
		cleanMetadata["clean_preprocess_options"] = artifactPreprocessOptions
	}
	for _, key := range []string{
		"source_input_char_count",
		"cleaned_input_char_count",
		"clean_reduced_char_count",
	} {
		if value, ok := artifactInt(response.Artifact, key); ok {
			cleanMetadata[key] = value
		}
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		cleanMetadata["clean_summary"] = summary
		if value, ok := summary["output_row_count"]; ok {
			if intValue, ok := anyToInt(value); ok {
				version.RecordCount = &intValue
			}
		}
	}
	version.Metadata = mergeStringAny(version.Metadata, cleanMetadata)
	delete(version.Metadata, "clean_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	enrichDatasetVersionView(&version)
	return version, nil
}

func (s *DatasetService) BuildPrepare(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare requires unstructured or mixed dataset version"}
	}
	if status := cleanStatus(version); status == "queued" || status == "cleaning" || status == "failed" || status == "stale" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset clean must be ready before prepare"}
	}

	force := input.Force != nil && *input.Force
	if version.PrepareStatus == "ready" && version.PrepareURI != nil && !force {
		return version, nil
	}

	datasetName, defaultPrepareColumns := datasetSourceForPrepare(version)
	requestedColumns := input.TextColumns
	if len(normalizeStringList(requestedColumns)) == 0 && len(defaultPrepareColumns) > 0 {
		requestedColumns = defaultPrepareColumns
	}
	textSelection := resolveDatasetBuildTextSelection(version.Metadata, requestedColumns)
	if len(textSelection.Columns) == 0 {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "text_columns is required for dataset prepare"}
	}
	textColumn := textSelection.TextColumn
	textColumns := textSelection.Columns
	textJoiner := textSelection.Joiner
	rawTextColumn := textColumn
	rawTextColumns := append([]string(nil), textColumns...)
	if isCleanReady(version) {
		if value := metadataString(version.Metadata, "raw_text_column", ""); value != "" {
			rawTextColumn = value
		}
		if values := metadataStringList(version.Metadata, "raw_text_columns"); len(values) > 0 {
			rawTextColumns = values
		}
	}

	outputPath := s.derivePrepareURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}
	maxRows, err := normalizeOptionalPositiveInt(input.MaxRows, "max_rows")
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	batchSize, err := normalizeOptionalPositiveInt(input.BatchSize, "batch_size")
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	progressPath := outputPath + ".progress.json"

	version.PrepareStatus = "preparing"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.PrepareURI = &outputPath
	invalidateDownstreamArtifactsForPrepare(&version, "prepare output changed")
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	version.Metadata["text_column"] = textColumn
	version.Metadata["text_columns"] = append([]string(nil), textColumns...)
	version.Metadata["text_joiner"] = textJoiner
	version.Metadata["raw_text_column"] = rawTextColumn
	version.Metadata["raw_text_columns"] = append([]string(nil), rawTextColumns...)
	version.Metadata["prepare_progress_ref"] = progressPath
	if maxRows > 0 {
		version.Metadata["prepare_max_rows"] = maxRows
	} else {
		delete(version.Metadata, "prepare_max_rows")
	}
	if batchSize > 0 {
		version.Metadata["prepare_batch_size"] = batchSize
	}
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		model := strings.TrimSpace(*input.Model)
		version.PrepareModel = &model
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	var configuredPreparePromptVersion *string
	if version.Profile != nil {
		configuredPreparePromptVersion = version.Profile.PreparePromptVersion
	}
	preparePromptVersion, err := s.resolveEffectiveProjectPromptVersion(projectID, configuredPreparePromptVersion, "prepare")
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	projectPromptOverride, err := s.resolveProjectPromptTemplates(projectID, preparePromptVersion, "prepare", "prepare_batch")
	if err != nil {
		version.PrepareStatus = "failed"
		version.Metadata["prepare_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textColumn,
		"text_columns":       append([]string(nil), textColumns...),
		"text_joiner":        textJoiner,
		"output_path":        outputPath,
		"progress_path":      progressPath,
		"llm_mode":           version.PrepareLLMMode,
	}
	if maxRows > 0 {
		payload["max_rows"] = maxRows
	}
	if batchSize > 0 {
		payload["prepare_batch_size"] = batchSize
	}
	if version.Profile != nil && !isCleanReady(version) {
		if len(version.Profile.RegexRuleNames) > 0 {
			payload["regex_rule_names"] = append([]string(nil), version.Profile.RegexRuleNames...)
		}
	}
	if preparePromptVersion != "" {
		payload["prepare_prompt_version"] = preparePromptVersion
	}
	if projectPromptOverride.UsesProjectSlot {
		payload["prepare_prompt_template"] = projectPromptOverride.RowTemplate
		if projectPromptOverride.BatchTemplate != "" {
			payload["prepare_batch_prompt_template"] = projectPromptOverride.BatchTemplate
		} else {
			payload["prepare_batch_size"] = 1
		}
	}
	if version.PrepareModel != nil && strings.TrimSpace(*version.PrepareModel) != "" {
		payload["model"] = strings.TrimSpace(*version.PrepareModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_prepare", payload)
	if err != nil {
		version.PrepareStatus = "failed"
		version.Metadata["prepare_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.PrepareStatus = "ready"
	version.PreparedAt = &now
	preparedTextColumn := artifactString(response.Artifact, "prepared_text_column")
	if preparedTextColumn == "" {
		preparedTextColumn = "normalized_text"
	}
	prepareRef := artifactString(response.Artifact, "prepared_ref")
	if prepareRef == "" {
		prepareRef = artifactString(response.Artifact, "prepare_uri")
	}
	prepareFormat := artifactString(response.Artifact, "prepare_format")
	if prepareFormat == "" && prepareRef != "" {
		prepareFormat = inferArtifactFormat(prepareRef, "jsonl")
	}
	prepareMetadata := map[string]any{
		"prepare_notes":        response.Notes,
		"text_column":          textColumn,
		"text_columns":         append([]string(nil), textColumns...),
		"text_joiner":          textJoiner,
		"raw_text_column":      rawTextColumn,
		"raw_text_columns":     append([]string(nil), rawTextColumns...),
		"prepared_text_column": preparedTextColumn,
	}
	if prepareRef != "" {
		prepareMetadata["prepared_ref"] = prepareRef
	}
	if prepareFormat != "" {
		prepareMetadata["prepared_format"] = prepareFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		prepareMetadata["row_id_column"] = rowIDColumn
	}
	if progressRef := artifactString(response.Artifact, "progress_ref"); progressRef != "" {
		prepareMetadata["prepare_progress_ref"] = progressRef
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		prepareMetadata["storage_contract_version"] = contractVersion
	}
	if artifactMaxRows, ok := artifactInt(response.Artifact, "max_rows"); ok && artifactMaxRows > 0 {
		prepareMetadata["prepare_max_rows"] = artifactMaxRows
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		prepareMetadata["prepare_usage"] = usage
	}
	clearLLMFallbackMetadata(version.Metadata, "prepare")
	if fallbackInfo := applyLLMFallbackMetadata(prepareMetadata, "prepare", response.Artifact); fallbackInfo.Fallback {
		log.Printf(
			"dataset build llm fallback: build_type=prepare project_id=%s dataset_id=%s dataset_version_id=%s model=%s reason=%s",
			projectID,
			datasetID,
			version.DatasetVersionID,
			fallbackInfo.Model,
			fallbackInfo.Reason,
		)
	}
	version.Metadata = mergeStringAny(version.Metadata, prepareMetadata)
	if promptVersion := artifactString(response.Artifact, "prepare_prompt_version"); promptVersion != "" {
		version.PreparePromptVer = &promptVersion
	}
	if prepareURI := artifactString(response.Artifact, "prepare_uri"); prepareURI != "" {
		version.PrepareURI = &prepareURI
	}
	if prepareModel := artifactString(response.Artifact, "prepare_model"); prepareModel != "" {
		version.PrepareModel = &prepareModel
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		version.Metadata = mergeStringAny(version.Metadata, map[string]any{
			"prepare_summary": summary,
		})
		if value, ok := summary["output_row_count"]; ok {
			if intValue, ok := anyToInt(value); ok {
				version.RecordCount = &intValue
			}
		}
	}
	delete(version.Metadata, "prepare_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	result := s.maybeRunEagerSentiment(projectID, datasetID, version)
	enrichDatasetVersionView(&result)
	return result, nil
}

func (s *DatasetService) BuildPrepareSample(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest) (domain.DatasetPrepareSampleResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetPrepareSampleResponse{}, ErrInvalidArgument{Message: "dataset prepare sample requires unstructured or mixed dataset version"}
	}
	if status := cleanStatus(version); status == "queued" || status == "cleaning" || status == "failed" || status == "stale" {
		return domain.DatasetPrepareSampleResponse{}, ErrInvalidArgument{Message: "dataset clean must be ready before prepare sample"}
	}

	datasetName, defaultPrepareColumns := datasetSourceForPrepare(version)
	requestedColumns := input.TextColumns
	if len(normalizeStringList(requestedColumns)) == 0 && len(defaultPrepareColumns) > 0 {
		requestedColumns = defaultPrepareColumns
	}
	textSelection := resolveDatasetBuildTextSelection(version.Metadata, requestedColumns)
	if len(textSelection.Columns) == 0 {
		return domain.DatasetPrepareSampleResponse{}, ErrInvalidArgument{Message: "text_columns is required for dataset prepare sample"}
	}

	maxRows, err := normalizeOptionalPositiveInt(input.MaxRows, "max_rows")
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}
	if maxRows == 0 {
		maxRows = 10
	}
	batchSize, err := normalizeOptionalPositiveInt(input.BatchSize, "batch_size")
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}

	outputPath := ""
	if path, ok := s.datasetArtifactPath(version, "prepare_sample", fmt.Sprintf("sample-%s.parquet", id.New())); ok {
		outputPath = path
	} else {
		handle, createErr := os.CreateTemp("", "prepare-sample-*.parquet")
		if createErr != nil {
			return domain.DatasetPrepareSampleResponse{}, createErr
		}
		outputPath = handle.Name()
		_ = handle.Close()
		_ = os.Remove(outputPath)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}

	var configuredPreparePromptVersion *string
	if version.Profile != nil {
		configuredPreparePromptVersion = version.Profile.PreparePromptVersion
	}
	preparePromptVersion, err := s.resolveEffectiveProjectPromptVersion(projectID, configuredPreparePromptVersion, "prepare")
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}
	projectPromptOverride, err := s.resolveProjectPromptTemplates(projectID, preparePromptVersion, "prepare", "prepare_batch")
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textSelection.TextColumn,
		"text_columns":       append([]string(nil), textSelection.Columns...),
		"text_joiner":        textSelection.Joiner,
		"output_path":        outputPath,
		"llm_mode":           version.PrepareLLMMode,
		"max_rows":           maxRows,
	}
	if batchSize > 0 {
		payload["prepare_batch_size"] = batchSize
	}
	if version.Profile != nil && !isCleanReady(version) && len(version.Profile.RegexRuleNames) > 0 {
		payload["regex_rule_names"] = append([]string(nil), version.Profile.RegexRuleNames...)
	}
	if preparePromptVersion != "" {
		payload["prepare_prompt_version"] = preparePromptVersion
	}
	if projectPromptOverride.UsesProjectSlot {
		payload["prepare_prompt_template"] = projectPromptOverride.RowTemplate
		if projectPromptOverride.BatchTemplate != "" {
			payload["prepare_batch_prompt_template"] = projectPromptOverride.BatchTemplate
		} else {
			payload["prepare_batch_size"] = 1
		}
	}
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		payload["model"] = strings.TrimSpace(*input.Model)
	} else if version.PrepareModel != nil && strings.TrimSpace(*version.PrepareModel) != "" {
		payload["model"] = strings.TrimSpace(*version.PrepareModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_prepare", payload)
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}

	preparedRef := artifactString(response.Artifact, "prepared_ref")
	if preparedRef == "" {
		preparedRef = artifactString(response.Artifact, "prepare_uri")
	}
	if preparedRef == "" {
		preparedRef = outputPath
	}
	prepareFormat := artifactString(response.Artifact, "prepare_format")
	if prepareFormat == "" {
		prepareFormat = inferArtifactFormat(preparedRef, "parquet")
	}
	if prepareFormat != "parquet" {
		return domain.DatasetPrepareSampleResponse{}, ErrInvalidArgument{Message: "prepare sample supports parquet artifact only"}
	}

	samples, err := loadPrepareSamplesFromParquet(preparedRef, maxRows, "")
	if err != nil {
		return domain.DatasetPrepareSampleResponse{}, err
	}
	var summary *domain.DatasetPrepareSummary
	if rawSummary, ok := response.Artifact["summary"].(map[string]any); ok {
		summary = buildPrepareSummary(map[string]any{"prepare_summary": rawSummary})
	}
	return domain.DatasetPrepareSampleResponse{
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		PreparedRef:      preparedRef,
		PrepareFormat:    prepareFormat,
		SampleLimit:      maxRows,
		Summary:          summary,
		Samples:          samples,
	}, nil
}

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
	return version, nil
}

func (s *DatasetService) BuildClusters(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "cluster build requires unstructured or mixed dataset version"}
	}
	if !embeddingBuildReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embeddings must be ready before cluster build"}
	}

	force := input.Force != nil && *input.Force
	normalizedRequest := domain.NormalizeClusterBuildRequest(input)
	if domain.ClusterRequestMatchesMetadata(normalizedRequest, version.Metadata) && !force {
		return version, nil
	}

	embeddingIndexSourceRef := strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", ""))
	if input.EmbeddingIndexSourceRef != nil && strings.TrimSpace(*input.EmbeddingIndexSourceRef) != "" {
		embeddingIndexSourceRef = strings.TrimSpace(*input.EmbeddingIndexSourceRef)
	}
	if embeddingIndexSourceRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embedding index source ref is required for cluster build"}
	}

	chunkRef := strings.TrimSpace(metadataString(version.Metadata, "chunk_ref", ""))
	if input.ChunkRef != nil && strings.TrimSpace(*input.ChunkRef) != "" {
		chunkRef = strings.TrimSpace(*input.ChunkRef)
	}
	if chunkRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "chunk_ref is required for cluster build"}
	}

	outputPath := s.deriveClusterURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}
	membershipOutputPath := deriveClusterMembershipURI(outputPath)
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := ensureParentDir(membershipOutputPath); err != nil {
		return domain.DatasetVersion{}, err
	}

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["cluster_status"] = "building"
	version.Metadata["cluster_ref"] = outputPath
	version.Metadata["cluster_format"] = "json"
	version.Metadata["cluster_summary_ref"] = outputPath
	version.Metadata["cluster_summary_format"] = "json"
	version.Metadata["cluster_membership_ref"] = membershipOutputPath
	version.Metadata["cluster_membership_format"] = "parquet"
	version.Metadata["cluster_source_embedding_ref"] = embeddingIndexSourceRef
	version.Metadata["cluster_similarity_threshold"] = *normalizedRequest.SimilarityThreshold
	version.Metadata["cluster_top_n"] = *normalizedRequest.TopN
	version.Metadata["cluster_sample_n"] = *normalizedRequest.SampleN
	version.Metadata["cluster_params_hash"] = domain.ClusterRequestHash(normalizedRequest)
	delete(version.Metadata, "cluster_error")
	delete(version.Metadata, "cluster_stale_reason")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id":           version.DatasetVersionID,
		"dataset_name":                 datasetSourceForUnstructured(version),
		"embedding_index_source_ref":   embeddingIndexSourceRef,
		"chunk_ref":                    chunkRef,
		"output_path":                  outputPath,
		"cluster_similarity_threshold": *normalizedRequest.SimilarityThreshold,
		"top_n":                        *normalizedRequest.TopN,
		"sample_n":                     *normalizedRequest.SampleN,
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_cluster_build", payload)
	if err != nil {
		version.Metadata["cluster_status"] = "failed"
		version.Metadata["cluster_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.ReadyAt = &now
	clusterRef := artifactString(response.Artifact, "cluster_ref")
	if clusterRef == "" {
		clusterRef = outputPath
	}
	clusterSummaryRef := artifactString(response.Artifact, "cluster_summary_ref")
	if clusterSummaryRef == "" {
		clusterSummaryRef = clusterRef
	}
	clusterSummaryFormat := artifactString(response.Artifact, "cluster_summary_format")
	if clusterSummaryFormat == "" {
		clusterSummaryFormat = artifactString(response.Artifact, "cluster_format")
	}
	if clusterSummaryFormat == "" {
		clusterSummaryFormat = "json"
	}
	clusterMembershipRef := artifactString(response.Artifact, "cluster_membership_ref")
	if clusterMembershipRef == "" {
		clusterMembershipRef = membershipOutputPath
	}
	clusterMembershipFormat := artifactString(response.Artifact, "cluster_membership_format")
	if clusterMembershipFormat == "" {
		clusterMembershipFormat = "parquet"
	}
	clusterMetadata := map[string]any{
		"cluster_status":               "ready",
		"cluster_ref":                  clusterRef,
		"cluster_format":               clusterSummaryFormat,
		"cluster_summary_ref":          clusterSummaryRef,
		"cluster_summary_format":       clusterSummaryFormat,
		"cluster_membership_ref":       clusterMembershipRef,
		"cluster_membership_format":    clusterMembershipFormat,
		"cluster_notes":                response.Notes,
		"cluster_algorithm":            artifactString(response.Artifact, "cluster_algorithm"),
		"cluster_source_embedding_ref": embeddingIndexSourceRef,
		"cluster_similarity_threshold": *normalizedRequest.SimilarityThreshold,
		"cluster_top_n":                *normalizedRequest.TopN,
		"cluster_sample_n":             *normalizedRequest.SampleN,
		"cluster_params_hash":          domain.ClusterRequestHash(normalizedRequest),
		"clustered_at":                 now,
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		clusterMetadata["cluster_summary"] = summary
	}
	version.Metadata = mergeStringAny(version.Metadata, clusterMetadata)
	delete(version.Metadata, "cluster_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) BuildSentiment(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "sentiment labeling requires unstructured or mixed dataset version"}
	}
	if requiresPrepare(version) && !isPrepareReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare must be ready before sentiment labeling"}
	}

	force := input.Force != nil && *input.Force
	if version.SentimentStatus == "ready" && version.SentimentURI != nil && !force {
		return version, nil
	}

	source := domain.ResolveDatasetSource(version)
	textColumn := source.TextColumn
	textColumns := normalizeStringList(input.TextColumns)
	if len(textColumns) > 0 {
		textColumn = datasetBuildTextColumnLabel(textColumns)
		if source.Stage != domain.DatasetSourceStageRaw && textSelectionMatchesRawSource(version, textColumn, textColumns) {
			textColumn = source.TextColumn
			textColumns = append([]string(nil), source.TextColumns...)
		}
	} else {
		if existingColumns := metadataStringList(version.Metadata, "sentiment_text_columns"); len(existingColumns) > 0 {
			textColumns = existingColumns
			textColumn = datasetBuildTextColumnLabel(textColumns)
			if source.Stage != domain.DatasetSourceStageRaw && textSelectionMatchesRawSource(version, textColumn, textColumns) {
				textColumn = source.TextColumn
				textColumns = append([]string(nil), source.TextColumns...)
			}
		} else {
			textColumn = metadataString(version.Metadata, "sentiment_text_column", textColumn)
			textColumns = []string{textColumn}
		}
	}
	textJoiner := defaultDatasetBuildTextJoiner
	datasetName := datasetSourceForUnstructured(version)
	outputPath := s.deriveSentimentURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}

	version.SentimentStatus = "labeling"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.SentimentURI = &outputPath
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	version.Metadata["sentiment_dataset_name"] = datasetName
	version.Metadata["sentiment_text_column"] = textColumn
	version.Metadata["sentiment_text_columns"] = append([]string(nil), textColumns...)
	version.Metadata["sentiment_text_joiner"] = textJoiner
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		model := strings.TrimSpace(*input.Model)
		version.SentimentModel = &model
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	var configuredSentimentPromptVersion *string
	if version.Profile != nil {
		configuredSentimentPromptVersion = version.Profile.SentimentPromptVersion
	}
	sentimentPromptVersion, err := s.resolveEffectiveProjectPromptVersion(projectID, configuredSentimentPromptVersion, "sentiment")
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	projectPromptOverride, err := s.resolveProjectPromptTemplates(projectID, sentimentPromptVersion, "sentiment", "sentiment_batch")
	if err != nil {
		version.SentimentStatus = "failed"
		version.Metadata["sentiment_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textColumn,
		"text_columns":       append([]string(nil), textColumns...),
		"text_joiner":        textJoiner,
		"output_path":        outputPath,
		"llm_mode":           version.SentimentLLMMode,
	}
	if sentimentPromptVersion != "" {
		payload["sentiment_prompt_version"] = sentimentPromptVersion
	}
	if projectPromptOverride.UsesProjectSlot {
		payload["sentiment_prompt_template"] = projectPromptOverride.RowTemplate
		if projectPromptOverride.BatchTemplate != "" {
			payload["sentiment_batch_prompt_template"] = projectPromptOverride.BatchTemplate
		} else {
			payload["sentiment_batch_size"] = 1
		}
	}
	if version.SentimentModel != nil && strings.TrimSpace(*version.SentimentModel) != "" {
		payload["model"] = strings.TrimSpace(*version.SentimentModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/sentiment_label", payload)
	if err != nil {
		version.SentimentStatus = "failed"
		version.Metadata["sentiment_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.SentimentStatus = "ready"
	version.SentimentLabeledAt = &now
	version.ReadyAt = &now
	sentimentRef := artifactString(response.Artifact, "sentiment_ref")
	if sentimentRef == "" {
		sentimentRef = artifactString(response.Artifact, "sentiment_uri")
	}
	sentimentFormat := artifactString(response.Artifact, "sentiment_format")
	if sentimentFormat == "" && sentimentRef != "" {
		sentimentFormat = inferArtifactFormat(sentimentRef, "jsonl")
	}
	sentimentMetadata := map[string]any{
		"sentiment_notes":             response.Notes,
		"sentiment_text_column":       textColumn,
		"sentiment_text_columns":      append([]string(nil), textColumns...),
		"sentiment_text_joiner":       textJoiner,
		"sentiment_label_column":      artifactString(response.Artifact, "sentiment_label_column"),
		"sentiment_reason_column":     artifactString(response.Artifact, "sentiment_reason_column"),
		"sentiment_confidence_column": artifactString(response.Artifact, "sentiment_confidence_column"),
	}
	if sentimentRef != "" {
		sentimentMetadata["sentiment_ref"] = sentimentRef
	}
	if sentimentFormat != "" {
		sentimentMetadata["sentiment_format"] = sentimentFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		sentimentMetadata["row_id_column"] = rowIDColumn
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		sentimentMetadata["storage_contract_version"] = contractVersion
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		sentimentMetadata["sentiment_usage"] = usage
	}
	clearLLMFallbackMetadata(version.Metadata, "sentiment")
	if fallbackInfo := applyLLMFallbackMetadata(sentimentMetadata, "sentiment", response.Artifact); fallbackInfo.Fallback {
		log.Printf(
			"dataset build llm fallback: build_type=sentiment project_id=%s dataset_id=%s dataset_version_id=%s model=%s reason=%s",
			projectID,
			datasetID,
			version.DatasetVersionID,
			fallbackInfo.Model,
			fallbackInfo.Reason,
		)
	}
	version.Metadata = mergeStringAny(version.Metadata, sentimentMetadata)
	if sentimentURI := artifactString(response.Artifact, "sentiment_uri"); sentimentURI != "" {
		version.SentimentURI = &sentimentURI
	}
	if sentimentModel := artifactString(response.Artifact, "sentiment_model"); sentimentModel != "" {
		version.SentimentModel = &sentimentModel
	}
	if promptVersion := artifactString(response.Artifact, "sentiment_prompt_version"); promptVersion != "" {
		version.SentimentPromptVer = &promptVersion
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		version.Metadata["sentiment_summary"] = summary
	}
	delete(version.Metadata, "sentiment_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}
