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
