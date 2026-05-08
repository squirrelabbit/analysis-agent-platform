package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/obs"
)

type sentimentBuildTextSelection struct {
	DatasetName string
	TextColumn  string
	TextColumns []string
	TextJoiner  string
}

func resolveSentimentBuildTextSelection(version domain.DatasetVersion, requestedColumns []string) sentimentBuildTextSelection {
	source := domain.ResolveDatasetSource(version)
	textColumn := source.TextColumn
	textColumns := normalizeStringList(requestedColumns)
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
	return sentimentBuildTextSelection{
		DatasetName: source.DatasetName,
		TextColumn:  textColumn,
		TextColumns: append([]string(nil), textColumns...),
		TextJoiner:  defaultDatasetBuildTextJoiner,
	}
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

	textSelection := resolveSentimentBuildTextSelection(version, input.TextColumns)
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
	version.Metadata["sentiment_dataset_name"] = textSelection.DatasetName
	version.Metadata["sentiment_text_column"] = textSelection.TextColumn
	version.Metadata["sentiment_text_columns"] = append([]string(nil), textSelection.TextColumns...)
	version.Metadata["sentiment_text_joiner"] = textSelection.TextJoiner
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
		"dataset_name":       textSelection.DatasetName,
		"text_column":        textSelection.TextColumn,
		"text_columns":       append([]string(nil), textSelection.TextColumns...),
		"text_joiner":        textSelection.TextJoiner,
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
		"sentiment_text_column":       textSelection.TextColumn,
		"sentiment_text_columns":      append([]string(nil), textSelection.TextColumns...),
		"sentiment_text_joiner":       textSelection.TextJoiner,
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
		obs.Logger.Warn("dataset build llm fallback",
			"event", "llm.fallback.triggered",
			"build_type", "sentiment",
			"project_id", projectID,
			"dataset_id", datasetID,
			"dataset_version_id", version.DatasetVersionID,
			"model", fallbackInfo.Model,
			"reason", fallbackInfo.Reason,
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
	enrichDatasetVersionView(&version)
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) BuildSentimentSample(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentSampleRequest) (domain.DatasetSentimentSampleResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetSentimentSampleResponse{}, ErrInvalidArgument{Message: "sentiment sample requires unstructured or mixed dataset version"}
	}
	if requiresPrepare(version) && !isPrepareReady(version) {
		return domain.DatasetSentimentSampleResponse{}, ErrInvalidArgument{Message: "dataset prepare must be ready before sentiment sample"}
	}

	textSelection := resolveSentimentBuildTextSelection(version, input.TextColumns)
	if len(textSelection.TextColumns) == 0 {
		return domain.DatasetSentimentSampleResponse{}, ErrInvalidArgument{Message: "text_columns is required for sentiment sample"}
	}

	maxRows, err := normalizeDatasetBuildSampleRows(input.MaxRows, "max_rows")
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}
	batchSize, err := normalizeOptionalPositiveInt(input.BatchSize, "batch_size")
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}

	outputPath := ""
	if path, ok := s.datasetArtifactPath(version, "sentiment_sample", fmt.Sprintf("sample-%s.parquet", id.New())); ok {
		outputPath = path
	} else {
		handle, createErr := os.CreateTemp("", "sentiment-sample-*.parquet")
		if createErr != nil {
			return domain.DatasetSentimentSampleResponse{}, createErr
		}
		outputPath = handle.Name()
		_ = handle.Close()
		_ = os.Remove(outputPath)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}

	var configuredSentimentPromptVersion *string
	if version.Profile != nil {
		configuredSentimentPromptVersion = version.Profile.SentimentPromptVersion
	}
	sentimentPromptVersion, err := s.resolveEffectiveProjectPromptVersion(projectID, configuredSentimentPromptVersion, "sentiment")
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}
	projectPromptOverride, err := s.resolveProjectPromptTemplates(projectID, sentimentPromptVersion, "sentiment", "sentiment_batch")
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       textSelection.DatasetName,
		"text_column":        textSelection.TextColumn,
		"text_columns":       append([]string(nil), textSelection.TextColumns...),
		"text_joiner":        textSelection.TextJoiner,
		"output_path":        outputPath,
		"llm_mode":           version.SentimentLLMMode,
		"max_rows":           maxRows,
	}
	if batchSize > 0 {
		payload["sentiment_batch_size"] = batchSize
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
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		payload["model"] = strings.TrimSpace(*input.Model)
	} else if version.SentimentModel != nil && strings.TrimSpace(*version.SentimentModel) != "" {
		payload["model"] = strings.TrimSpace(*version.SentimentModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/sentiment_label", payload)
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}

	sentimentRef := artifactString(response.Artifact, "sentiment_ref")
	if sentimentRef == "" {
		sentimentRef = artifactString(response.Artifact, "sentiment_uri")
	}
	if sentimentRef == "" {
		sentimentRef = outputPath
	}
	sentimentFormat := artifactString(response.Artifact, "sentiment_format")
	if sentimentFormat == "" {
		sentimentFormat = inferArtifactFormat(sentimentRef, "parquet")
	}
	if sentimentFormat != "parquet" {
		return domain.DatasetSentimentSampleResponse{}, ErrInvalidArgument{Message: "sentiment sample supports parquet artifact only"}
	}

	samples, err := loadSentimentSamplesFromParquet(sentimentRef, maxRows)
	if err != nil {
		return domain.DatasetSentimentSampleResponse{}, err
	}
	var summary *domain.DatasetSentimentSummary
	if rawSummary, ok := response.Artifact["summary"].(map[string]any); ok {
		summary = buildSentimentSummary(map[string]any{"sentiment_summary": rawSummary})
	}
	return domain.DatasetSentimentSampleResponse{
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		SentimentRef:     sentimentRef,
		SentimentFormat:  sentimentFormat,
		SampleLimit:      maxRows,
		Summary:          summary,
		Columns:          sentimentSampleColumns(),
		Samples:          samples,
	}, nil
}
