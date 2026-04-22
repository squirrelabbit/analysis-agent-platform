package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
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
	version.CleanStatus = "cleaning"
	version.CleanURI = &outputPath
	version.CleanedRef = &outputPath
	version.CleanedAt = nil
	version.Metadata["clean_status"] = "cleaning"
	version.Metadata["clean_uri"] = outputPath
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
		version.CleanStatus = "failed"
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
	version.CleanStatus = "ready"
	version.CleanURI = &cleanedRef
	version.CleanedRef = &cleanedRef
	version.CleanedAt = &now
	cleanFormat := artifactString(response.Artifact, "clean_format")
	if cleanFormat == "" {
		cleanFormat = inferArtifactFormat(cleanedRef, "parquet")
	}
	cleanMetadata := map[string]any{
		"clean_status":        "ready",
		"clean_uri":           cleanedRef,
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
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}
