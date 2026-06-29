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

	// 2026-05-21 — force flag 제거. 이미 ready면 그대로 반환 (재정제는 새 upload로).
	if isCleanReady(version) {
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
	progressPath := outputPath + ".progress.json"

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
	// δ-3 (5/21) — ADR-018 β2로 prepare/sentiment/embedding/segment 단계가
	// 사라지면서 invalidateDownstreamArtifactsForClean → invalidatePrepareArtifacts
	// 체인이 모두 dead-code가 됐다. 현재 downstream(doc_genuineness, clause_label)
	// 은 명시 트리거라 재실행 시 metadata가 자연히 덮어써진다. 명시적
	// invalidation이 필요해지면 여기서 doc_genuineness_status / clause_label_status
	// 를 stale로 표시하는 helper를 도입.
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
	}
	// silverone 2026-05-28 (clean 정식화) — date_column 명시 시 worker에
	// 전달. worker가 source row의 해당 컬럼을 created_at ISO 8601로 변환.
	if input.DateColumn != nil {
		if v := strings.TrimSpace(*input.DateColumn); v != "" {
			payload["date_column"] = v
			version.Metadata["clean_date_column"] = v
		}
	}
	if version.Profile != nil && len(version.Profile.RegexRuleNames) > 0 {
		payload["regex_rule_names"] = append([]string(nil), version.Profile.RegexRuleNames...)
	}

	// 5/11 (silverone): dataset_build를 plan skill과 분리한다는 결정에 따라
	// 모든 build이 buildClient port(RunTask/RunDatasetClean)로 통일됐다(ADR-031 4단계,
	// 2026-06-29). taskPath/timeout/4xx-5xx wrap은 client 내부에 있고, 후속 처리
	// (artifactString 등)는 동일 shape의 response를 그대로 쓴다.
	response, err := s.buildClient().RunDatasetClean(context.Background(), payload)
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
