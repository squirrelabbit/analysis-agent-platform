package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func normalizeDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	normalized := &domain.DatasetProfile{
		ProfileID:              strings.TrimSpace(profile.ProfileID),
		PreparePromptVersion:   trimStringPointer(profile.PreparePromptVersion),
		SentimentPromptVersion: trimStringPointer(profile.SentimentPromptVersion),
		RegexRuleNames:         normalizeStringList(profile.RegexRuleNames),
		GarbageRuleNames:       normalizeStringList(profile.GarbageRuleNames),
		EmbeddingModel:         trimStringPointer(profile.EmbeddingModel),
	}
	if normalized.ProfileID == "" &&
		normalized.PreparePromptVersion == nil &&
		normalized.SentimentPromptVersion == nil &&
		len(normalized.RegexRuleNames) == 0 &&
		len(normalized.GarbageRuleNames) == 0 &&
		normalized.EmbeddingModel == nil {
		return nil
	}
	return normalized
}

func cloneDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	cloned := &domain.DatasetProfile{
		ProfileID:        profile.ProfileID,
		RegexRuleNames:   append([]string(nil), profile.RegexRuleNames...),
		GarbageRuleNames: append([]string(nil), profile.GarbageRuleNames...),
	}
	if profile.PreparePromptVersion != nil {
		value := strings.TrimSpace(*profile.PreparePromptVersion)
		cloned.PreparePromptVersion = &value
	}
	if profile.SentimentPromptVersion != nil {
		value := strings.TrimSpace(*profile.SentimentPromptVersion)
		cloned.SentimentPromptVersion = &value
	}
	if profile.EmbeddingModel != nil {
		value := strings.TrimSpace(*profile.EmbeddingModel)
		cloned.EmbeddingModel = &value
	}
	return cloned
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

type datasetBuildTextSelection struct {
	TextColumn string
	Columns    []string
	Joiner     string
}

const defaultDatasetBuildTextJoiner = "\n\n"

func resolveDatasetBuildTextSelection(
	metadata map[string]any,
	inputColumns []string,
) datasetBuildTextSelection {
	columns := normalizeStringList(inputColumns)
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "raw_text_columns")
	}
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "text_columns")
	}
	return datasetBuildTextSelection{
		TextColumn: datasetBuildTextColumnLabel(columns),
		Columns:    append([]string(nil), columns...),
		Joiner:     defaultDatasetBuildTextJoiner,
	}
}

func resolveCleanPreprocessOptions(
	metadata map[string]any,
	input *domain.DatasetCleanPreprocessOptions,
) map[string]bool {
	if input != nil {
		return cleanPreprocessOptionsFromDomain(input)
	}
	if metadata == nil {
		return defaultCleanPreprocessOptions()
	}
	for _, key := range []string{"clean_preprocess_options", "preprocess_options"} {
		if options := cleanPreprocessOptionsFromAny(metadata[key]); len(options) > 0 {
			return options
		}
	}
	return defaultCleanPreprocessOptions()
}

func defaultCleanPreprocessOptions() map[string]bool {
	return map[string]bool{
		"remove_english":       false,
		"remove_numbers":       false,
		"remove_special":       false,
		"remove_monosyllables": false,
	}
}

func cleanPreprocessOptionsFromDomain(input *domain.DatasetCleanPreprocessOptions) map[string]bool {
	if input == nil {
		return defaultCleanPreprocessOptions()
	}
	return map[string]bool{
		"remove_english":       input.RemoveEnglish,
		"remove_numbers":       input.RemoveNumbers,
		"remove_special":       input.RemoveSpecial,
		"remove_monosyllables": input.RemoveMonosyllables,
	}
}

func cleanPreprocessOptionsFromAny(value any) map[string]bool {
	result := defaultCleanPreprocessOptions()
	hasKnownKey := false
	if source, ok := value.(map[string]bool); ok {
		for key := range result {
			if raw, exists := source[key]; exists {
				result[key] = raw
				hasKnownKey = true
			}
		}
		if !hasKnownKey {
			return nil
		}
		return result
	}
	source, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for key := range result {
		if raw, exists := source[key]; exists {
			result[key] = anyBoolValue(raw)
			hasKnownKey = true
		}
	}
	if !hasKnownKey {
		return nil
	}
	return result
}

func hasEnabledPreparePreprocessOption(options map[string]bool) bool {
	for _, enabled := range options {
		if enabled {
			return true
		}
	}
	return false
}

func datasetBuildTextColumnLabel(columns []string) string {
	normalized := normalizeStringList(columns)
	if len(normalized) == 0 {
		return ""
	}
	if len(normalized) == 1 {
		return normalized[0]
	}
	return strings.Join(normalized, " + ")
}

func metadataRawString(metadata map[string]any, key string) (string, bool) {
	if metadata == nil {
		return "", false
	}
	value, ok := metadata[key]
	if !ok {
		return "", false
	}
	return anyStringValue(value), true
}

func metadataTime(metadata map[string]any, key string) (time.Time, bool) {
	if metadata == nil {
		return time.Time{}, false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return time.Time{}, false
	}
	switch typed := value.(type) {
	case time.Time:
		return typed, true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}

func metadataStringList(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	value, ok := metadata[key]
	if !ok {
		return nil
	}
	return anyStringList(value)
}

func anyStringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return normalizeStringList(typed)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			values = append(values, anyStringValue(item))
		}
		return normalizeStringList(values)
	case string:
		return normalizeStringList([]string{typed})
	default:
		return nil
	}
}

func trimStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func shouldActivateDatasetVersionOnCreate(value *bool) bool {
	if value == nil {
		return true
	}
	return *value
}

func markDatasetVersionActive(version *domain.DatasetVersion, dataset domain.Dataset) {
	if version == nil {
		return
	}
	version.IsActive = dataset.ActiveDatasetVersionID != nil && *dataset.ActiveDatasetVersionID == version.DatasetVersionID
}

func (s *DatasetService) saveDatasetActiveVersion(dataset domain.Dataset, datasetVersionID *string) (domain.Dataset, error) {
	dataset.ActiveDatasetVersionID = trimStringPointer(datasetVersionID)
	now := time.Now().UTC()
	dataset.ActiveVersionUpdatedAt = &now
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func enrichDatasetVersionView(version *domain.DatasetVersion) {
	if version == nil {
		return
	}
	version.CleanStatus = cleanStatus(*version)
	if cleanedRef := metadataString(version.Metadata, "cleaned_ref", ""); cleanedRef != "" {
		version.CleanedRef = &cleanedRef
	}
	if cleanedAt, ok := metadataTime(version.Metadata, "cleaned_at"); ok {
		version.CleanedAt = &cleanedAt
	}
	version.CleanSummary = buildCleanSummary(version.Metadata)
	version.PrepareSummary = buildPrepareSummary(version.Metadata)
}

func buildCleanSummary(metadata map[string]any) *domain.DatasetCleanSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["clean_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetCleanSummary{
		InputRowCount:         intValueOrZero(raw["input_row_count"]),
		OutputRowCount:        intValueOrZero(raw["output_row_count"]),
		KeptCount:             intValueOrZero(raw["kept_count"]),
		DroppedCount:          intValueOrZero(raw["dropped_count"]),
		SkippedRowCount:       intValueOrZero(raw["skipped_row_count"]),
		TextColumn:            strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:           anyStringList(raw["text_columns"]),
		TextJoiner:            anyStringValue(raw["text_joiner"]),
		PreprocessOptions:     boolMapValue(raw["preprocess_options"]),
		SourceInputCharCount:  intValueOrZero(raw["source_input_char_count"]),
		CleanedInputCharCount: intValueOrZero(raw["cleaned_input_char_count"]),
		CleanReducedCharCount: intValueOrZero(raw["clean_reduced_char_count"]),
		CleanRegexRuleHits:    intMapValue(raw["clean_regex_rule_hits"]),
	}
}

func buildSentimentSummary(metadata map[string]any) *domain.DatasetSentimentSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["sentiment_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetSentimentSummary{
		InputRowCount:      intValueOrZero(raw["input_row_count"]),
		LabeledRowCount:    intValueOrZero(raw["labeled_row_count"]),
		TextColumn:         strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:        anyStringList(raw["text_columns"]),
		TextJoiner:         anyStringValue(raw["text_joiner"]),
		SentimentBatchSize: intValueOrZero(raw["sentiment_batch_size"]),
		LabelCounts:        intMapValue(raw["label_counts"]),
	}
}

func buildPrepareSummary(metadata map[string]any) *domain.DatasetPrepareSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["prepare_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetPrepareSummary{
		InputRowCount:  intValueOrZero(raw["input_row_count"]),
		OutputRowCount: intValueOrZero(raw["output_row_count"]),
		KeptCount:      intValueOrZero(raw["kept_count"]),
		ReviewCount:    intValueOrZero(raw["review_count"]),
		DroppedCount:   intValueOrZero(raw["dropped_count"]),
		TextColumn:     strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:    anyStringList(raw["text_columns"]),
		TextJoiner:     anyStringValue(raw["text_joiner"]),
	}
}

func cloneSentimentSummary(summary *domain.DatasetSentimentSummary) *domain.DatasetSentimentSummary {
	if summary == nil {
		return nil
	}
	cloned := *summary
	if len(summary.TextColumns) > 0 {
		cloned.TextColumns = append([]string(nil), summary.TextColumns...)
	}
	if len(summary.LabelCounts) > 0 {
		cloned.LabelCounts = cloneStringIntMap(summary.LabelCounts)
	}
	return &cloned
}

func clonePrepareSummary(summary *domain.DatasetPrepareSummary) *domain.DatasetPrepareSummary {
	if summary == nil {
		return nil
	}
	cloned := *summary
	if len(summary.TextColumns) > 0 {
		cloned.TextColumns = append([]string(nil), summary.TextColumns...)
	}
	return &cloned
}

func datasetSourceForUnstructured(version domain.DatasetVersion) string {
	return domain.ResolveDatasetSource(version).DatasetName
}

func datasetSourceForPrepare(version domain.DatasetVersion) (string, []string) {
	source := domain.ResolvePrepareInputSource(version)
	return source.DatasetName, source.TextColumns
}

func datasetSourceForSentiment(version domain.DatasetVersion) string {
	if isSentimentReady(version) && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	return deriveSentimentURI(version)
}

func defaultPreparedTextColumn(version domain.DatasetVersion) string {
	return domain.DatasetSourceDefaultTextColumn(version)
}

func textSelectionMatchesRawSource(version domain.DatasetVersion, textColumn string, textColumns []string) bool {
	if domain.DatasetSourceIsRawTextColumn(version, textColumn) {
		return true
	}
	for _, column := range textColumns {
		if domain.DatasetSourceIsRawTextColumn(version, column) {
			return true
		}
	}
	return false
}

func defaultPrepareRequired(dataType string, value *bool) bool {
	if value != nil {
		return *value
	}
	return false
}

func requiresClean(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return cleanStatus(version) != "not_applicable"
	default:
		return false
	}
}

func requiresPrepare(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return version.PrepareStatus != "not_requested" && version.PrepareStatus != "not_applicable"
	default:
		return false
	}
}

func requiresSentiment(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return version.SentimentStatus != "" && version.SentimentStatus != "not_requested" && version.SentimentStatus != "not_applicable"
	default:
		return false
	}
}

func cleanStatus(version domain.DatasetVersion) string {
	status := strings.TrimSpace(metadataString(version.Metadata, "clean_status", ""))
	if status != "" {
		return status
	}
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return "not_requested"
	default:
		return "not_applicable"
	}
}

func isCleanReady(version domain.DatasetVersion) bool {
	return cleanStatus(version) == "ready" && strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", "")) != ""
}

func isPrepareReady(version domain.DatasetVersion) bool {
	return version.PrepareStatus == "ready" && version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != ""
}

func isSentimentReady(version domain.DatasetVersion) bool {
	return version.SentimentStatus == "ready" && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != ""
}

func embeddingBuildReady(version domain.DatasetVersion) bool {
	if version.EmbeddingStatus != "ready" {
		return false
	}
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", "")) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_ref", "")) != "" {
		return true
	}
	return false
}

func datasetClusterReady(version domain.DatasetVersion) bool {
	if strings.TrimSpace(metadataString(version.Metadata, "cluster_status", "")) != "ready" {
		return false
	}
	return strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", "")) != ""
}

func resolvePrepareArtifact(version domain.DatasetVersion) (string, string, error) {
	preparedRef := strings.TrimSpace(metadataString(version.Metadata, "prepared_ref", ""))
	if preparedRef == "" && version.PrepareURI != nil {
		preparedRef = strings.TrimSpace(*version.PrepareURI)
	}
	if version.PrepareStatus != "ready" || preparedRef == "" {
		return "", "", ErrInvalidArgument{Message: "prepare artifact is not ready"}
	}
	prepareFormat := strings.TrimSpace(metadataString(version.Metadata, "prepared_format", ""))
	if prepareFormat == "" {
		prepareFormat = strings.TrimSpace(metadataString(version.Metadata, "prepare_format", ""))
	}
	if prepareFormat == "" {
		prepareFormat = inferArtifactFormat(preparedRef, "parquet")
	}
	return preparedRef, prepareFormat, nil
}

func resolveSentimentArtifact(version domain.DatasetVersion) (string, string, error) {
	sentimentRef := strings.TrimSpace(metadataString(version.Metadata, "sentiment_ref", ""))
	if sentimentRef == "" && version.SentimentURI != nil {
		sentimentRef = strings.TrimSpace(*version.SentimentURI)
	}
	if version.SentimentStatus != "ready" || sentimentRef == "" {
		return "", "", ErrInvalidArgument{Message: "sentiment artifact is not ready"}
	}
	sentimentFormat := strings.TrimSpace(metadataString(version.Metadata, "sentiment_format", ""))
	if sentimentFormat == "" {
		sentimentFormat = inferArtifactFormat(sentimentRef, "parquet")
	}
	return sentimentRef, sentimentFormat, nil
}

func deriveClusterMembershipURI(summaryURI string) string {
	summaryURI = strings.TrimSpace(summaryURI)
	if summaryURI == "" {
		return ""
	}
	if strings.HasSuffix(summaryURI, ".json") {
		return strings.TrimSuffix(summaryURI, ".json") + ".memberships.parquet"
	}
	return summaryURI + ".memberships.parquet"
}

func artifactString(artifact map[string]any, key string) string {
	if artifact == nil {
		return ""
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func artifactMap(artifact map[string]any, key string) map[string]any {
	if artifact == nil {
		return nil
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return nil
	}
	typed, ok := value.(map[string]any)
	if !ok || len(typed) == 0 {
		return nil
	}
	return typed
}

func artifactBoolMap(artifact map[string]any, key string) map[string]bool {
	return boolMapValue(artifactMap(artifact, key))
}

type llmFallbackInfo struct {
	Fallback bool
	Reason   string
	Count    int
	Provider string
	Model    string
}

func clearLLMFallbackMetadata(metadata map[string]any, prefix string) {
	if metadata == nil {
		return
	}
	delete(metadata, prefix+"_llm_fallback")
	delete(metadata, prefix+"_llm_fallback_reason")
	delete(metadata, prefix+"_llm_fallback_count")
	delete(metadata, prefix+"_llm_fallback_reasons")
	delete(metadata, prefix+"_llm_provider")
	delete(metadata, prefix+"_llm_model")
	delete(metadata, prefix+"_warning")
}

func applyLLMFallbackMetadata(metadata map[string]any, prefix string, artifact map[string]any) llmFallbackInfo {
	info := llmFallbackInfo{
		Fallback: artifactBool(artifact, "llm_fallback"),
		Reason:   artifactString(artifact, "llm_fallback_reason"),
		Provider: artifactString(artifact, "llm_provider"),
		Model:    artifactString(artifact, "llm_model"),
	}
	if count, ok := artifactInt(artifact, "llm_fallback_count"); ok {
		info.Count = count
	}
	if !info.Fallback {
		return info
	}
	metadata[prefix+"_llm_fallback"] = true
	metadata[prefix+"_llm_fallback_count"] = info.Count
	if info.Reason != "" {
		metadata[prefix+"_llm_fallback_reason"] = info.Reason
		metadata[prefix+"_warning"] = fmt.Sprintf("%s llm fallback used: %s", prefix, info.Reason)
	} else {
		metadata[prefix+"_warning"] = fmt.Sprintf("%s llm fallback used", prefix)
	}
	if reasons := metadataStringList(artifact, "llm_fallback_reasons"); len(reasons) > 0 {
		metadata[prefix+"_llm_fallback_reasons"] = reasons
	}
	if info.Provider != "" {
		metadata[prefix+"_llm_provider"] = info.Provider
	}
	if info.Model != "" {
		metadata[prefix+"_llm_model"] = info.Model
	}
	return info
}

func artifactBool(artifact map[string]any, key string) bool {
	if artifact == nil {
		return false
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func artifactInt(artifact map[string]any, key string) (int, bool) {
	if artifact == nil {
		return 0, false
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return 0, false
	}
	return anyToInt(value)
}

func normalizeOptionalPositiveInt(value *int, fieldName string) (int, error) {
	if value == nil {
		return 0, nil
	}
	normalized := *value
	if normalized <= 0 {
		return 0, ErrInvalidArgument{Message: fmt.Sprintf("%s must be a positive integer", fieldName)}
	}
	return normalized, nil
}

func inferArtifactFormat(path string, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(normalized, ".parquet"):
		return "parquet"
	case strings.HasSuffix(normalized, ".jsonl"):
		return "jsonl"
	default:
		return fallback
	}
}

func resolveReadableArtifactRef(candidates ...string) string {
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		if _, err := os.Stat(trimmed); err == nil {
			return trimmed
		}
	}
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func resolveReadableEmbeddingRef(primary string, fallback string, versionURI *string) string {
	return resolveReadableArtifactRef(primary, fallback, derefString(versionURI))
}

func (s *DatasetService) syncEmbeddingIndex(version domain.DatasetVersion, embeddingRef string, chunkRef string) error {
	indexer, ok := s.store.(store.EmbeddingChunkIndexer)
	if !ok {
		return nil
	}
	embeddingRef = strings.TrimSpace(embeddingRef)
	if embeddingRef == "" {
		return nil
	}
	records, err := loadEmbeddingIndexChunks(version.DatasetVersionID, embeddingRef, chunkRef, derefString(version.EmbeddingModel))
	if err != nil {
		return err
	}
	if err := indexer.ReplaceEmbeddingChunkIndex(version.DatasetVersionID, records); err != nil {
		return err
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["embedding_index_backend"] = "pgvector"
	version.Metadata["embedding_index_ref"] = fmt.Sprintf("pgvector://embedding_index_chunks?dataset_version_id=%s", version.DatasetVersionID)
	vectorDim := 0
	if len(records) > 0 {
		vectorDim = records[0].VectorDim
	}
	version.Metadata["embedding_vector_dim"] = vectorDim
	version.Metadata["embedding_indexed_chunk_count"] = len(records)
	return nil
}

func anyToInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func intValueOrZero(value any) int {
	if converted, ok := anyToInt(value); ok {
		return converted
	}
	return 0
}

func intMapValue(value any) map[string]int {
	source, ok := value.(map[string]any)
	if !ok || len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, item := range source {
		if count, ok := anyToInt(item); ok {
			result[key] = count
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func boolMapValue(value any) map[string]bool {
	if source, ok := value.(map[string]bool); ok {
		return cloneStringBoolMap(source)
	}
	source, ok := value.(map[string]any)
	if !ok || len(source) == 0 {
		return nil
	}
	result := make(map[string]bool, len(source))
	for key, item := range source {
		result[key] = anyBoolValue(item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func anyBoolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.TrimSpace(strings.ToLower(typed))
		return normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "y"
	case int:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	default:
		return false
	}
}

func cloneStringIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneStringBoolMap(source map[string]bool) map[string]bool {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]bool, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func mergeStringAny(base, overlay map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func metadataNestedString(metadata map[string]any, key, field string) string {
	if metadata == nil {
		return ""
	}
	nested, ok := metadata[key].(map[string]any)
	if !ok {
		return ""
	}
	value, ok := nested[field]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(strings.TrimSpace(path))
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func sanitizeFilename(value string) string {
	trimmed := strings.TrimSpace(filepath.Base(value))
	if trimmed == "" || trimmed == "." || trimmed == string(filepath.Separator) {
		return ""
	}
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, trimmed)
	sanitized = strings.Trim(sanitized, "._")
	if sanitized == "" {
		return ""
	}
	return sanitized
}
