package domain

import (
	"fmt"
	"strings"
)

const (
	DatasetSourceStageRaw       = "raw"
	DatasetSourceStageClean     = "clean"
	DatasetSourceStagePrepared  = "prepared"
	DatasetSourceStageSentiment = "sentiment"
)

type DatasetResolvedSource struct {
	DatasetName string
	TextColumn  string
	TextColumns []string
	Stage       string
}

func ResolveDatasetSource(version DatasetVersion) DatasetResolvedSource {
	// silverone 2026-05-28 (β2 cleanup PR2) — prepare stage 분기 제거.
	if ref := cleanedDatasetRef(version); ref != "" {
		textColumn := metadataStringValue(version.Metadata, "cleaned_text_column", "cleaned_text")
		return DatasetResolvedSource{
			DatasetName: ref,
			TextColumn:  textColumn,
			TextColumns: []string{textColumn},
			Stage:       DatasetSourceStageClean,
		}
	}
	return ResolveRawDatasetSource(version)
}

func ResolveRawDatasetSource(version DatasetVersion) DatasetResolvedSource {
	textColumns := metadataStringListValue(version.Metadata, "raw_text_columns")
	if len(textColumns) == 0 {
		textColumns = metadataStringListValue(version.Metadata, "text_columns")
	}
	textColumn := metadataStringValue(version.Metadata, "raw_text_column", "")
	if textColumn == "" {
		textColumn = metadataStringValue(version.Metadata, "text_column", "")
	}
	if textColumn == "" && len(textColumns) == 1 {
		textColumn = textColumns[0]
	}
	if textColumn == "" {
		textColumn = "text"
	}
	if len(textColumns) == 0 {
		textColumns = []string{textColumn}
	}
	return DatasetResolvedSource{
		DatasetName: strings.TrimSpace(version.StorageURI),
		TextColumn:  textColumn,
		TextColumns: append([]string(nil), textColumns...),
		Stage:       DatasetSourceStageRaw,
	}
}

// silverone 2026-05-28 (β2 cleanup PR2) — ResolveSentimentDatasetSource 제거.
// β2로 sentiment stage 자체가 사라져 호출처 없음.

func DatasetSourceIsRawTextColumn(version DatasetVersion, column string) bool {
	column = strings.TrimSpace(column)
	if column == "" {
		return false
	}
	rawSource := ResolveRawDatasetSource(version)
	if column == rawSource.TextColumn {
		return true
	}
	if column == "text" && rawSource.TextColumn != "text" {
		return true
	}
	for _, value := range rawSource.TextColumns {
		if column == value {
			return true
		}
	}
	return false
}

// silverone 2026-05-28 (β2 cleanup PR2) — preparedDatasetRef / sentimentDatasetRef
// 제거. ADR-018 β2로 prepare/sentiment 단계 자체가 없어진 후 호출처 0.

func cleanedDatasetRef(version DatasetVersion) string {
	status := metadataStringValue(version.Metadata, "clean_status", strings.TrimSpace(version.CleanStatus))
	if status != "ready" {
		return ""
	}
	if version.CleanURI != nil && strings.TrimSpace(*version.CleanURI) != "" {
		return strings.TrimSpace(*version.CleanURI)
	}
	if version.CleanedRef != nil && strings.TrimSpace(*version.CleanedRef) != "" {
		return strings.TrimSpace(*version.CleanedRef)
	}
	if ref := metadataStringValue(version.Metadata, "clean_uri", ""); ref != "" {
		return ref
	}
	return metadataStringValue(version.Metadata, "cleaned_ref", "")
}

func metadataStringValue(metadata map[string]any, key, fallback string) string {
	if metadata == nil {
		return fallback
	}
	value, ok := metadata[key]
	if !ok {
		return fallback
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" {
		return fallback
	}
	return text
}

func metadataStringListValue(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	value, ok := metadata[key]
	if !ok {
		return nil
	}
	return normalizeMetadataStringList(value)
}

func normalizeMetadataStringList(value any) []string {
	var values []string
	switch typed := value.(type) {
	case []string:
		values = append(values, typed...)
	case []any:
		for _, item := range typed {
			values = append(values, strings.TrimSpace(fmt.Sprintf("%v", item)))
		}
	case string:
		values = append(values, typed)
	default:
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
