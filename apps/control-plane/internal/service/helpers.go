package service

import (
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-21 δ-2: 옛 analysis.go가 보유하던 일반 helper들 중
// dataset_build / analyze 흐름이 계속 쓰는 것만 추출. analysis.go 삭제 후
// 의존이 끊긴 helper들이 여기 모인다.

func metadataString(metadata map[string]any, key, fallback string) string {
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

func anyStringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case *string:
		return optionalStringValue(typed)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringPointer(value string) *string {
	return &value
}

func latestDatasetBuildJobsByType(items []domain.DatasetBuildJob) map[string]domain.DatasetBuildJob {
	latest := make(map[string]domain.DatasetBuildJob)
	for _, item := range items {
		current, ok := latest[item.BuildType]
		if !ok || item.CreatedAt.After(current.CreatedAt) {
			latest[item.BuildType] = item
		}
	}
	return latest
}
