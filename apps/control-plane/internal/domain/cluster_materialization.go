package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DefaultClusterSimilarityThreshold = 0.3
	DefaultClusterTopN                = 10
	DefaultClusterSampleN             = 3
)

func ClusterBuildRequestFromStep(step SkillPlanStep) DatasetClusterBuildRequest {
	inputs := step.Inputs
	threshold := DefaultClusterSimilarityThreshold
	if value, ok := anyToFloat64(inputs["cluster_similarity_threshold"]); ok {
		if value < 0 {
			value = 0
		}
		if value > 1 {
			value = 1
		}
		threshold = value
	}
	topN := DefaultClusterTopN
	if value, ok := anyToInt(inputs["top_n"]); ok && value > 0 {
		topN = value
	}
	sampleN := DefaultClusterSampleN
	if value, ok := anyToInt(inputs["sample_n"]); ok && value > 0 {
		sampleN = value
	}
	return DatasetClusterBuildRequest{
		SimilarityThreshold: float64Ptr(roundFloat64(threshold, 4)),
		TopN:                intPtr(topN),
		SampleN:             intPtr(sampleN),
	}
}

func ClusterMaterializationRequestForPlan(plan SkillPlan) (*DatasetClusterBuildRequest, bool) {
	seenExecutableBeforeCluster := false
	var normalized *DatasetClusterBuildRequest
	for _, step := range plan.Steps {
		if strings.TrimSpace(step.SkillName) != "embedding_cluster" {
			if strings.TrimSpace(step.SkillName) != "" {
				seenExecutableBeforeCluster = true
			}
			continue
		}
		if seenExecutableBeforeCluster {
			return nil, false
		}
		request := NormalizeClusterBuildRequest(ClusterBuildRequestFromStep(step))
		if normalized == nil {
			copy := request
			normalized = &copy
			continue
		}
		if !clusterRequestsEqual(*normalized, request) {
			return nil, false
		}
	}
	if normalized == nil {
		return nil, false
	}
	return normalized, true
}

func NormalizeClusterBuildRequest(input DatasetClusterBuildRequest) DatasetClusterBuildRequest {
	threshold := DefaultClusterSimilarityThreshold
	if input.SimilarityThreshold != nil {
		value := *input.SimilarityThreshold
		if value < 0 {
			value = 0
		}
		if value > 1 {
			value = 1
		}
		threshold = value
	}
	topN := DefaultClusterTopN
	if input.TopN != nil && *input.TopN > 0 {
		topN = *input.TopN
	}
	sampleN := DefaultClusterSampleN
	if input.SampleN != nil && *input.SampleN > 0 {
		sampleN = *input.SampleN
	}
	return DatasetClusterBuildRequest{
		EmbeddingIndexSourceRef: trimStringPtr(input.EmbeddingIndexSourceRef),
		ChunkRef:                trimStringPtr(input.ChunkRef),
		OutputPath:              trimStringPtr(input.OutputPath),
		SimilarityThreshold:     float64Ptr(roundFloat64(threshold, 4)),
		TopN:                    intPtr(topN),
		SampleN:                 intPtr(sampleN),
		Force:                   input.Force,
	}
}

func ClusterRequestMatchesMetadata(input DatasetClusterBuildRequest, metadata map[string]any) bool {
	normalized := NormalizeClusterBuildRequest(input)
	if strings.TrimSpace(anyString(metadata["cluster_status"])) != "ready" {
		return false
	}
	if strings.TrimSpace(anyString(metadata["cluster_ref"])) == "" {
		return false
	}
	hash := strings.TrimSpace(anyString(metadata["cluster_params_hash"]))
	if hash != "" {
		return hash == ClusterRequestHash(normalized)
	}
	metadataRequest := NormalizeClusterBuildRequest(DatasetClusterBuildRequest{
		SimilarityThreshold: metadataFloatPtr(metadata["cluster_similarity_threshold"]),
		TopN:                metadataIntPtr(metadata["cluster_top_n"]),
		SampleN:             metadataIntPtr(metadata["cluster_sample_n"]),
	})
	return clusterRequestsEqual(normalized, metadataRequest)
}

func ClusterRequestHash(input DatasetClusterBuildRequest) string {
	normalized := NormalizeClusterBuildRequest(input)
	payload, err := json.Marshal(map[string]any{
		"cluster_similarity_threshold": valueOrDefaultFloat(normalized.SimilarityThreshold, DefaultClusterSimilarityThreshold),
		"top_n":                        valueOrDefaultInt(normalized.TopN, DefaultClusterTopN),
		"sample_n":                     valueOrDefaultInt(normalized.SampleN, DefaultClusterSampleN),
	})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func clusterRequestsEqual(left, right DatasetClusterBuildRequest) bool {
	l := NormalizeClusterBuildRequest(left)
	r := NormalizeClusterBuildRequest(right)
	return valueOrDefaultFloat(l.SimilarityThreshold, DefaultClusterSimilarityThreshold) == valueOrDefaultFloat(r.SimilarityThreshold, DefaultClusterSimilarityThreshold) &&
		valueOrDefaultInt(l.TopN, DefaultClusterTopN) == valueOrDefaultInt(r.TopN, DefaultClusterTopN) &&
		valueOrDefaultInt(l.SampleN, DefaultClusterSampleN) == valueOrDefaultInt(r.SampleN, DefaultClusterSampleN)
}

func trimStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func valueOrDefaultFloat(value *float64, fallback float64) float64 {
	if value == nil {
		return fallback
	}
	return *value
}

func valueOrDefaultInt(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func float64Ptr(value float64) *float64 {
	return &value
}

func intPtr(value int) *int {
	return &value
}

func anyString(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func anyToFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
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

func metadataFloatPtr(value any) *float64 {
	typed, ok := anyToFloat64(value)
	if !ok {
		return nil
	}
	rounded := roundFloat64(typed, 4)
	return &rounded
}

func metadataIntPtr(value any) *int {
	typed, ok := anyToInt(value)
	if !ok {
		return nil
	}
	return &typed
}

func roundFloat64(value float64, scale int) float64 {
	if scale <= 0 {
		return value
	}
	factor := 1.0
	for i := 0; i < scale; i++ {
		factor *= 10
	}
	return float64(int(value*factor+0.5)) / factor
}
