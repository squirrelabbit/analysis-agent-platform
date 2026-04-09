package skills

import (
	"encoding/json"
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func compactExecutionArtifactForStorage(step domain.SkillPlanStep, storedArtifact string) (string, error) {
	trimmed := strings.TrimSpace(storedArtifact)
	if trimmed == "" {
		return storedArtifact, nil
	}
	var artifact map[string]any
	if err := json.Unmarshal([]byte(trimmed), &artifact); err != nil {
		return storedArtifact, nil
	}
	compacted := compactExecutionArtifactMap(step, artifact)
	payload, err := json.Marshal(compacted)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func compactExecutionArtifactMap(step domain.SkillPlanStep, artifact map[string]any) map[string]any {
	if len(artifact) == 0 {
		return artifact
	}

	ref := strings.TrimSpace(stringValue(artifact["artifact_ref"]))
	if ref != "" {
		if compacted := compactSidecarArtifact(step, artifact, ref); compacted != nil {
			return compacted
		}
	}

	compacted := map[string]any{}
	copyArtifactValue(compacted, artifact, "skill_name")
	copyArtifactValue(compacted, artifact, "step_id")
	copyArtifactValue(compacted, artifact, "dataset_name")
	copyArtifactValue(compacted, artifact, "artifact_storage_mode")
	copyArtifactValue(compacted, artifact, "artifact_ref")
	copyArtifactValue(compacted, artifact, "artifact_format")
	copyArtifactValue(compacted, artifact, "summary")
	copyArtifactValue(compacted, artifact, "usage")
	copyArtifactValue(compacted, artifact, "selection_source")
	copyArtifactValue(compacted, artifact, "citation_mode")
	copyArtifactValue(compacted, artifact, "query")
	copyArtifactValue(compacted, artifact, "match_mode")
	copyArtifactValue(compacted, artifact, "bucket")
	copyArtifactValue(compacted, artifact, "time_column")
	copyArtifactValue(compacted, artifact, "dimension_column")
	copyArtifactValue(compacted, artifact, "sentiment_column")
	copyArtifactValue(compacted, artifact, "prepared_dataset_name")
	copyArtifactValue(compacted, artifact, "embedding_uri")
	copyArtifactValue(compacted, artifact, "embedding_index_ref")
	copyArtifactValue(compacted, artifact, "embedding_source_backend")
	copyArtifactValue(compacted, artifact, "embedding_model")
	copyArtifactValue(compacted, artifact, "cluster_ref")
	copyArtifactValue(compacted, artifact, "cluster_format")
	copyArtifactValue(compacted, artifact, "cluster_summary_ref")
	copyArtifactValue(compacted, artifact, "cluster_summary_format")
	copyArtifactValue(compacted, artifact, "cluster_membership_ref")
	copyArtifactValue(compacted, artifact, "cluster_membership_format")
	copyArtifactValue(compacted, artifact, "cluster_execution_mode")
	copyArtifactValue(compacted, artifact, "cluster_materialization_scope")
	copyArtifactValue(compacted, artifact, "cluster_materialized_ref_used")
	copyArtifactValue(compacted, artifact, "cluster_fallback_reason")
	copyArtifactValue(compacted, artifact, "cluster_algorithm")
	copyArtifactValue(compacted, artifact, "chunk_ref")
	copyArtifactValue(compacted, artifact, "chunk_format")
	copyArtifactValue(compacted, artifact, "language")

	copyCompactedSlice(compacted, artifact, "warnings", 5)
	copyCompactedSlice(compacted, artifact, "key_findings", 5)
	copyCompactedSlice(compacted, artifact, "top_terms", 10)
	copyCompactedSlice(compacted, artifact, "top_keywords", 10)
	copyCompactedSlice(compacted, artifact, "follow_up_questions", 5)
	copyCompactedSlice(compacted, artifact, "matches", 3)
	copyCompactedSlice(compacted, artifact, "evidence", 3)
	copyCompactedSlice(compacted, artifact, "clusters", 5)
	copyCompactedSlice(compacted, artifact, "breakdown", 5)
	copyCompactedSlice(compacted, artifact, "taxonomy_breakdown", 5)
	copyCompactedSlice(compacted, artifact, "samples", 3)
	copyCompactedSlice(compacted, artifact, "sample_documents", 3)
	copyCompactedSlice(compacted, artifact, "sample_rows", 3)
	copyCompactedSlice(compacted, artifact, "duplicate_records", 5)
	copyCompactedSlice(compacted, artifact, "duplicate_groups_preview", 3)
	copyCompactedSlice(compacted, artifact, "series", 10)

	if periods, ok := artifact["periods"].(map[string]any); ok && len(periods) > 0 {
		compactedPeriods := map[string]any{}
		for key, value := range periods {
			periodMap, ok := value.(map[string]any)
			if !ok {
				continue
			}
			periodCopy := map[string]any{}
			copyArtifactValue(periodCopy, periodMap, "start_bucket")
			copyArtifactValue(periodCopy, periodMap, "end_bucket")
			copyArtifactValue(periodCopy, periodMap, "document_count")
			copyCompactedSlice(periodCopy, periodMap, "samples", 3)
			copyCompactedSlice(periodCopy, periodMap, "top_terms", 5)
			if len(periodCopy) > 0 {
				compactedPeriods[key] = periodCopy
			}
		}
		if len(compactedPeriods) > 0 {
			compacted["periods"] = compactedPeriods
		}
	}

	return compacted
}

func compactSidecarArtifact(step domain.SkillPlanStep, artifact map[string]any, ref string) map[string]any {
	switch step.SkillName {
	case "garbage_filter":
		return map[string]any{
			"skill_name":            artifact["skill_name"],
			"step_id":               artifact["step_id"],
			"dataset_name":          artifact["dataset_name"],
			"garbage_rule_names":    artifact["garbage_rule_names"],
			"artifact_storage_mode": artifact["artifact_storage_mode"],
			"artifact_ref":          ref,
			"artifact_format":       artifact["artifact_format"],
			"row_id_column":         artifact["row_id_column"],
			"source_index_column":   artifact["source_index_column"],
			"status_column":         artifact["status_column"],
			"matched_rules_column":  artifact["matched_rules_column"],
			"summary":               artifact["summary"],
			"removed_samples":       artifact["removed_samples"],
		}
	case "document_filter":
		return map[string]any{
			"skill_name":            artifact["skill_name"],
			"step_id":               artifact["step_id"],
			"dataset_name":          artifact["dataset_name"],
			"query":                 artifact["query"],
			"match_mode":            artifact["match_mode"],
			"artifact_storage_mode": artifact["artifact_storage_mode"],
			"artifact_ref":          ref,
			"artifact_format":       artifact["artifact_format"],
			"row_id_column":         artifact["row_id_column"],
			"source_index_column":   artifact["source_index_column"],
			"rank_column":           artifact["rank_column"],
			"score_column":          artifact["score_column"],
			"summary":               artifact["summary"],
			"matches":               artifact["matches"],
		}
	case "deduplicate_documents":
		return map[string]any{
			"skill_name":                    artifact["skill_name"],
			"step_id":                       artifact["step_id"],
			"dataset_name":                  artifact["dataset_name"],
			"artifact_storage_mode":         artifact["artifact_storage_mode"],
			"artifact_ref":                  ref,
			"artifact_format":               artifact["artifact_format"],
			"row_id_column":                 artifact["row_id_column"],
			"source_index_column":           artifact["source_index_column"],
			"canonical_row_id_column":       artifact["canonical_row_id_column"],
			"canonical_source_index_column": artifact["canonical_source_index_column"],
			"group_id_column":               artifact["group_id_column"],
			"status_column":                 artifact["status_column"],
			"similarity_column":             artifact["similarity_column"],
			"member_count_column":           artifact["member_count_column"],
			"summary":                       artifact["summary"],
			"duplicate_records":             artifact["duplicate_records"],
			"duplicate_groups_preview":      compactDuplicateGroupsPreview(artifact["duplicate_groups"]),
		}
	case "sentence_split":
		return map[string]any{
			"skill_name":            artifact["skill_name"],
			"step_id":               artifact["step_id"],
			"dataset_name":          artifact["dataset_name"],
			"language":              artifact["language"],
			"artifact_storage_mode": artifact["artifact_storage_mode"],
			"artifact_ref":          ref,
			"artifact_format":       artifact["artifact_format"],
			"row_id_column":         artifact["row_id_column"],
			"source_index_column":   artifact["source_index_column"],
			"sentence_index_column": artifact["sentence_index_column"],
			"sentence_text_column":  artifact["sentence_text_column"],
			"char_start_column":     artifact["char_start_column"],
			"char_end_column":       artifact["char_end_column"],
			"summary":               artifact["summary"],
			"sample_documents":      artifact["sample_documents"],
		}
	default:
		return nil
	}
}

func copyArtifactValue(target map[string]any, source map[string]any, key string) {
	if target == nil || source == nil {
		return
	}
	value, ok := source[key]
	if !ok || value == nil {
		return
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return
		}
	case []any:
		if len(typed) == 0 {
			return
		}
	case []string:
		if len(typed) == 0 {
			return
		}
	case map[string]any:
		if len(typed) == 0 {
			return
		}
	}
	target[key] = value
}

func copyCompactedSlice(target map[string]any, source map[string]any, key string, limit int) {
	if target == nil || source == nil {
		return
	}
	value, ok := source[key]
	if !ok || value == nil {
		return
	}
	switch typed := value.(type) {
	case []any:
		if len(typed) == 0 {
			return
		}
		target[key] = compactAnySlice(typed, limit)
	case []map[string]any:
		if len(typed) == 0 {
			return
		}
		items := make([]any, 0, len(typed))
		for _, item := range typed {
			items = append(items, compactAnyMap(item))
		}
		target[key] = compactAnySlice(items, limit)
	case []string:
		if len(typed) == 0 {
			return
		}
		items := make([]any, 0, len(typed))
		for _, item := range typed {
			items = append(items, item)
		}
		target[key] = compactAnySlice(items, limit)
	}
}

func compactAnySlice(items []any, limit int) []any {
	if limit <= 0 || len(items) <= limit {
		limit = len(items)
	}
	compacted := make([]any, 0, limit)
	for _, item := range items[:limit] {
		switch typed := item.(type) {
		case map[string]any:
			compacted = append(compacted, compactAnyMap(typed))
		case []any:
			compacted = append(compacted, compactAnySlice(typed, 3))
		default:
			compacted = append(compacted, item)
		}
	}
	return compacted
}

func compactAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return input
	}
	result := map[string]any{}
	for key, value := range input {
		switch key {
		case "members", "token_counts", "embedding":
			continue
		}
		switch typed := value.(type) {
		case []any:
			result[key] = compactAnySlice(typed, 3)
		case map[string]any:
			result[key] = compactAnyMap(typed)
		case string:
			result[key] = compactPreviewString(typed, key)
		default:
			result[key] = value
		}
	}
	return result
}

func compactPreviewString(value string, key string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	maxLen := 240
	if strings.Contains(strings.ToLower(key), "summary") {
		maxLen = 400
	}
	if len([]rune(trimmed)) <= maxLen {
		return trimmed
	}
	runes := []rune(trimmed)
	return fmt.Sprintf("%s...", string(runes[:maxLen]))
}
