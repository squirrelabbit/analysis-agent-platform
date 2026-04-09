package skills

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

type PythonAIClient struct {
	BaseURL      string
	HTTPClient   *http.Client
	ArtifactRoot string
	Hooks        []StepHook
}

type pythonAIStepRequest struct {
	ExecutionID    string                     `json:"execution_id"`
	ProjectID      string                     `json:"project_id"`
	DatasetVersion string                     `json:"dataset_version_id,omitempty"`
	Step           domain.SkillPlanStep       `json:"step"`
	PriorArtifacts map[string]json.RawMessage `json:"prior_artifacts,omitempty"`
}

type pythonAITaskResponse struct {
	Notes    []string       `json:"notes"`
	Artifact map[string]any `json:"artifact"`
	Usage    map[string]any `json:"usage,omitempty"`
}

func (c PythonAIClient) Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return ExecutionRunResult{}, errors.New("python ai worker url is required")
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	result := ExecutionRunResult{
		Artifacts: map[string]string{},
		Notes:     []string{},
		Engine:    "python-ai",
	}
	runtimeArtifacts := map[string]json.RawMessage{}
	datasetVersionID := ""
	if execution.DatasetVersionID != nil {
		datasetVersionID = strings.TrimSpace(*execution.DatasetVersionID)
	}

	for _, step := range execution.Plan.Steps {
		taskPath, ok := pythonAITaskPath(step.SkillName)
		if !ok {
			result.Notes = append(result.Notes, fmt.Sprintf("unsupported skill skipped: %s", step.SkillName))
			continue
		}

		priorArtifacts := map[string]json.RawMessage{}
		for key, value := range runtimeArtifacts {
			priorArtifacts[key] = value
		}
		requestStep := c.prepareStep(execution, step)
		beforeRecords, err := executeBeforeStepHooks(ctx, c.Hooks, execution, requestStep)
		if err != nil {
			return ExecutionRunResult{}, err
		}
		result.StepHooks = append(result.StepHooks, beforeRecords...)

		payload, err := json.Marshal(pythonAIStepRequest{
			ExecutionID:    execution.ExecutionID,
			ProjectID:      execution.ProjectID,
			DatasetVersion: datasetVersionID,
			Step:           requestStep,
			PriorArtifacts: priorArtifacts,
		})
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			baseURL+taskPath,
			bytes.NewReader(payload),
		)
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}

		var taskResponse pythonAITaskResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&taskResponse)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, decodeErr)
			return ExecutionRunResult{}, decodeErr
		}
		if closeErr != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, closeErr)
			return ExecutionRunResult{}, closeErr
		}
		if resp.StatusCode >= 300 {
			err = fmt.Errorf("python ai worker returned %d", resp.StatusCode)
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}

		runtimeArtifact, err := compactPythonArtifactForRuntime(step, taskResponse.Artifact)
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}
		artifactJSON, err := json.Marshal(runtimeArtifact)
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}
		runtimeArtifacts[artifactKey(step)] = json.RawMessage(artifactJSON)
		storedArtifact, err := compactPythonArtifactForStorage(step, taskResponse.Artifact)
		if err != nil {
			result.StepHooks = appendFailedStepHooks(ctx, result.StepHooks, c.Hooks, execution, requestStep, err)
			return ExecutionRunResult{}, err
		}
		result.Artifacts[artifactKey(step)] = storedArtifact
		result.Notes = append(result.Notes, taskResponse.Notes...)
		result.UsageSummary = mergeUsageSummary(
			result.UsageSummary,
			extractUsageSummary(taskResponse),
		)
		afterRecords, err := executeAfterStepHooks(
			ctx,
			c.Hooks,
			execution,
			requestStep,
			StepHookOutcome{
				Status:         "completed",
				ArtifactBytes:  len(storedArtifact),
				ArtifactRef:    stringValue(taskResponse.Artifact["artifact_ref"]),
				UsageSummary:   extractUsageSummary(taskResponse),
				StoredArtifact: storedArtifact,
			},
		)
		if err != nil {
			return ExecutionRunResult{}, err
		}
		result.StepHooks = append(result.StepHooks, afterRecords...)
		result.ProcessedSteps++
	}

	if result.ProcessedSteps == 0 {
		result.Notes = append(result.Notes, "no unstructured skills were executed")
	}

	return result, nil
}

func pythonAITaskPath(skillName string) (string, bool) {
	definition, ok := registry.Skill(skillName)
	if !ok || strings.TrimSpace(definition.Engine) != "python-ai" || strings.TrimSpace(definition.TaskPath) == "" {
		return "", false
	}
	return strings.TrimSpace(definition.TaskPath), true
}

func (c PythonAIClient) prepareStep(execution domain.ExecutionSummary, step domain.SkillPlanStep) domain.SkillPlanStep {
	prepared := step
	prepared.Inputs = cloneInputs(step.Inputs)
	outputFileName, ok := sidecarOutputFileName(step.SkillName)
	if !ok {
		return prepared
	}
	if strings.TrimSpace(prepared.DatasetName) == "" {
		return prepared
	}
	if strings.TrimSpace(c.ArtifactRoot) == "" {
		return prepared
	}
	if existing := strings.TrimSpace(stringInput(prepared.Inputs, "artifact_output_path")); existing != "" {
		return prepared
	}
	prepared.Inputs["artifact_output_path"] = filepath.Join(
		c.ArtifactRoot,
		"projects",
		execution.ProjectID,
		"executions",
		execution.ExecutionID,
		"steps",
		fmt.Sprintf("%s.%s", safeSegment(step.StepID), outputFileName),
	)
	return prepared
}

func compactPythonArtifactForStorage(step domain.SkillPlanStep, artifact map[string]any) (string, error) {
	ref := strings.TrimSpace(stringValue(artifact["artifact_ref"]))
	if ref != "" {
		var compacted map[string]any
		switch step.SkillName {
		case "garbage_filter":
			compacted = map[string]any{
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
			compacted = map[string]any{
				"skill_name":            artifact["skill_name"],
				"step_id":               artifact["step_id"],
				"dataset_name":          artifact["dataset_name"],
				"query":                 artifact["query"],
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
			compacted = map[string]any{
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
			compacted = map[string]any{
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
		}
		if compacted != nil {
			payload, err := json.Marshal(compacted)
			if err != nil {
				return "", err
			}
			return string(payload), nil
		}
	}
	payload, err := json.Marshal(artifact)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func compactPythonArtifactForRuntime(step domain.SkillPlanStep, artifact map[string]any) (map[string]any, error) {
	ref := strings.TrimSpace(stringValue(artifact["artifact_ref"]))
	if ref == "" {
		return artifact, nil
	}
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
		}, nil
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
		}, nil
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
			"duplicate_groups":              compactDuplicateGroupsPreview(artifact["duplicate_groups"]),
		}, nil
	default:
		return artifact, nil
	}
}

func sidecarOutputFileName(skillName string) (string, bool) {
	switch strings.TrimSpace(skillName) {
	case "garbage_filter":
		return "garbage_filter.rows.parquet", true
	case "document_filter":
		return "document_filter.matches.parquet", true
	case "deduplicate_documents":
		return "deduplicate_documents.rows.parquet", true
	case "sentence_split":
		return "sentence_split.rows.parquet", true
	default:
		return "", false
	}
}

func extractUsageSummary(response pythonAITaskResponse) map[string]any {
	if len(response.Usage) > 0 {
		return response.Usage
	}
	if response.Artifact == nil {
		return nil
	}
	usage, ok := response.Artifact["usage"].(map[string]any)
	if !ok || len(usage) == 0 {
		return nil
	}
	return usage
}

func compactDuplicateGroupsPreview(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	preview := make([]map[string]any, 0, min(3, len(items)))
	for _, raw := range items {
		group, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		preview = append(preview, map[string]any{
			"group_id":               group["group_id"],
			"canonical_source_index": group["canonical_source_index"],
			"member_count":           group["member_count"],
			"samples":                group["samples"],
		})
		if len(preview) >= 3 {
			break
		}
	}
	return preview
}

func cloneInputs(inputs map[string]any) map[string]any {
	if len(inputs) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(inputs))
	for key, value := range inputs {
		cloned[key] = value
	}
	return cloned
}

func stringInput(inputs map[string]any, key string) string {
	if inputs == nil {
		return ""
	}
	return stringValue(inputs[key])
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%v", value)
	}
}

func safeSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	value = replacer.Replace(value)
	value = strings.ReplaceAll(value, "\n", "_")
	value = strings.ReplaceAll(value, "\r", "_")
	value = strings.ReplaceAll(value, "\t", "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "unknown"
	}
	return value
}
