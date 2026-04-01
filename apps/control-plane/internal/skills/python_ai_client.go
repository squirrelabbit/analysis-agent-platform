package skills

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

type PythonAIClient struct {
	BaseURL    string
	HTTPClient *http.Client
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
		for key, value := range result.Artifacts {
			priorArtifacts[key] = json.RawMessage(value)
		}

		payload, err := json.Marshal(pythonAIStepRequest{
			ExecutionID:    execution.ExecutionID,
			ProjectID:      execution.ProjectID,
			DatasetVersion: datasetVersionID,
			Step:           step,
			PriorArtifacts: priorArtifacts,
		})
		if err != nil {
			return ExecutionRunResult{}, err
		}

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			baseURL+taskPath,
			bytes.NewReader(payload),
		)
		if err != nil {
			return ExecutionRunResult{}, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return ExecutionRunResult{}, err
		}

		var taskResponse pythonAITaskResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&taskResponse)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			return ExecutionRunResult{}, decodeErr
		}
		if closeErr != nil {
			return ExecutionRunResult{}, closeErr
		}
		if resp.StatusCode >= 300 {
			return ExecutionRunResult{}, fmt.Errorf("python ai worker returned %d", resp.StatusCode)
		}

		artifactJSON, err := json.Marshal(taskResponse.Artifact)
		if err != nil {
			return ExecutionRunResult{}, err
		}
		result.Artifacts[artifactKey(step)] = string(artifactJSON)
		result.Notes = append(result.Notes, taskResponse.Notes...)
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
