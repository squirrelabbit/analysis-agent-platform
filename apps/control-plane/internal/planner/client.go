package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/domain"
)

type Planner interface {
	GeneratePlan(ctx context.Context, input domain.AnalysisSubmitRequest) (PlanGenerationResult, error)
}

type PlanGenerationResult struct {
	Plan                 domain.SkillPlan `json:"plan"`
	PlannerType          string           `json:"planner_type"`
	PlannerModel         *string          `json:"planner_model,omitempty"`
	PlannerPromptVersion *string          `json:"planner_prompt_version,omitempty"`
}

type PythonAIPlanner struct {
	BaseURL    string
	HTTPClient *http.Client
}

type planRequest struct {
	DatasetName      *string        `json:"dataset_name,omitempty"`
	DatasetVersionID *string        `json:"dataset_version_id,omitempty"`
	DataType         *string        `json:"data_type,omitempty"`
	Goal             string         `json:"goal"`
	Constraints      []string       `json:"constraints,omitempty"`
	Context          map[string]any `json:"context,omitempty"`
}

type planResponse struct {
	Plan                 domain.SkillPlan `json:"plan"`
	PlannerType          string           `json:"planner_type"`
	PlannerModel         *string          `json:"planner_model,omitempty"`
	PlannerPromptVersion *string          `json:"planner_prompt_version,omitempty"`
}

func New(cfg config.Config) (Planner, error) {
	switch cfg.PlannerBackend {
	case "", "stub":
		return nil, nil
	case "python-ai":
		return PythonAIPlanner{
			BaseURL: cfg.PythonAIWorkerURL,
		}, nil
	default:
		return nil, errors.New("unsupported planner backend: " + cfg.PlannerBackend)
	}
}

func (p PythonAIPlanner) GeneratePlan(ctx context.Context, input domain.AnalysisSubmitRequest) (PlanGenerationResult, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(p.BaseURL), "/")
	if baseURL == "" {
		return PlanGenerationResult{}, errors.New("python ai worker url is required")
	}

	httpClient := p.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	payload, err := json.Marshal(planRequest{
		DatasetName:      input.DatasetName,
		DatasetVersionID: input.DatasetVersionID,
		DataType:         input.DataType,
		Goal:             input.Goal,
		Constraints:      input.Constraints,
		Context:          input.Context,
	})
	if err != nil {
		return PlanGenerationResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/tasks/planner", bytes.NewReader(payload))
	if err != nil {
		return PlanGenerationResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return PlanGenerationResult{}, err
	}
	defer resp.Body.Close()

	var decoded planResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return PlanGenerationResult{}, err
	}
	if resp.StatusCode >= 300 {
		return PlanGenerationResult{}, fmt.Errorf("python ai planner returned %d", resp.StatusCode)
	}

	result := PlanGenerationResult{
		Plan:                 decoded.Plan,
		PlannerType:          decoded.PlannerType,
		PlannerModel:         decoded.PlannerModel,
		PlannerPromptVersion: decoded.PlannerPromptVersion,
	}
	if strings.TrimSpace(result.PlannerType) == "" {
		result.PlannerType = "python-ai"
	}
	return result, nil
}
