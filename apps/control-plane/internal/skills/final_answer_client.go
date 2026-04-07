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
)

type FinalAnswerRequest struct {
	ExecutionID string
	ProjectID   string
	Question    string
	Context     map[string]any
	ResultV1    domain.ExecutionResultV1
}

type FinalAnswerGenerator interface {
	Generate(ctx context.Context, input FinalAnswerRequest) (domain.ExecutionFinalAnswer, []string, error)
}

type PythonAIFinalAnswerClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type pythonAIFinalAnswerPayload struct {
	ExecutionID string                   `json:"execution_id"`
	ProjectID   string                   `json:"project_id"`
	Question    string                   `json:"question"`
	Context     map[string]any           `json:"context,omitempty"`
	ResultV1    domain.ExecutionResultV1 `json:"result_v1"`
}

type pythonAIFinalAnswerResponse struct {
	Notes  []string                    `json:"notes"`
	Answer domain.ExecutionFinalAnswer `json:"answer"`
}

func (c PythonAIFinalAnswerClient) Generate(ctx context.Context, input FinalAnswerRequest) (domain.ExecutionFinalAnswer, []string, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return domain.ExecutionFinalAnswer{}, nil, errors.New("python ai worker url is required")
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	payload, err := json.Marshal(pythonAIFinalAnswerPayload{
		ExecutionID: input.ExecutionID,
		ProjectID:   input.ProjectID,
		Question:    strings.TrimSpace(input.Question),
		Context:     input.Context,
		ResultV1:    input.ResultV1,
	})
	if err != nil {
		return domain.ExecutionFinalAnswer{}, nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/tasks/execution_final_answer",
		bytes.NewReader(payload),
	)
	if err != nil {
		return domain.ExecutionFinalAnswer{}, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return domain.ExecutionFinalAnswer{}, nil, err
	}
	defer resp.Body.Close()

	var decoded pythonAIFinalAnswerResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return domain.ExecutionFinalAnswer{}, nil, err
	}
	if resp.StatusCode >= 300 {
		return domain.ExecutionFinalAnswer{}, nil, fmt.Errorf("python ai worker returned %d", resp.StatusCode)
	}
	return decoded.Answer, decoded.Notes, nil
}
