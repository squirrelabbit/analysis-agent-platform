package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	workerTaskTimeoutPrepare   = 60 * time.Minute
	workerTaskTimeoutSentiment = 30 * time.Minute
	workerTaskTimeoutEmbedding = 45 * time.Minute
)

type workerTaskResponse struct {
	Notes    []string       `json:"notes"`
	Artifact map[string]any `json:"artifact"`
}

func (s *DatasetService) runWorkerTask(ctx context.Context, taskPath string, payload map[string]any) (workerTaskResponse, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return workerTaskResponse{}, errors.New("python ai worker url is required")
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, workerTaskTimeout(taskPath))
	defer cancel()

	body, err := json.Marshal(payload)
	if err != nil {
		return workerTaskResponse{}, err
	}
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, baseURL+taskPath, bytes.NewReader(body))
	if err != nil {
		return workerTaskResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return workerTaskResponse{}, err
	}
	defer resp.Body.Close()

	var decoded workerTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return workerTaskResponse{}, err
	}
	if resp.StatusCode >= 300 {
		return workerTaskResponse{}, workerTaskHTTPError{
			TaskPath:   taskPath,
			StatusCode: resp.StatusCode,
		}
	}
	return decoded, nil
}

type workerTaskHTTPError struct {
	TaskPath   string
	StatusCode int
}

func (e workerTaskHTTPError) Error() string {
	return fmt.Sprintf("worker task %s returned %d", e.TaskPath, e.StatusCode)
}

func workerTaskTimeout(taskPath string) time.Duration {
	switch strings.TrimSpace(taskPath) {
	case "/tasks/dataset_clean":
		return 20 * time.Minute
	case "/tasks/dataset_prepare":
		return workerTaskTimeoutPrepare
	case "/tasks/sentiment_label":
		return workerTaskTimeoutSentiment
	case "/tasks/embedding":
		return workerTaskTimeoutEmbedding
	case "/tasks/dataset_cluster_build":
		return workerTaskTimeoutEmbedding
	default:
		return 2 * time.Minute
	}
}
