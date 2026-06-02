package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// GetPromptOptions는 task-folder prompt의 선택지(version/default/label)를 Python
// worker로 proxy해서 반환한다. Go control-plane은 prompt 파일을 직접 읽지 않는다
// (silverone 2026-06-02). worker `/tasks/prompt_options`가 config/prompts/<task>/
// 를 해석한다.
//
// worker 4xx(invalid task / index.yaml 오류)는 ErrInvalidArgument로 wrap해
// HTTP 400으로 surface한다. 5xx / 연결 오류는 일반 error → 500.
func (s *DatasetService) GetPromptOptions(ctx context.Context, task string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(task)
	if trimmed == "" {
		return nil, ErrInvalidArgument{Message: "task query parameter is required"}
	}
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return nil, ErrInvalidArgument{Message: "python-ai worker is not configured"}
	}

	body, err := json.Marshal(map[string]any{"task": trimmed})
	if err != nil {
		return nil, fmt.Errorf("prompt_options marshal: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/tasks/prompt_options", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("prompt_options request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prompt_options worker call: %w", err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("prompt_options worker read: %w", err)
	}

	// worker 4xx → invalid task 등. worker error 메시지를 그대로 400으로 노출.
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, ErrInvalidArgument{Message: promptOptionsErrorMessage(buf.Bytes())}
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("prompt_options worker returned %d: %s", resp.StatusCode, buf.String())
	}
	return json.RawMessage(buf.Bytes()), nil
}

// promptOptionsErrorMessage는 worker 응답 body({"error": "..."})에서 메시지를
// 추출한다. 파싱 실패 시 raw body를 그대로 반환.
func promptOptionsErrorMessage(raw []byte) string {
	var decoded struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err == nil && strings.TrimSpace(decoded.Error) != "" {
		return decoded.Error
	}
	return strings.TrimSpace(string(raw))
}
