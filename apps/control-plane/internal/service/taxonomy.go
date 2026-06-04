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

// GetTaxonomy는 aspect/sentiment taxonomy 정의(key/label/description)를 Python
// worker로 proxy해서 반환한다. Go control-plane은 config/taxonomies/ 파일을 직접
// 읽지 않는다 (silverone 2026-06-04) — prompt_options와 동일한 위임 패턴.
// worker `/tasks/taxonomy`가 config/taxonomies/<id>.json을 해석한다.
//
// taxonomyID가 빈 값이면 worker가 default(festival-v2)를 적용한다.
// worker 4xx(unknown taxonomy_id 등)는 ErrInvalidArgument로 wrap해 HTTP 400으로
// surface한다. 5xx / 연결 오류는 일반 error → 500.
func (s *DatasetService) GetTaxonomy(ctx context.Context, taxonomyID string) (json.RawMessage, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return nil, ErrInvalidArgument{Message: "python-ai worker is not configured"}
	}

	body, err := json.Marshal(map[string]any{"taxonomy_id": strings.TrimSpace(taxonomyID)})
	if err != nil {
		return nil, fmt.Errorf("taxonomy marshal: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/tasks/taxonomy", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("taxonomy request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("taxonomy worker call: %w", err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("taxonomy worker read: %w", err)
	}

	// worker 4xx → unknown taxonomy_id 등. worker error 메시지를 그대로 400으로 노출.
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, ErrInvalidArgument{Message: taxonomyErrorMessage(buf.Bytes())}
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("taxonomy worker returned %d: %s", resp.StatusCode, buf.String())
	}
	return json.RawMessage(buf.Bytes()), nil
}

// taxonomyErrorMessage는 worker 응답 body({"error": "..."})에서 메시지를 추출한다.
// 파싱 실패 시 raw body를 그대로 반환.
func taxonomyErrorMessage(raw []byte) string {
	var decoded struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err == nil && strings.TrimSpace(decoded.Error) != "" {
		return decoded.Error
	}
	return strings.TrimSpace(string(raw))
}
