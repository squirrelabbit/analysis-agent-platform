// Package workererror centralizes worker HTTP failure → Temporal error
// type mapping so every control-plane client (python_ai, planner,
// final_answer, dataset_worker) gets the same retry semantics.
//
// Background (5/6 진단):
// 4/30 ADR-015 §A4 회귀(quality_tier="llm_augmented" enum 누락)가 5/6
// production trace에서 step triplet 18회 무한 재시도로 폭발했다. 직접 trigger는
// schema literal 누락이지만 *진짜 원인*은 control-plane이 worker HTTP 4xx를
// generic Go error로 wrap해 Temporal default retry policy가 영구 실패와
// 일시 장애를 구분 못 했다는 것. 4xx는 같은 payload 재시도해도 동일 거부라
// non-retryable이고 5xx만 retry 가치가 있는데 그 분기가 누락.
//
// 본 패키지는 분기 1곳에서만 정의해서 모든 client가 동일하게 사용한다.
package workererror

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// RejectionErrType는 Temporal ApplicationError의 식별 type. 운영자는 Temporal
// UI 또는 obs log에서 이 type을 보고 "worker가 영구 거부 — 재시도 무의미"임을
// 즉시 인식 가능. 변경하면 분류 통계가 끊기므로 신중.
const RejectionErrType = "WorkerInputRejected"

// Rejection wraps 4xx response (validation/input rejection) as a Temporal
// non-retryable ApplicationError so workflow stops dispatching the same
// payload. Includes body's error/detail/message for Temporal UI surface.
func Rejection(statusCode int, body []byte) error {
	msg := strings.TrimSpace(ParseMessage(body))
	if msg == "" {
		msg = fmt.Sprintf("worker rejected input with status %d", statusCode)
	} else {
		msg = fmt.Sprintf("worker rejected input (status %d): %s", statusCode, msg)
	}
	return temporal.NewNonRetryableApplicationError(msg, RejectionErrType, nil)
}

// Upstream wraps 5xx (transient upstream/LLM hiccup) as plain error so
// Temporal default retry policy can recover.
func Upstream(statusCode int, body []byte) error {
	msg := strings.TrimSpace(ParseMessage(body))
	if msg == "" {
		return fmt.Errorf("worker returned %d", statusCode)
	}
	return fmt.Errorf("worker returned %d: %s", statusCode, msg)
}

// ParseMessage attempts to read ``{"error":...}``, ``{"detail":...}`` or
// ``{"message":...}`` shapes the python-ai worker emits. Falls back to
// raw body when JSON parse fails so the original payload still reaches
// the operator log.
func ParseMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err == nil {
		for _, key := range []string{"error", "detail", "message"} {
			if v, ok := parsed[key]; ok {
				if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
					return s
				}
			}
		}
	}
	return string(body)
}

// ReadAllAndClose reads the full HTTP response body and closes it,
// returning whichever error happened first. Caller must drain body
// *before* status-code branching so the rejection message is preserved
// even when the body isn't a valid response shape.
func ReadAllAndClose(rc io.ReadCloser) ([]byte, error) {
	buf := bytes.Buffer{}
	_, readErr := buf.ReadFrom(rc)
	closeErr := rc.Close()
	if readErr != nil {
		return nil, readErr
	}
	return buf.Bytes(), closeErr
}
