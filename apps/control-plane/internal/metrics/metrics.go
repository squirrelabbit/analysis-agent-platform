// Package metrics — control-plane 최소 메트릭 (silverone 2026-06-04, metrics 1차).
//
// Prometheus client 의존 없이 plain text exposition format으로 노출한다(/metrics).
// 1차 범위는 analyze worker 호출 결과뿐 — HTTP request count/status/duration bucket은
// 과해서 보류. atomic 카운터라 lock-free, ThreadingHTTPServer/goroutine 양쪽 안전.
package metrics

import (
	"fmt"
	"strings"
	"sync/atomic"
)

var (
	analysisWorkerCallOK        atomic.Int64
	analysisWorkerCallError     atomic.Int64
	analysisWorkerDurationMsSum atomic.Int64
	analysisWorkerDurationCount atomic.Int64
)

// RecordAnalysisWorkerCall — postPythonAITask 1회 호출 결과를 기록한다.
// status는 "ok"(2xx/3xx 응답) 또는 그 외 모두 "error"(transport 실패 / 4xx / 5xx).
func RecordAnalysisWorkerCall(status string, durationMs int64) {
	if status == "ok" {
		analysisWorkerCallOK.Add(1)
	} else {
		analysisWorkerCallError.Add(1)
	}
	if durationMs < 0 {
		durationMs = 0
	}
	analysisWorkerDurationMsSum.Add(durationMs)
	analysisWorkerDurationCount.Add(1)
}

// Render — Prometheus text exposition format. /metrics가 그대로 반환.
func Render() string {
	var b strings.Builder
	b.WriteString("# HELP control_plane_analysis_worker_call_total Total analyze worker (python-ai /tasks/analyze) calls by status.\n")
	b.WriteString("# TYPE control_plane_analysis_worker_call_total counter\n")
	fmt.Fprintf(&b, "control_plane_analysis_worker_call_total{status=\"ok\"} %d\n", analysisWorkerCallOK.Load())
	fmt.Fprintf(&b, "control_plane_analysis_worker_call_total{status=\"error\"} %d\n", analysisWorkerCallError.Load())
	// 단순 summary(sum + count) — 평균 = sum/count. histogram bucket은 1차 보류.
	b.WriteString("# HELP control_plane_analysis_worker_call_duration_ms_sum Sum of analyze worker call durations in ms.\n")
	b.WriteString("# TYPE control_plane_analysis_worker_call_duration_ms_sum counter\n")
	fmt.Fprintf(&b, "control_plane_analysis_worker_call_duration_ms_sum %d\n", analysisWorkerDurationMsSum.Load())
	b.WriteString("# HELP control_plane_analysis_worker_call_duration_ms_count Count of timed analyze worker calls.\n")
	b.WriteString("# TYPE control_plane_analysis_worker_call_duration_ms_count counter\n")
	fmt.Fprintf(&b, "control_plane_analysis_worker_call_duration_ms_count %d\n", analysisWorkerDurationCount.Load())
	return b.String()
}

// ResetForTest — 테스트 격리용 (production 비호출).
func ResetForTest() {
	analysisWorkerCallOK.Store(0)
	analysisWorkerCallError.Store(0)
	analysisWorkerDurationMsSum.Store(0)
	analysisWorkerDurationCount.Store(0)
}
