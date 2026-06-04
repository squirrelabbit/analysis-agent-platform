package metrics

import (
	"strings"
	"testing"
)

func TestRecordAndRender(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	RecordAnalysisWorkerCall("ok", 10)
	RecordAnalysisWorkerCall("ok", 20)
	RecordAnalysisWorkerCall("error", 5)

	out := Render()
	for _, want := range []string{
		"# TYPE control_plane_analysis_worker_call_total counter",
		`control_plane_analysis_worker_call_total{status="ok"} 2`,
		`control_plane_analysis_worker_call_total{status="error"} 1`,
		"control_plane_analysis_worker_call_duration_ms_sum 35",
		"control_plane_analysis_worker_call_duration_ms_count 3",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing %q:\n%s", want, out)
		}
	}
}

func TestNegativeDurationClamped(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	RecordAnalysisWorkerCall("ok", -5)
	if !strings.Contains(Render(), "control_plane_analysis_worker_call_duration_ms_sum 0") {
		t.Fatalf("negative duration not clamped:\n%s", Render())
	}
}
