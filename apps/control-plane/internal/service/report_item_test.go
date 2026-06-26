package service

import (
	"encoding/json"
	"errors"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

func seedRunForAppend(t *testing.T, svc *DatasetService) {
	t.Helper()
	if err := svc.store.SaveAnalysisRun(domain.AnalysisRun{
		RunID:       "run1",
		ThreadID:    "th1",
		ProjectID:   "p1",
		DatasetID:   "d1",
		RequestJSON: map[string]any{"user_question": "긍정 절 보여줘"},
		ResultJSON: json.RawMessage(
			`{"composer":{"assistant_content":"답변 본문","display":{"title":"분석 제목"}},"plan":{"plan_version":"v2"}}`,
		),
		Status: "completed",
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
}

func TestAppendReportItem_AnalysisResult(t *testing.T) {
	svc := newReportSvc(t)
	seedRunForAppend(t, svc)
	report, err := svc.CreateReport("p1", domain.ReportCreateRequest{
		Title: "R", Blocks: json.RawMessage(`[{"uid":"existing"}]`),
	})
	if err != nil {
		t.Fatalf("CreateReport: %v", err)
	}

	resp, err := svc.AppendReportItem("p1", report.ReportID, domain.ReportItemAppendRequest{
		Type:     "analysis_result",
		RunID:    "run1",
		ThreadID: "th1",
		Options:  map[string]any{"show_question": true},
		Layout:   map[string]any{"span": float64(12), "new_row": true},
	})
	if err != nil {
		t.Fatalf("AppendReportItem: %v", err)
	}

	// item: run에서 추출된 필드 + 입력 옵션.
	if resp.Item["run_id"] != "run1" || resp.Item["thread_id"] != "th1" {
		t.Errorf("item run/thread = %v / %v", resp.Item["run_id"], resp.Item["thread_id"])
	}
	if resp.Item["question"] != "긍정 절 보여줘" {
		t.Errorf("question = %v", resp.Item["question"])
	}
	if resp.Item["assistant_content"] != "답변 본문" {
		t.Errorf("assistant_content = %v", resp.Item["assistant_content"])
	}
	if resp.Item["title"] != "분석 제목" { // display.title fallback
		t.Errorf("title = %v", resp.Item["title"])
	}
	if resp.Item["uid"] == nil || resp.Item["uid"] == "" {
		t.Error("uid not generated")
	}
	if _, ok := resp.Item["display"].(map[string]any); !ok {
		t.Error("display snapshot missing")
	}
	if _, ok := resp.Item["plan"].(map[string]any); !ok {
		t.Error("plan snapshot missing")
	}

	// 기존 blocks 보존 + append (1 → 2).
	var blocks []any
	if err := json.Unmarshal(resp.Report.Blocks, &blocks); err != nil {
		t.Fatalf("blocks unmarshal: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks not appended: %d (expected 2)", len(blocks))
	}
	if first := blocks[0].(map[string]any); first["uid"] != "existing" {
		t.Errorf("existing block not preserved: %v", first)
	}

	// updated_at 갱신(created_at보다 같거나 이후), report_id 유지.
	if resp.Report.ReportID != report.ReportID {
		t.Error("report_id changed")
	}
	if resp.Report.UpdatedAt.Before(report.UpdatedAt) {
		t.Error("updated_at not advanced")
	}
}

func TestAppendReportItem_Errors(t *testing.T) {
	svc := newReportSvc(t)
	seedRunForAppend(t, svc)
	report, _ := svc.CreateReport("p1", domain.ReportCreateRequest{Title: "R"})

	// report 없음.
	if _, err := svc.AppendReportItem("p1", "nope", domain.ReportItemAppendRequest{
		Type: "analysis_result", RunID: "run1",
	}); !errors.As(err, &ErrNotFound{}) {
		t.Errorf("missing report: want ErrNotFound, got %v", err)
	}

	// run 없음.
	if _, err := svc.AppendReportItem("p1", report.ReportID, domain.ReportItemAppendRequest{
		Type: "analysis_result", RunID: "nope",
	}); !errors.As(err, &ErrNotFound{}) {
		t.Errorf("missing run: want ErrNotFound, got %v", err)
	}

	// run_id 누락.
	if _, err := svc.AppendReportItem("p1", report.ReportID, domain.ReportItemAppendRequest{
		Type: "analysis_result",
	}); !errors.As(err, &ErrInvalidArgument{}) {
		t.Errorf("missing run_id: want ErrInvalidArgument, got %v", err)
	}

	// thread_id 불일치.
	if _, err := svc.AppendReportItem("p1", report.ReportID, domain.ReportItemAppendRequest{
		Type: "analysis_result", RunID: "run1", ThreadID: "wrong",
	}); !errors.As(err, &ErrInvalidArgument{}) {
		t.Errorf("thread mismatch: want ErrInvalidArgument, got %v", err)
	}
}
