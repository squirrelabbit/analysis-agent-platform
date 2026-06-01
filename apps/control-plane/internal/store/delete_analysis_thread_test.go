package store

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-06-01 — DeleteAnalysisThread: project+dataset+thread 일치 row만
// 삭제, messages/runs/rejection_events cascade(메모리는 수동 모사), 불일치/없음은
// ErrNotFound.
func TestMemoryStore_DeleteAnalysisThread_Cascade(t *testing.T) {
	s := NewMemoryStore()
	now := time.Now().UTC()
	if err := s.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t1", ProjectID: "p1", DatasetID: "d1", DatasetVersionID: "v1",
		Title: "test", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("save thread: %v", err)
	}
	if err := s.SaveAnalysisMessage(domain.AnalysisMessage{
		MessageID: "m1", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		Role: "user", Content: "비슷한 후기끼리 묶어줘", CreatedAt: now,
	}); err != nil {
		t.Fatalf("save message: %v", err)
	}
	if err := s.SaveAnalysisRun(domain.AnalysisRun{
		RunID: "r1", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "completed", CreatedAt: now,
	}); err != nil {
		t.Fatalf("save run: %v", err)
	}
	if err := s.SaveRejectionEvent(domain.PlannerRejectionEvent{
		EventID: "e1", MessageID: "m1", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		UserQuestion: "비슷한 후기끼리 묶어줘", Reason: "unsupported_skill",
	}); err != nil {
		t.Fatalf("save event: %v", err)
	}

	// 다른 dataset으로는 삭제 불가.
	if err := s.DeleteAnalysisThread("p1", "OTHER", "t1"); err != ErrNotFound {
		t.Fatalf("cross-dataset delete: want ErrNotFound, got %v", err)
	}
	// 없는 thread → ErrNotFound.
	if err := s.DeleteAnalysisThread("p1", "d1", "nope"); err != ErrNotFound {
		t.Fatalf("missing thread: want ErrNotFound, got %v", err)
	}
	// 데이터는 아직 그대로여야 함 (실패한 삭제가 cascade를 건드리지 않음).
	if len(s.analysisMessages) != 1 || len(s.analysisRuns) != 1 || len(s.rejectionEvents) != 1 {
		t.Fatalf("failed delete must not touch children")
	}

	// 정상 삭제.
	if err := s.DeleteAnalysisThread("p1", "d1", "t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(s.analysisMessages) != 0 {
		t.Errorf("messages not cascaded: %d", len(s.analysisMessages))
	}
	if len(s.analysisRuns) != 0 {
		t.Errorf("runs not cascaded: %d", len(s.analysisRuns))
	}
	if len(s.rejectionEvents) != 0 {
		t.Errorf("rejection_events not cascaded: %d", len(s.rejectionEvents))
	}
	items, _ := s.ListAnalysisThreads("p1", "d1")
	if len(items) != 0 {
		t.Errorf("thread still in list: %d", len(items))
	}
	// 이미 삭제된 thread 재삭제 → ErrNotFound.
	if err := s.DeleteAnalysisThread("p1", "d1", "t1"); err != ErrNotFound {
		t.Fatalf("re-delete: want ErrNotFound, got %v", err)
	}
}
