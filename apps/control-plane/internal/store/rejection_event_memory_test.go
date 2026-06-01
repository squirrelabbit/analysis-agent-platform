package store

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-06-01 (PR2) — MemoryStore.SaveRejectionEvent는 message_id 기준
// idempotent여야 한다 (같은 거절 응답 재처리 시 중복 적재 방지).
func TestMemoryStore_SaveRejectionEvent_DedupByMessageID(t *testing.T) {
	s := NewMemoryStore()
	ev := domain.PlannerRejectionEvent{
		EventID:      "e1",
		ProjectID:    "p1",
		DatasetID:    "d1",
		ThreadID:     "t1",
		MessageID:    "m1",
		UserQuestion: "비슷한 후기끼리 묶어줘",
		Reason:       "unsupported_skill",
		Message:      "현재 지원하지 않습니다.",
		CapabilityGap: map[string]any{
			"suggested_skill": "cluster_texts",
		},
	}
	if err := s.SaveRejectionEvent(ev); err != nil {
		t.Fatalf("save 1: %v", err)
	}
	// 같은 message_id로 다른 event_id를 다시 저장 → 무시되어야 함.
	dup := ev
	dup.EventID = "e2"
	dup.Reason = "missing_data_or_artifact"
	if err := s.SaveRejectionEvent(dup); err != nil {
		t.Fatalf("save 2: %v", err)
	}

	got := s.rejectionEvents["m1"]
	if got.EventID != "e1" || got.Reason != "unsupported_skill" {
		t.Errorf("dedup failed — first write should win: %+v", got)
	}
	if len(s.rejectionEvents) != 1 {
		t.Errorf("expected 1 event, got %d", len(s.rejectionEvents))
	}
	if got.CreatedAt.IsZero() {
		t.Errorf("created_at should be auto-filled")
	}
}
