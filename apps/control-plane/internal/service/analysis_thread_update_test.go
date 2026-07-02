package service

import (
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-06-30 (#28) — thread 제목 수정 잠금. CreateAnalysisThread는 active
// dataset version 셋업이 필요해 우회하고, store에 thread를 직접 적재한 뒤 rename만 검증.

func seedThread(t *testing.T, memory *store.MemoryStore) {
	t.Helper()
	if err := memory.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID:         "t1",
		ProjectID:        "p1",
		DatasetID:        "d1",
		DatasetVersionID: "v1",
		Title:            "옛 제목",
	}); err != nil {
		t.Fatalf("seed thread: %v", err)
	}
}

func TestUpdateAnalysisThread_Success(t *testing.T) {
	memory := store.NewMemoryStore()
	s := NewDatasetService(memory, "", "", "")
	seedThread(t, memory)

	updated, err := s.UpdateAnalysisThread("p1", "d1", "t1", domain.AnalysisThreadUpdateRequest{Title: "  새 제목  "})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Title != "새 제목" {
		t.Fatalf("title = %q, want %q (trim)", updated.Title, "새 제목")
	}
}

func TestUpdateAnalysisThread_EmptyTitleRejected(t *testing.T) {
	memory := store.NewMemoryStore()
	s := NewDatasetService(memory, "", "", "")
	seedThread(t, memory)

	if _, err := s.UpdateAnalysisThread("p1", "d1", "t1", domain.AnalysisThreadUpdateRequest{Title: "   "}); err == nil {
		t.Fatal("expected ErrInvalidArgument for blank title")
	} else if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("err = %T, want ErrInvalidArgument", err)
	}
}

func TestUpdateAnalysisThread_TitleTruncatedTo80Runes(t *testing.T) {
	memory := store.NewMemoryStore()
	s := NewDatasetService(memory, "", "", "")
	seedThread(t, memory)

	long := strings.Repeat("가", 100)
	updated, err := s.UpdateAnalysisThread("p1", "d1", "t1", domain.AnalysisThreadUpdateRequest{Title: long})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := len([]rune(updated.Title)); got != 80 {
		t.Fatalf("title rune len = %d, want 80", got)
	}
}

func TestUpdateAnalysisThread_NotFound(t *testing.T) {
	memory := store.NewMemoryStore()
	s := NewDatasetService(memory, "", "", "")
	seedThread(t, memory)

	// dataset 불일치 → 404
	if _, err := s.UpdateAnalysisThread("p1", "OTHER", "t1", domain.AnalysisThreadUpdateRequest{Title: "x"}); err == nil {
		t.Fatal("expected ErrNotFound for dataset mismatch")
	} else if _, ok := err.(ErrNotFound); !ok {
		t.Fatalf("err = %T, want ErrNotFound", err)
	}
	// 없는 thread → 404
	if _, err := s.UpdateAnalysisThread("p1", "d1", "nope", domain.AnalysisThreadUpdateRequest{Title: "x"}); err == nil {
		t.Fatal("expected ErrNotFound for missing thread")
	} else if _, ok := err.(ErrNotFound); !ok {
		t.Fatalf("err = %T, want ErrNotFound", err)
	}
}
