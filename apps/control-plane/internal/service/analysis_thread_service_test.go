package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-06-04 — AnalysisThreadService facade 분리 잠금.
// thread 동작 자체는 analyze_test.go의 AnalyzeDatasetAsNewThread* /
// PostAnalysisThreadMessage* 케이스가 facade를 통해 커버한다. 여기서는
// facade가 store + deps를 AnalysisThreadService로 올바로 전달하는지(decoupling
// wiring)만 잠근다.

func TestDatasetServiceThreadsWiring(t *testing.T) {
	memory := store.NewMemoryStore()
	s := NewDatasetService(memory, "", "", "")

	ts := s.threads()
	if ts == nil {
		t.Fatal("threads() returned nil")
	}
	if ts.store == nil {
		t.Fatal("AnalysisThreadService.store not wired")
	}
	if ts.deps == nil {
		t.Fatal("AnalysisThreadService.deps not wired")
	}
	// DatasetService는 threadServiceDeps를 구현해야 한다(컴파일 타임 보장 + 런타임 확인).
	var _ threadServiceDeps = s
}
