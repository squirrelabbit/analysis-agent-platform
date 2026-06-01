package datasetprompts_test

import (
	"testing"

	"analysis-support-platform/control-plane/internal/service/datasetprompts"
	"analysis-support-platform/control-plane/internal/store"
)

// TestRepositorySatisfiesStore — silverone 2026-05-28 subpackage pilot facade
// 잠금. store.Repository가 datasetprompts.Store interface(8 method subset)을
// 만족하는지 compile-time + runtime 양쪽으로 확인한다. 옛 service.DatasetService
// 가 `s.store` (store.Repository)로 datasetprompts.New(repository)를 부르는
// 흐름이 회귀 없이 유지되는지 보장.
func TestRepositorySatisfiesStore(t *testing.T) {
	var _ datasetprompts.Store = (*store.MemoryStore)(nil)

	repo := store.NewMemoryStore()
	svc := datasetprompts.New(repo)
	if svc == nil {
		t.Fatalf("New returned nil")
	}
	// silverone 2026-05-28 — 옛 SetTemplatesDir setter는 dead field 정리에서
	// 제거됐다. 외부 caller가 있는 `DatasetService.SetPromptTemplatesDir`는
	// no-op 형태로 service 패키지에 남아 있다.
}
