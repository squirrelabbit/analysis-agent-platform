package registry

import "testing"

// TestGoCalledTasksResolveInRegistry — drift detector (ADR-031 1단계).
//
// Go control-plane이 TaskPathFor로 호출하는 dataset_build task가 task_registry.json에
// 없으면 런타임에 빈 path가 반환되어 worker 호출이 조용히 실패한다. Go가 의존하는 task와
// registry가 어긋나지 않도록 잠근다(Go↔registry 단방향 — cross-language라 Python handler와는
// 각 언어가 각자 registry를 대조한다. Python 쪽은 test_contract_drift.py).
//
// 새 dataset_build task를 추가하면 이 목록도 함께 갱신해야 한다(그게 곧 drift 알림).
func TestGoCalledTasksResolveInRegistry(t *testing.T) {
	called := []string{
		"dataset_clean",
		"dataset_doc_genuineness",
		"dataset_clause_label",
		"dataset_clause_keywords",
	}
	for _, name := range called {
		if got := TaskPathFor(name); got == "" {
			t.Fatalf("task %q: Go가 호출하는데 registry에서 resolve 안 됨(빈 path) — "+
				"config/task_registry.json에 등록됐는지 확인", name)
		}
	}
}
