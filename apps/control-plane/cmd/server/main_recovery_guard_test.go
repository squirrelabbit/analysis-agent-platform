package main

import (
	"os"
	"strings"
	"testing"
)

// silverone 2026-05-27 (Codex adversarial review fix-2 follow-up) —
// startup reconciliation 호출이 main boot path에서 사라지지 않게 잠근다.
// 옛 회귀(이번 incident의 원인 중 하나)가 다시 일어나는 걸 정적으로 차단.

func TestMainBootCallsReconcileStartup(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	source := string(data)

	// server.ReconcileStartup(ctx) 호출이 main 본문에 있어야 한다. 인자/공백
	// 변동을 흡수하기 위해 `server.ReconcileStartup` 토큰만 검사.
	if !strings.Contains(source, "server.ReconcileStartup") {
		t.Fatal(
			"main.go must call server.ReconcileStartup before ListenAndServe. " +
				"in-flight analysis_runs / dataset_build_jobs를 단말 상태로 마감하지 않으면 " +
				"control-plane 재시작 후 active job lookup이 영원히 막힌다. " +
				"Codex adversarial review fix-2 (2026-05-26) 정책.",
		)
	}

	// listening 전에 호출되는지도 가벼운 순서 검사. ListenAndServe보다 앞 위치에
	// ReconcileStartup이 있어야 한다.
	reconcileIdx := strings.Index(source, "server.ReconcileStartup")
	listenIdx := strings.Index(source, "ListenAndServe")
	if reconcileIdx < 0 || listenIdx < 0 {
		t.Fatalf("could not locate boot landmarks (reconcile=%d listen=%d)", reconcileIdx, listenIdx)
	}
	if reconcileIdx > listenIdx {
		t.Fatalf(
			"server.ReconcileStartup must be called BEFORE ListenAndServe (got reconcile=%d listen=%d)",
			reconcileIdx, listenIdx,
		)
	}
}
