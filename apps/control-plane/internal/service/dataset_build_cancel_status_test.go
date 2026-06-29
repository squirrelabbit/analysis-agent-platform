package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// 빌드 중단 상태 해석 (silverone 2026-06-29) — 중단 시 ref는 제거되므로 cancelled 플래그만으로
// "cancelled"를 표시한다(artifact 없어도). 단 재실행 중(job running/queued)이면 그 상태 우선.
func TestResolveArtifactStatusCancelled(t *testing.T) {
	completed := &domain.DatasetBuildJob{Status: "completed"}
	running := &domain.DatasetBuildJob{Status: "running"}
	queued := &domain.DatasetBuildJob{Status: "queued"}

	// cancelled flag + ref 없음(중단으로 제거됨) + job completed → "cancelled".
	if got := resolveArtifactStatus("", completed, "", true); got != "cancelled" {
		t.Errorf("cancelled(no artifact) = %q, want cancelled", got)
	}
	// 재실행 중이면(job running) 이전 cancelled 잔재 무시 → running.
	if got := resolveArtifactStatus("", running, "", true); got != "running" {
		t.Errorf("cancelled+running = %q, want running", got)
	}
	// 재실행 대기(queued)도 마찬가지 → queued.
	if got := resolveArtifactStatus("", queued, "", true); got != "queued" {
		t.Errorf("cancelled+queued = %q, want queued", got)
	}
	// 정상 완료(cancelled=false) + artifact → completed.
	if got := resolveArtifactStatus("/x/clause_label.jsonl", completed, "", false); got != "completed" {
		t.Errorf("completed = %q, want completed", got)
	}
}
