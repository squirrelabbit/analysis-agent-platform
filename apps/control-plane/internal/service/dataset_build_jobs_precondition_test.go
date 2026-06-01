package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// 5/6 endpoint 통일 후 sentiment/embedding/cluster Create*Job 함수에 의존성
// precondition을 추가했다. 옛 단수 endpoint 흐름은 *호출자 책임*으로
// wait_for_status 폴링했지만 잘못 호출 시 worker가 빈 데이터로 fail하는
// silent fail이 가능했다. 본 테스트는 precondition 동작을 잠근다.
//
// silverone 2026-05-28 dead 14 제거 — `isPreparePreconditionSatisfied` /
// `isEmbeddingPreconditionSatisfied` 두 helper가 production caller 0건
// (ADR-018 β2 prepare/embedding 정리 잔존)이라 함께 제거됐다.
//
// silverone 2026-05-28 (β2 cleanup PR2) — DatasetVersion.PrepareStatus 필드
// 자체가 제거됐다. prepareStatus(version)은 이제 metadata jsonb만 본다.

func TestPrepareStatusReadsFromMetadata(t *testing.T) {
	v := domain.DatasetVersion{Metadata: map[string]any{"prepare_status": "queued"}}
	if got := prepareStatus(v); got != "queued" {
		t.Errorf("expected queued from metadata, got %q", got)
	}
	empty := domain.DatasetVersion{}
	if got := prepareStatus(empty); got != "" {
		t.Errorf("expected empty for empty version, got %q", got)
	}
}
