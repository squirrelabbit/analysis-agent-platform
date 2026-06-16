package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 모델별 결과 보관(runs) 잠금 (silverone 2026-06-15).

func TestUpsertDocGenuinenessRun_DedupeByModel(t *testing.T) {
	v := domain.DatasetVersion{Metadata: map[string]any{}}
	upsertDocGenuinenessRun(&v, domain.DocGenuinenessRun{Model: "m-a", Ref: "/a1.jsonl", CompletedAt: time.Now().UTC()})
	upsertDocGenuinenessRun(&v, domain.DocGenuinenessRun{Model: "m-b", Ref: "/b.jsonl", CompletedAt: time.Now().UTC()})
	// 같은 모델 재실행 → 갱신(추가 아님).
	upsertDocGenuinenessRun(&v, domain.DocGenuinenessRun{Model: "m-a", Ref: "/a2.jsonl", CompletedAt: time.Now().UTC()})

	runs := docGenuinenessRunsFromMetadata(v.Metadata)
	if len(runs) != 2 {
		t.Fatalf("runs=%d, want 2 (m-a updated, not appended): %+v", len(runs), runs)
	}
	a, ok := findDocGenuinenessRun(runs, "m-a")
	if !ok || a.Ref != "/a2.jsonl" {
		t.Fatalf("m-a ref=%q, want /a2.jsonl (latest)", a.Ref)
	}
}

func TestDocGenuinenessRunsFromMetadata_LegacyFallback(t *testing.T) {
	// runs 키 없는 옛 버전 — 단일 ref + summary.model로 run 1건 합성.
	metadata := map[string]any{
		"doc_genuineness_ref":     "/legacy.jsonl",
		"doc_genuineness_summary": map[string]any{"model": "wisenut/wise-lloa-max-v1.2.1"},
	}
	runs := docGenuinenessRunsFromMetadata(metadata)
	if len(runs) != 1 {
		t.Fatalf("runs=%d, want 1 (legacy synth)", len(runs))
	}
	if runs[0].Model != "wisenut/wise-lloa-max-v1.2.1" || runs[0].Ref != "/legacy.jsonl" {
		t.Fatalf("legacy run wrong: %+v", runs[0])
	}
}

func TestDocGenuinenessRunsFromMetadata_RoundTripStrings(t *testing.T) {
	// JSONB 왕복 모사 — completed_at이 string, runs가 []any{map}인 경우도 파싱.
	metadata := map[string]any{
		docGenuinenessRunsMetaKey: []any{
			map[string]any{"model": "m-a", "ref": "/a.jsonl", "completed_at": "2026-06-15T00:00:00Z"},
		},
	}
	runs := docGenuinenessRunsFromMetadata(metadata)
	if len(runs) != 1 || runs[0].Model != "m-a" || runs[0].CompletedAt.IsZero() {
		t.Fatalf("round-trip parse failed: %+v", runs)
	}
}

func TestPendingLegacyDocGenuinenessRun(t *testing.T) {
	// runs 키 없는 옛 단일 결과 → 새 모델 파일로 빌드 시 편입 대상.
	legacy := map[string]any{
		"doc_genuineness_ref":     "/dg.jsonl",
		"doc_genuineness_summary": map[string]any{"model": "model-max"},
	}
	run, ok := pendingLegacyDocGenuinenessRun(legacy, "/dg.model-ultra.jsonl")
	if !ok || run.Model != "model-max" || run.Ref != "/dg.jsonl" {
		t.Fatalf("expected legacy max run, got ok=%v run=%+v", ok, run)
	}

	// 이미 runs 키가 있으면 편입 안 함(이미 관리됨).
	withRuns := map[string]any{
		docGenuinenessRunsMetaKey: []any{map[string]any{"model": "m", "ref": "/r.jsonl"}},
		"doc_genuineness_ref":     "/dg.jsonl",
	}
	if _, ok := pendingLegacyDocGenuinenessRun(withRuns, "/new.jsonl"); ok {
		t.Fatal("should not fold legacy when runs key exists")
	}

	// 새로 쓸 파일과 같은 경로(같은 모델 재빌드)면 편입 안 함.
	if _, ok := pendingLegacyDocGenuinenessRun(legacy, "/dg.jsonl"); ok {
		t.Fatal("should not fold when ref == new output path")
	}

	// ref 없으면 편입 안 함.
	if _, ok := pendingLegacyDocGenuinenessRun(map[string]any{}, "/new.jsonl"); ok {
		t.Fatal("should not fold without ref")
	}
}

func TestGetDocGenuinenessRuns_FillsDisplayName(t *testing.T) {
	repo := store.NewMemoryStore()
	if err := repo.SaveProject(domain.Project{ProjectID: "p1"}); err != nil {
		t.Fatalf("project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{DatasetID: "d1", ProjectID: "p1"}); err != nil {
		t.Fatalf("dataset: %v", err)
	}
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	svc.SetLLOAModelDisplay("wisenut/wise-lloa-max-v1.2.1", "LLOA Max 1.2.1")

	v := domain.DatasetVersion{DatasetVersionID: "v1", DatasetID: "d1", ProjectID: "p1", Metadata: map[string]any{}}
	upsertDocGenuinenessRun(&v, domain.DocGenuinenessRun{Model: "wisenut/wise-lloa-max-v1.2.1", Ref: "/a.jsonl", CompletedAt: time.Now().UTC()})
	if err := repo.SaveDatasetVersion(v); err != nil {
		t.Fatalf("version: %v", err)
	}

	resp, err := svc.GetDocGenuinenessRuns("p1", "d1", "v1")
	if err != nil {
		t.Fatalf("get runs: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ModelDisplayName != "LLOA Max 1.2.1" {
		t.Fatalf("display name not filled: %+v", resp.Items)
	}
}
