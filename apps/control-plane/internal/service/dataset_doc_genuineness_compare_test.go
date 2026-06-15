package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 진성 분류 모델 비교 (silverone 2026-06-15) — 한 버전에 모델별로 누적된 결과를
// doc_id 1:1 비교. override는 원본 모델 라벨을 오염시키지 않고 정답 힌트로만 노출.

// writeCompareDGArtifact — doc_id→genuineness 맵으로 doc_genuineness jsonl 작성.
func writeCompareDGArtifact(t *testing.T, dir, name string, labels map[string]string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	var lines []string
	for docID, g := range labels {
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":"%s","genuineness":"%s","reason":"r-%s","source":"lloa","prompt_version":"v1"}`,
			docID, g, g,
		))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	return path
}

func newCompareService(t *testing.T) (*DatasetService, *store.MemoryStore) {
	t.Helper()
	repo := store.NewMemoryStore()
	if err := repo.SaveProject(domain.Project{ProjectID: "p1", Name: "P", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{DatasetID: "d1", ProjectID: "p1", Name: "ds", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	return NewDatasetService(repo, "", t.TempDir(), t.TempDir()), repo
}

// seedVersionWithRuns — 한 버전에 모델별 run을 누적해 저장한다.
func seedVersionWithRuns(t *testing.T, repo *store.MemoryStore, vid string, runs map[string]map[string]string) {
	t.Helper()
	dir := t.TempDir()
	version := domain.DatasetVersion{
		DatasetVersionID: vid, DatasetID: "d1", ProjectID: "p1",
		StorageURI: "/tmp/src.csv",
		Metadata:   map[string]any{},
	}
	for model, labels := range runs {
		ref := writeCompareDGArtifact(t, dir, "dg."+docGenuinenessModelSlug(model)+".jsonl", labels)
		upsertDocGenuinenessRun(&version, domain.DocGenuinenessRun{
			Model: model, Ref: ref, PromptVersion: "v1", CompletedAt: time.Now().UTC(),
		})
		// 최신 단일 ref도 갱신(하위 호환 view용).
		version.Metadata["doc_genuineness_ref"] = ref
		version.Metadata["doc_genuineness_status"] = "ready"
	}
	if err := repo.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version %s: %v", vid, err)
	}
}

func tierIndex(t *testing.T, view domain.DocGenuinenessCompareView, tier string) int {
	t.Helper()
	for i, x := range view.Tiers {
		if x == tier {
			return i
		}
	}
	t.Fatalf("tier %q not in %v", tier, view.Tiers)
	return -1
}

func TestCompareDocGenuineness_AgreementAndConfusion(t *testing.T) {
	svc, repo := newCompareService(t)
	seedVersionWithRuns(t, repo, "v1", map[string]map[string]string{
		"model-a": {"doc:1": "genuine_review", "doc:2": "non_review", "doc:3": "genuine_review", "doc:4": "mixed"},
		"model-b": {"doc:1": "genuine_review", "doc:2": "non_review", "doc:3": "non_review", "doc:4": "genuine_review"},
	})

	view, err := svc.CompareDocGenuineness("p1", "d1", "v1", "model-a", "model-b", 100, 0)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if view.Compared != 4 || view.Matched != 2 {
		t.Fatalf("compared=%d matched=%d, want 4/2", view.Compared, view.Matched)
	}
	if view.Rate != 0.5 {
		t.Fatalf("rate=%v, want 0.5", view.Rate)
	}
	if view.OnlyInA != 0 || view.OnlyInB != 0 {
		t.Fatalf("onlyA=%d onlyB=%d, want 0/0", view.OnlyInA, view.OnlyInB)
	}
	if view.VersionA.Model != "model-a" || view.VersionB.Model != "model-b" {
		t.Fatalf("models A=%q B=%q", view.VersionA.Model, view.VersionB.Model)
	}
	if view.VersionA.DatasetVersionID != "v1" || view.VersionB.DatasetVersionID != "v1" {
		t.Fatalf("both sides should be same version v1")
	}
	gi := tierIndex(t, view, "genuine_review")
	ni := tierIndex(t, view, "non_review")
	mi := tierIndex(t, view, "mixed")
	if view.Confusion[gi][gi] != 1 || view.Confusion[ni][ni] != 1 {
		t.Fatalf("diagonal wrong: %v", view.Confusion)
	}
	if view.Confusion[gi][ni] != 1 { // doc:3 A genuine, B non
		t.Fatalf("confusion[genuine][non]=%d, want 1", view.Confusion[gi][ni])
	}
	if view.Confusion[mi][gi] != 1 { // doc:4 A mixed, B genuine
		t.Fatalf("confusion[mixed][genuine]=%d, want 1", view.Confusion[mi][gi])
	}
	if view.DisagreementsTotal != 2 || len(view.Disagreements) != 2 {
		t.Fatalf("disagreements total=%d len=%d, want 2/2", view.DisagreementsTotal, len(view.Disagreements))
	}
	if view.Disagreements[0].DocID != "doc:3" {
		t.Fatalf("first disagreement=%q, want doc:3", view.Disagreements[0].DocID)
	}
}

func TestCompareDocGenuineness_OverrideIsGroundTruthHint(t *testing.T) {
	svc, repo := newCompareService(t)
	seedVersionWithRuns(t, repo, "v1", map[string]map[string]string{
		"model-a": {"doc:1": "genuine_review"},
		"model-b": {"doc:1": "non_review"},
	})
	// 사람이 doc:1을 non_review로 보정(정답=non_review, B가 맞음).
	if err := repo.UpsertDocGenuinenessOverride(domain.DocGenuinenessOverride{
		ProjectID: "p1", DatasetID: "d1", DatasetVersionID: "v1", DocID: "doc:1",
		OriginalGenuineness: "genuine_review", OverrideGenuineness: "non_review", OverrideReason: "광고",
	}); err != nil {
		t.Fatalf("upsert override: %v", err)
	}

	view, err := svc.CompareDocGenuineness("p1", "d1", "v1", "model-a", "model-b", 100, 0)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	// 비교는 여전히 불일치(원본 라벨 기준). override가 일치로 마스킹하면 안 됨.
	if view.Compared != 1 || view.Matched != 0 {
		t.Fatalf("compared=%d matched=%d, want 1/0", view.Compared, view.Matched)
	}
	if len(view.Disagreements) != 1 {
		t.Fatalf("disagreements=%d, want 1", len(view.Disagreements))
	}
	d := view.Disagreements[0]
	if d.AGenuineness != "genuine_review" {
		t.Fatalf("A raw=%q, want genuine_review", d.AGenuineness)
	}
	if d.OverrideGenuineness != "non_review" {
		t.Fatalf("override hint=%q, want non_review", d.OverrideGenuineness)
	}
}

func TestCompareDocGenuineness_DisagreementPagination(t *testing.T) {
	svc, repo := newCompareService(t)
	a := map[string]string{}
	b := map[string]string{}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("doc:%d", i)
		a[id] = "genuine_review"
		b[id] = "non_review"
	}
	seedVersionWithRuns(t, repo, "v1", map[string]map[string]string{"model-a": a, "model-b": b})

	view, err := svc.CompareDocGenuineness("p1", "d1", "v1", "model-a", "model-b", 2, 2)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if view.DisagreementsTotal != 5 || len(view.Disagreements) != 2 {
		t.Fatalf("total=%d page=%d, want 5/2", view.DisagreementsTotal, len(view.Disagreements))
	}
	if view.Disagreements[0].DocID != "doc:2" {
		t.Fatalf("page[0]=%q, want doc:2", view.Disagreements[0].DocID)
	}
}

func TestCompareDocGenuineness_Validation(t *testing.T) {
	svc, repo := newCompareService(t)
	seedVersionWithRuns(t, repo, "v1", map[string]map[string]string{
		"model-a": {"doc:1": "genuine_review"},
	})

	// 같은 모델.
	if _, err := svc.CompareDocGenuineness("p1", "d1", "v1", "model-a", "model-a", 100, 0); err == nil {
		t.Fatal("same model should error")
	}
	// 없는 버전.
	_, err := svc.CompareDocGenuineness("p1", "d1", "vMissing", "model-a", "model-b", 100, 0)
	var notFound ErrNotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("missing version should be ErrNotFound, got %v", err)
	}
	// 그 버전에 없는 모델.
	_, err = svc.CompareDocGenuineness("p1", "d1", "v1", "model-a", "model-z", 100, 0)
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("missing model should be ErrInvalidArgument, got %v", err)
	}
}
