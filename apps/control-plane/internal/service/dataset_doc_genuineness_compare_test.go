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

// 진성 분류 모델 비교 (silverone 2026-06-15) — doc_id 1:1 비교, 일치율/혼동행렬/
// 불일치 목록, override는 원본 모델 라벨을 오염시키지 않고 정답 힌트로만 노출.

// writeCompareDGArtifact — doc_id→genuineness 맵으로 doc_genuineness jsonl 작성.
func writeCompareDGArtifact(t *testing.T, name string, labels map[string]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
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

// seedCompareVersion — 같은 project/dataset 아래 진성 분류 ready 버전 1개 생성.
func seedCompareVersion(t *testing.T, repo *store.MemoryStore, vid, model string, labels map[string]string) {
	t.Helper()
	ref := writeCompareDGArtifact(t, vid+".jsonl", labels)
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: vid, DatasetID: "d1", ProjectID: "p1",
		StorageURI: "/tmp/src.csv",
		Metadata: map[string]any{
			"doc_genuineness_ref":     ref,
			"doc_genuineness_status":  "ready",
			"doc_genuineness_summary": map[string]any{"model": model},
		},
	}); err != nil {
		t.Fatalf("save version %s: %v", vid, err)
	}
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
	// d1,d2 일치 / d3,d4 불일치.
	seedCompareVersion(t, repo, "vA", "wisenut/wise-lloa-max-v1.2.1", map[string]string{
		"doc:1": "genuine_review", "doc:2": "non_review",
		"doc:3": "genuine_review", "doc:4": "mixed",
	})
	seedCompareVersion(t, repo, "vB", "wisenut/wise-lloa-ultra-v1.1.0", map[string]string{
		"doc:1": "genuine_review", "doc:2": "non_review",
		"doc:3": "non_review", "doc:4": "genuine_review",
	})

	view, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vB", 100, 0)
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
	if view.VersionA.Model != "wisenut/wise-lloa-max-v1.2.1" || view.VersionB.Model != "wisenut/wise-lloa-ultra-v1.1.0" {
		t.Fatalf("models: A=%q B=%q", view.VersionA.Model, view.VersionB.Model)
	}
	// confusion: d1 genuine→genuine, d2 non→non, d3 genuine→non, d4 mixed→genuine.
	gi := tierIndex(t, view, "genuine_review")
	ni := tierIndex(t, view, "non_review")
	mi := tierIndex(t, view, "mixed")
	if view.Confusion[gi][gi] != 1 {
		t.Fatalf("confusion[genuine][genuine]=%d, want 1", view.Confusion[gi][gi])
	}
	if view.Confusion[ni][ni] != 1 {
		t.Fatalf("confusion[non][non]=%d, want 1", view.Confusion[ni][ni])
	}
	if view.Confusion[gi][ni] != 1 { // d3: A genuine, B non
		t.Fatalf("confusion[genuine][non]=%d, want 1", view.Confusion[gi][ni])
	}
	if view.Confusion[mi][gi] != 1 { // d4: A mixed, B genuine
		t.Fatalf("confusion[mixed][genuine]=%d, want 1", view.Confusion[mi][gi])
	}
	// 불일치 2건(d3,d4), doc_id 정렬.
	if view.DisagreementsTotal != 2 || len(view.Disagreements) != 2 {
		t.Fatalf("disagreements total=%d len=%d, want 2/2", view.DisagreementsTotal, len(view.Disagreements))
	}
	if view.Disagreements[0].DocID != "doc:3" {
		t.Fatalf("first disagreement=%q, want doc:3", view.Disagreements[0].DocID)
	}
	if view.Disagreements[0].AGenuineness != "genuine_review" || view.Disagreements[0].BGenuineness != "non_review" {
		t.Fatalf("doc:3 labels A=%q B=%q", view.Disagreements[0].AGenuineness, view.Disagreements[0].BGenuineness)
	}
}

func TestCompareDocGenuineness_OnlyInOneSide(t *testing.T) {
	svc, repo := newCompareService(t)
	seedCompareVersion(t, repo, "vA", "m-a", map[string]string{"doc:1": "genuine_review", "doc:2": "non_review"})
	seedCompareVersion(t, repo, "vB", "m-b", map[string]string{"doc:1": "genuine_review", "doc:3": "non_review"})

	view, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vB", 100, 0)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if view.Compared != 1 || view.Matched != 1 {
		t.Fatalf("compared=%d matched=%d, want 1/1", view.Compared, view.Matched)
	}
	if view.OnlyInA != 1 || view.OnlyInB != 1 { // doc:2 only A, doc:3 only B
		t.Fatalf("onlyA=%d onlyB=%d, want 1/1", view.OnlyInA, view.OnlyInB)
	}
}

// override는 모델 비교값을 오염시키지 않고(원본 모델 라벨로 비교) 정답 힌트로만 노출.
func TestCompareDocGenuineness_OverrideIsGroundTruthHint(t *testing.T) {
	svc, repo := newCompareService(t)
	seedCompareVersion(t, repo, "vA", "m-a", map[string]string{"doc:1": "genuine_review"})
	seedCompareVersion(t, repo, "vB", "m-b", map[string]string{"doc:1": "non_review"})
	// 사람이 vA에서 doc:1을 non_review로 보정(정답=non_review, 즉 B가 맞음).
	if err := repo.UpsertDocGenuinenessOverride(domain.DocGenuinenessOverride{
		ProjectID: "p1", DatasetID: "d1", DatasetVersionID: "vA", DocID: "doc:1",
		OriginalGenuineness: "genuine_review", OverrideGenuineness: "non_review",
		OverrideReason: "광고",
	}); err != nil {
		t.Fatalf("upsert override: %v", err)
	}

	view, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vB", 100, 0)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	// 비교는 여전히 불일치(A 원본 genuine_review vs B non_review) — override로 일치 처리되면 안 됨.
	if view.Compared != 1 || view.Matched != 0 {
		t.Fatalf("compared=%d matched=%d, want 1/0 (override must not mask raw disagreement)", view.Compared, view.Matched)
	}
	if len(view.Disagreements) != 1 {
		t.Fatalf("disagreements=%d, want 1", len(view.Disagreements))
	}
	d := view.Disagreements[0]
	if d.AGenuineness != "genuine_review" {
		t.Fatalf("A raw label=%q, want genuine_review (not override)", d.AGenuineness)
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
		b[id] = "non_review" // 전부 불일치
	}
	seedCompareVersion(t, repo, "vA", "m-a", a)
	seedCompareVersion(t, repo, "vB", "m-b", b)

	view, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vB", 2, 2)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if view.DisagreementsTotal != 5 {
		t.Fatalf("total=%d, want 5", view.DisagreementsTotal)
	}
	if len(view.Disagreements) != 2 {
		t.Fatalf("page len=%d, want 2", len(view.Disagreements))
	}
	if view.Disagreements[0].DocID != "doc:2" { // 정렬 후 offset 2
		t.Fatalf("page[0]=%q, want doc:2", view.Disagreements[0].DocID)
	}
}

func TestCompareDocGenuineness_Validation(t *testing.T) {
	svc, repo := newCompareService(t)
	seedCompareVersion(t, repo, "vA", "m-a", map[string]string{"doc:1": "genuine_review"})

	// 같은 버전.
	if _, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vA", 100, 0); err == nil {
		t.Fatal("same version should error")
	}
	// 없는 버전.
	_, err := svc.CompareDocGenuineness("p1", "d1", "vA", "vMissing", 100, 0)
	var notFound ErrNotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("missing version should be ErrNotFound, got %v", err)
	}
	// 진성 분류 미완료 버전.
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "vRaw", DatasetID: "d1", ProjectID: "p1", StorageURI: "/tmp/x.csv",
		Metadata: map[string]any{},
	}); err != nil {
		t.Fatalf("save raw version: %v", err)
	}
	_, err = svc.CompareDocGenuineness("p1", "d1", "vA", "vRaw", 100, 0)
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("not-ready version should be ErrInvalidArgument, got %v", err)
	}
}
