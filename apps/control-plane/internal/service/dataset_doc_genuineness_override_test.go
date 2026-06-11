package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 진성 보정 overlay 합성 — items effective 교체 + summary 재집계 + 경계 cross.
func TestApplyDocGenuinenessOverrides(t *testing.T) {
	view := domain.DatasetArtifactView{
		Items: []map[string]any{
			{"doc_id": "d1", "genuineness": "non_review", "reason": "광고글"},
			{"doc_id": "d2", "genuineness": "genuine_review", "reason": "방문 후기"},
		},
		Summary: map[string]any{
			"total":       3,
			"genuineness": map[string]int{"genuine_review": 1, "non_review": 1, "uncertain": 1},
		},
	}
	overrides := []domain.DocGenuinenessOverride{
		{DocID: "d1", OriginalGenuineness: "non_review", OriginalReason: "광고글", OverrideGenuineness: "genuine_review", OverrideReason: "실제 방문 표현 있음"},
	}
	crossed := applyDocGenuinenessOverrides(&view, overrides)
	if !crossed {
		t.Fatal("non_review→genuine_review는 clause_label 경계 cross여야 한다")
	}
	it := view.Items[0]
	// effective 필드(genuineness/reason)는 override 값으로 교체.
	if it["genuineness"] != "genuine_review" || it["reason"] != "실제 방문 표현 있음" {
		t.Fatalf("effective 교체 잘못: %+v", it)
	}
	if it["original_genuineness"] != "non_review" || it["original_reason"] != "광고글" {
		t.Fatalf("original snapshot 잘못: %+v", it)
	}
	if it["override_genuineness"] != "genuine_review" || it["override_reason"] != "실제 방문 표현 있음" || it["is_overridden"] != true {
		t.Fatalf("override 필드 잘못: %+v", it)
	}
	counts := view.Summary["genuineness"].(map[string]int)
	if counts["genuine_review"] != 2 || counts["non_review"] != 0 || counts["uncertain"] != 1 {
		t.Fatalf("summary 재집계 잘못: %+v", counts)
	}
	if view.Summary["override_count"] != 1 || view.Summary["downstream_boundary_crossed"] != true {
		t.Errorf("summary 플래그 잘못: %+v", view.Summary)
	}
}

// uncertain↔non_review는 둘 다 clause_label 제외 → 경계 cross 아님.
func TestApplyDocGenuinenessOverridesNoCross(t *testing.T) {
	view := domain.DatasetArtifactView{
		Items:   []map[string]any{{"doc_id": "d1", "genuineness": "uncertain"}},
		Summary: map[string]any{"genuineness": map[string]int{"uncertain": 1, "non_review": 0}},
	}
	crossed := applyDocGenuinenessOverrides(&view, []domain.DocGenuinenessOverride{
		{DocID: "d1", OriginalGenuineness: "uncertain", OverrideGenuineness: "non_review"},
	})
	if crossed {
		t.Fatal("uncertain→non_review는 cross 아님")
	}
}

func TestDocGenuinenessOriginalForDoc(t *testing.T) {
	jsonlPath, _ := setupDocGenuinenessFixture(t, []string{"v:row:0", "v:row:1"}, []string{"v:row:0", "v:row:1"})
	// fixture: 짝수 index genuine_review(valid review text), 홀수 non_review(ad post).
	label, reason, found, err := docGenuinenessOriginalForDoc(jsonlPath, "v:row:1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found || label != "non_review" || reason != "ad post" {
		t.Fatalf("v:row:1 = %q/%q found=%v, want non_review/ad post", label, reason, found)
	}
	_, _, found, err = docGenuinenessOriginalForDoc(jsonlPath, "missing")
	if err != nil {
		t.Fatalf("missing lookup: %v", err)
	}
	if found {
		t.Fatal("없는 doc_id는 found=false여야 한다")
	}
}

func seedGenuinenessVersion(t *testing.T, svc *DatasetService, repo *store.MemoryStore, jsonlPath string) (string, string, string) {
	t.Helper()
	if err := repo.SaveProject(domain.Project{ProjectID: "p1", Name: "P", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{DatasetID: "d1", ProjectID: "p1", Name: "ds", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "v1", DatasetID: "d1", ProjectID: "p1",
		StorageURI: "/tmp/none.csv",
		Metadata: map[string]any{
			"doc_genuineness_ref":    jsonlPath,
			"doc_genuineness_status": "ready",
		},
	}); err != nil {
		t.Fatalf("save version: %v", err)
	}
	return "p1", "d1", "v1"
}

func TestSetAndDeleteDocGenuinenessOverrideFlow(t *testing.T) {
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	jsonlPath, _ := setupDocGenuinenessFixture(t, []string{"v:row:0", "v:row:1"}, []string{"v:row:0", "v:row:1"})
	pid, did, vid := seedGenuinenessVersion(t, svc, repo, jsonlPath)

	// non_review(v:row:1, reason "ad post") → genuine_review 보정. 사유 미입력 →
	// 서버 기본값.
	ov, err := svc.SetDocGenuinenessOverride(pid, did, vid, "v:row:1", domain.DocGenuinenessOverrideRequest{
		Genuineness: "genuine_review",
	})
	if err != nil {
		t.Fatalf("SetOverride: %v", err)
	}
	if ov.OriginalGenuineness != "non_review" || ov.OriginalReason != "ad post" || ov.OverrideGenuineness != "genuine_review" {
		t.Fatalf("override snapshot 잘못: %+v", ov)
	}
	if ov.OverrideReason != "운영자 수동 수정" {
		t.Fatalf("사유 미입력 시 기본값 기대, got %q", ov.OverrideReason)
	}
	list, _ := repo.ListDocGenuinenessOverrides(pid, vid)
	if len(list) != 1 || list[0].OriginalReason != "ad post" {
		t.Fatalf("store 보정 1건 + original_reason snapshot 기대, got %+v", list)
	}

	// PATCH는 항상 upsert(원본값으로 set해도 해제 아님 — 해제는 DELETE).
	if _, err := svc.SetDocGenuinenessOverride(pid, did, vid, "v:row:1", domain.DocGenuinenessOverrideRequest{
		Genuineness: "non_review", Reason: "다시 확인함",
	}); err != nil {
		t.Fatalf("SetOverride(재보정): %v", err)
	}
	list2, _ := repo.ListDocGenuinenessOverrides(pid, vid)
	if len(list2) != 1 || list2[0].OverrideGenuineness != "non_review" || list2[0].OverrideReason != "다시 확인함" {
		t.Fatalf("재보정 upsert 기대, got %+v", list2)
	}

	// DELETE 되돌리기.
	if err := svc.DeleteDocGenuinenessOverride(pid, did, vid, "v:row:1"); err != nil {
		t.Fatalf("DeleteOverride: %v", err)
	}
	if err := svc.DeleteDocGenuinenessOverride(pid, did, vid, "v:row:1"); err == nil {
		t.Fatal("이미 없는 보정 DELETE는 404여야")
	}

	// artifact에 없는 doc → 404.
	if _, err := svc.SetDocGenuinenessOverride(pid, did, vid, "nope", domain.DocGenuinenessOverrideRequest{Genuineness: "genuine_review"}); err == nil {
		t.Fatal("없는 doc_id set은 404여야")
	}
}

func TestSetDocGenuinenessOverrideRejectsInvalidTier(t *testing.T) {
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	_, err := svc.SetDocGenuinenessOverride("p1", "d1", "v1", "doc", domain.DocGenuinenessOverrideRequest{Genuineness: "bogus"})
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("invalid tier: want ErrInvalidArgument, got %v", err)
	}
}
