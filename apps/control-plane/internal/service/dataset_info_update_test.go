package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-06-05 — 데이터셋 이름/설명 수정(PATCH /datasets/{id}) 잠금.
func newInfoUpdateFixture(t *testing.T) (*DatasetService, string) {
	t.Helper()
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", t.TempDir())
	if err := memory.SaveProject(domain.Project{ProjectID: "p1", Name: "p"}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	desc := "원래 설명"
	if err := memory.SaveDataset(domain.Dataset{
		ProjectID:   "p1",
		DatasetID:   "d1",
		Name:        "원래 이름",
		Description: &desc,
		DataType:    "unstructured",
	}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	return svc, "d1"
}

func strptr(v string) *string { return &v }

func TestUpdateDatasetInfo_NameAndDescription(t *testing.T) {
	svc, did := newInfoUpdateFixture(t)

	updated, err := svc.UpdateDatasetInfo("p1", did, domain.DatasetInfoUpdateRequest{
		Name:        strptr("  새 이름  "),
		Description: strptr("새 설명"),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name != "새 이름" {
		t.Fatalf("name trim/update mismatch: %q", updated.Name)
	}
	if updated.Description == nil || *updated.Description != "새 설명" {
		t.Fatalf("description mismatch: %v", updated.Description)
	}

	// 저장 영속 확인.
	got, err := svc.GetDataset("p1", did)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "새 이름" || got.Description == nil || *got.Description != "새 설명" {
		t.Fatalf("persisted mismatch: name=%q desc=%v", got.Name, got.Description)
	}
}

func TestUpdateDatasetInfo_PartialDescriptionOnly(t *testing.T) {
	svc, did := newInfoUpdateFixture(t)

	updated, err := svc.UpdateDatasetInfo("p1", did, domain.DatasetInfoUpdateRequest{
		Description: strptr("설명만 변경"),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	// name 미지정 → 기존 이름 보존.
	if updated.Name != "원래 이름" {
		t.Fatalf("name should be preserved, got %q", updated.Name)
	}
	if updated.Description == nil || *updated.Description != "설명만 변경" {
		t.Fatalf("description mismatch: %v", updated.Description)
	}
}

func TestUpdateDatasetInfo_EmptyNameRejected(t *testing.T) {
	svc, did := newInfoUpdateFixture(t)

	if _, err := svc.UpdateDatasetInfo("p1", did, domain.DatasetInfoUpdateRequest{
		Name: strptr("   "),
	}); err == nil {
		t.Fatal("empty name should be rejected")
	}
}

func TestUpdateDatasetInfo_NoFieldsRejected(t *testing.T) {
	svc, did := newInfoUpdateFixture(t)

	if _, err := svc.UpdateDatasetInfo("p1", did, domain.DatasetInfoUpdateRequest{}); err == nil {
		t.Fatal("no fields should be rejected")
	}
}

func TestUpdateDatasetInfo_NotFound(t *testing.T) {
	svc, _ := newInfoUpdateFixture(t)

	if _, err := svc.UpdateDatasetInfo("p1", "does-not-exist", domain.DatasetInfoUpdateRequest{
		Name: strptr("x"),
	}); err == nil {
		t.Fatal("missing dataset should error")
	}
}
