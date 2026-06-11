package service

import (
	"encoding/json"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func newReportSvc(t *testing.T) *DatasetService {
	t.Helper()
	memory := store.NewMemoryStore()
	if err := memory.SaveProject(domain.Project{ProjectID: "p1", Name: "P"}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	return NewDatasetService(memory, "", "", "")
}

func TestReportCrudLifecycle(t *testing.T) {
	svc := newReportSvc(t)
	blocks := json.RawMessage(`[{"block_id":"b1","type":"chart","title":"키워드 TOP","snapshot":{"rows":[{"k":"드론","v":36}]}}]`)

	created, err := svc.CreateReport("p1", domain.ReportCreateRequest{
		Title: "여름축제 보고서", Blocks: blocks,
	})
	if err != nil {
		t.Fatalf("CreateReport: %v", err)
	}
	if created.ReportID == "" || created.Title != "여름축제 보고서" {
		t.Fatalf("created unexpected: %+v", created)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Error("timestamps not set")
	}

	// 목록 — summary에 block_count.
	list, err := svc.ListReports("p1")
	if err != nil {
		t.Fatalf("ListReports: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].BlockCount != 1 {
		t.Fatalf("list want 1 item block_count 1, got %+v", list.Items)
	}

	// 단건 — blocks 보존.
	got, err := svc.GetReport("p1", created.ReportID)
	if err != nil {
		t.Fatalf("GetReport: %v", err)
	}
	if string(got.Blocks) != string(blocks) {
		t.Errorf("blocks snapshot mismatch: %s", got.Blocks)
	}

	// 갱신 — title + blocks 교체.
	updated, err := svc.UpdateReport("p1", created.ReportID, domain.ReportUpdateRequest{
		Title: "수정본", Blocks: json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatalf("UpdateReport: %v", err)
	}
	if updated.Title != "수정본" || string(updated.Blocks) != "[]" {
		t.Errorf("update not applied: %+v", updated)
	}

	// 삭제 → 목록 0.
	if err := svc.DeleteReport("p1", created.ReportID); err != nil {
		t.Fatalf("DeleteReport: %v", err)
	}
	after, _ := svc.ListReports("p1")
	if len(after.Items) != 0 {
		t.Errorf("after delete want 0, got %d", len(after.Items))
	}
}

func TestCreateReportDefaultsAndEmptyBlocks(t *testing.T) {
	svc := newReportSvc(t)
	// title/blocks 미지정 → 기본 제목 + 빈 배열.
	created, err := svc.CreateReport("p1", domain.ReportCreateRequest{})
	if err != nil {
		t.Fatalf("CreateReport: %v", err)
	}
	if created.Title != "제목 없는 보고서" {
		t.Errorf("default title: got %q", created.Title)
	}
	if string(created.Blocks) != "[]" {
		t.Errorf("empty blocks normalize: got %s", created.Blocks)
	}
}

func TestCreateReportRejectsNonArrayBlocks(t *testing.T) {
	svc := newReportSvc(t)
	_, err := svc.CreateReport("p1", domain.ReportCreateRequest{
		Blocks: json.RawMessage(`{"not":"array"}`),
	})
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("non-array blocks: want ErrInvalidArgument, got %v", err)
	}
}

func TestReportNotFoundAndProjectScope(t *testing.T) {
	svc := newReportSvc(t)
	if _, err := svc.GetReport("p1", "missing"); err == nil {
		t.Fatal("GetReport missing: want error")
	}
	if _, err := svc.UpdateReport("p1", "missing", domain.ReportUpdateRequest{Title: "x"}); err == nil {
		t.Fatal("UpdateReport missing: want error")
	}
	if err := svc.DeleteReport("p1", "missing"); err == nil {
		t.Fatal("DeleteReport missing: want error")
	}

	// 다른 project의 보고서는 안 보인다.
	created, err := svc.CreateReport("p1", domain.ReportCreateRequest{Title: "p1 report"})
	if err != nil {
		t.Fatalf("CreateReport: %v", err)
	}
	if _, err := svc.GetReport("p_other", created.ReportID); err == nil {
		t.Error("cross-project GetReport should 404")
	}
}
