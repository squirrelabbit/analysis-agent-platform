package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

// silverone 2026-06-04 — dataset version_number (생성순 1-based, 삭제 stable) 잠금.
func newVersionNumberFixture(t *testing.T) *DatasetService {
	t.Helper()
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", t.TempDir())
	if err := memory.SaveProject(domain.Project{ProjectID: "p1", Name: "p"}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := memory.SaveDataset(domain.Dataset{ProjectID: "p1", DatasetID: "d1", Name: "d", DataType: "unstructured"}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	return svc
}

func createVersion(t *testing.T, svc *DatasetService) string {
	t.Helper()
	// StorageURI는 존재하지 않는 경로 — summary/clean은 skip되고 version_number 부여만 검증.
	v, err := svc.CreateDatasetVersion("p1", "d1", domain.DatasetVersionCreateRequest{
		StorageURI: "/tmp/does-not-exist.csv",
	})
	if err != nil {
		t.Fatalf("create version: %v", err)
	}
	return v.DatasetVersionID
}

func listVersionNumbers(t *testing.T, svc *DatasetService) map[string]int {
	t.Helper()
	resp, err := svc.ListDatasetVersions("p1", "d1")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	out := map[string]int{}
	for _, it := range resp.Items {
		out[it.DatasetVersionID] = it.VersionNumber
	}
	return out
}

func TestDatasetVersionNumber_CreationOrder(t *testing.T) {
	svc := newVersionNumberFixture(t)
	v1 := createVersion(t, svc)
	v2 := createVersion(t, svc)
	v3 := createVersion(t, svc)

	// 1) 목록 응답에 생성순 1,2,3.
	nums := listVersionNumbers(t, svc)
	if nums[v1] != 1 || nums[v2] != 2 || nums[v3] != 3 {
		t.Fatalf("list version_number mismatch: v1=%d v2=%d v3=%d", nums[v1], nums[v2], nums[v3])
	}

	// 2) detail에서도 동일 번호. 최신 버전 = 3.
	for id, want := range map[string]int{v1: 1, v2: 2, v3: 3} {
		d, err := svc.GetDatasetVersionDetail("p1", "d1", id)
		if err != nil {
			t.Fatalf("detail %s: %v", id, err)
		}
		if d.VersionNumber != want {
			t.Fatalf("detail version_number %s: got %d want %d", id, d.VersionNumber, want)
		}
	}
}

func TestDatasetVersionNumber_StableAfterDelete(t *testing.T) {
	svc := newVersionNumberFixture(t)
	v1 := createVersion(t, svc)
	v2 := createVersion(t, svc)
	v3 := createVersion(t, svc)

	// 중간 버전(v2) 삭제 → 남은 v1=1, v3=3 (재계산되어 1,2가 되면 안 됨).
	if err := svc.DeleteDatasetVersion("p1", "d1", v2); err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	nums := listVersionNumbers(t, svc)
	if nums[v1] != 1 {
		t.Fatalf("v1 should stay 1 after delete, got %d", nums[v1])
	}
	if nums[v3] != 3 {
		t.Fatalf("v3 should stay 3 after delete (no renumber), got %d", nums[v3])
	}

	// 삭제 후 새 버전 → 기존 최대(3)+1 = 4.
	v4 := createVersion(t, svc)
	nums = listVersionNumbers(t, svc)
	if nums[v4] != 4 {
		t.Fatalf("new version after delete should be 4, got %d", nums[v4])
	}
}

func TestDatasetVersionNumber_ActiveVersionNumber(t *testing.T) {
	svc := newVersionNumberFixture(t)
	_ = createVersion(t, svc)
	_ = createVersion(t, svc)
	v3 := createVersion(t, svc)

	// v3를 active로 설정 → active version의 detail version_number = 3 (프론트가 "활성 v3" 표시).
	if _, err := svc.ActivateDatasetVersion("p1", "d1", v3); err != nil {
		t.Fatalf("activate v3: %v", err)
	}
	d, err := svc.GetDatasetVersionDetail("p1", "d1", v3)
	if err != nil {
		t.Fatalf("detail: %v", err)
	}
	if !d.IsActive || d.VersionNumber != 3 {
		t.Fatalf("active version detail: is_active=%v version_number=%d (want true/3)", d.IsActive, d.VersionNumber)
	}
}

// legacy(미저장) row는 read-time created_at rank fallback으로 번호가 매겨진다.
func TestDatasetVersionNumber_LegacyFallback(t *testing.T) {
	svc := newVersionNumberFixture(t)
	// metadata에 version_number 없이 직접 저장(= 옛 데이터 시뮬레이션).
	for i, ts := range []string{"2026-05-12T09:24:00Z", "2026-05-20T14:12:00Z", "2026-05-28T17:41:00Z"} {
		parsed := mustTime(t, ts)
		err := svc.store.SaveDatasetVersion(domain.DatasetVersion{
			DatasetVersionID: "legacy-" + string(rune('a'+i)),
			DatasetID:        "d1",
			ProjectID:        "p1",
			DataType:         "unstructured",
			Metadata:         map[string]any{}, // version_number 없음
			CreatedAt:        parsed,
		})
		if err != nil {
			t.Fatalf("save legacy %d: %v", i, err)
		}
	}
	nums := listVersionNumbers(t, svc)
	if nums["legacy-a"] != 1 || nums["legacy-b"] != 2 || nums["legacy-c"] != 3 {
		t.Fatalf("legacy fallback ranks: %v", nums)
	}
}
