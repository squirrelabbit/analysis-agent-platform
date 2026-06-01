package store

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-28 (B1): dataset_version_artifacts UPSERT no-op update
// 방지 잠금. GET dataset_version 흐름이 attachDatasetVersionArtifacts를 호출
// 해도 payload field가 동일하면 updated_at이 갱신되지 않아야 한다.

func newTestDatasetVersionForArtifactSync() domain.DatasetVersion {
	cleanedURI := "/tmp/cleaned.parquet"
	return domain.DatasetVersion{
		DatasetVersionID: "v-1",
		DatasetID:        "11111111-1111-1111-1111-111111111111",
		ProjectID:        "22222222-2222-2222-2222-222222222222",
		StorageURI:       "/tmp/source.csv",
		DataType:         "unstructured",
		CleanStatus:      "ready",
		CleanURI:         &cleanedURI,
		Metadata: map[string]any{
			"clean_uri":          cleanedURI,
			"clean_completed_at": "2026-05-28T11:00:00Z",
			"clean_summary":      map[string]any{"kept": 100, "dropped": 0},
		},
	}
}

func TestSaveDatasetVersionArtifactsIdempotentKeepsUpdatedAt(t *testing.T) {
	store := NewMemoryStore()
	version := newTestDatasetVersionForArtifactSync()
	if err := store.SaveDataset(domain.Dataset{
		DatasetID: version.DatasetID,
		ProjectID: version.ProjectID,
		Name:      "test",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveDataset: %v", err)
	}
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion first: %v", err)
	}
	first, err := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("ListDatasetVersionArtifacts first: %v", err)
	}
	if len(first) == 0 {
		t.Fatalf("expected artifact rows after first save")
	}

	// 의도적으로 시계가 흐르도록 잠시 대기 후 동일 version 재save.
	time.Sleep(20 * time.Millisecond)
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion second: %v", err)
	}
	second, err := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("ListDatasetVersionArtifacts second: %v", err)
	}
	if len(second) != len(first) {
		t.Fatalf("row count changed: %d → %d", len(first), len(second))
	}
	firstByType := map[string]domain.DatasetVersionArtifact{}
	for _, a := range first {
		firstByType[a.ArtifactType] = a
	}
	for _, a := range second {
		prev, ok := firstByType[a.ArtifactType]
		if !ok {
			t.Fatalf("artifact_type %q present in second but not first", a.ArtifactType)
		}
		if !prev.UpdatedAt.Equal(a.UpdatedAt) {
			t.Errorf("artifact_type=%q updated_at touched on no-op save: %v → %v", a.ArtifactType, prev.UpdatedAt, a.UpdatedAt)
		}
		if !prev.CreatedAt.Equal(a.CreatedAt) {
			t.Errorf("artifact_type=%q created_at touched: %v → %v", a.ArtifactType, prev.CreatedAt, a.CreatedAt)
		}
	}
}

func TestSaveDatasetVersionArtifactsUpdatesWhenURIChanges(t *testing.T) {
	store := NewMemoryStore()
	version := newTestDatasetVersionForArtifactSync()
	if err := store.SaveDataset(domain.Dataset{
		DatasetID: version.DatasetID,
		ProjectID: version.ProjectID,
		Name:      "test",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveDataset: %v", err)
	}
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion first: %v", err)
	}
	first, _ := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)
	firstByType := map[string]domain.DatasetVersionArtifact{}
	for _, a := range first {
		firstByType[a.ArtifactType] = a
	}

	// 변경: clean_uri 갱신
	time.Sleep(20 * time.Millisecond)
	newURI := "/tmp/cleaned-v2.parquet"
	version.CleanURI = &newURI
	version.Metadata["clean_uri"] = newURI
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion second: %v", err)
	}
	second, _ := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)

	var cleanRow *domain.DatasetVersionArtifact
	for i := range second {
		if second[i].ArtifactType == "clean" {
			cleanRow = &second[i]
			break
		}
	}
	if cleanRow == nil {
		t.Fatalf("clean artifact row not found after URI change")
	}
	prev := firstByType["clean"]
	if cleanRow.URI != newURI {
		t.Errorf("expected URI to be %q, got %q", newURI, cleanRow.URI)
	}
	if !cleanRow.UpdatedAt.After(prev.UpdatedAt) {
		t.Errorf("expected updated_at to advance after URI change: prev=%v, next=%v", prev.UpdatedAt, cleanRow.UpdatedAt)
	}
	if !prev.CreatedAt.Equal(cleanRow.CreatedAt) {
		t.Errorf("created_at must be preserved: prev=%v, next=%v", prev.CreatedAt, cleanRow.CreatedAt)
	}

	// clean 외 row는 그대로 — no-op 보장
	for _, a := range second {
		if a.ArtifactType == "clean" {
			continue
		}
		if other := firstByType[a.ArtifactType]; !other.UpdatedAt.Equal(a.UpdatedAt) {
			t.Errorf("unrelated artifact_type=%q updated_at must not advance: %v → %v", a.ArtifactType, other.UpdatedAt, a.UpdatedAt)
		}
	}
}

func TestSaveDatasetVersionArtifactsUpdatesWhenSummaryChanges(t *testing.T) {
	store := NewMemoryStore()
	version := newTestDatasetVersionForArtifactSync()
	if err := store.SaveDataset(domain.Dataset{
		DatasetID: version.DatasetID,
		ProjectID: version.ProjectID,
		Name:      "test",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveDataset: %v", err)
	}
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion first: %v", err)
	}
	first, _ := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)
	firstByType := map[string]domain.DatasetVersionArtifact{}
	for _, a := range first {
		firstByType[a.ArtifactType] = a
	}

	time.Sleep(20 * time.Millisecond)
	// summary 변경 (kept count 다름) → clean artifact summary 갱신
	version.Metadata["clean_summary"] = map[string]any{"kept": 200, "dropped": 5}
	if err := store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("SaveDatasetVersion second: %v", err)
	}
	second, _ := store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)

	for _, a := range second {
		if a.ArtifactType != "clean" {
			continue
		}
		prev := firstByType["clean"]
		if !a.UpdatedAt.After(prev.UpdatedAt) {
			t.Errorf("expected updated_at to advance after summary change: prev=%v, next=%v", prev.UpdatedAt, a.UpdatedAt)
		}
	}
}

func TestDatasetVersionArtifactPayloadEqualHelper(t *testing.T) {
	base := domain.DatasetVersionArtifact{
		ArtifactID:       "v:clean",
		ProjectID:        "p",
		DatasetID:        "d",
		DatasetVersionID: "v",
		ArtifactType:     "clean",
		Stage:            "clean",
		Status:           "ready",
		URI:              "/tmp/a.parquet",
		Format:           "parquet",
		Model:            "",
		PromptVersion:    "",
		Summary:          map[string]any{"kept": 100},
		Metadata:         map[string]any{"ref": "/tmp/a.parquet"},
	}
	// 같은 payload, updated_at 만 다름 → equal
	otherSameTime := base
	otherSameTime.CreatedAt = time.Now().Add(-time.Hour)
	otherSameTime.UpdatedAt = time.Now()
	if !datasetVersionArtifactPayloadEqual(base, otherSameTime) {
		t.Errorf("expected payload equal when only timestamps differ")
	}
	// URI 변경 → not equal
	uriChanged := base
	uriChanged.URI = "/tmp/b.parquet"
	if datasetVersionArtifactPayloadEqual(base, uriChanged) {
		t.Errorf("expected payload not equal when URI changes")
	}
	// summary 변경 → not equal
	summaryChanged := base
	summaryChanged.Summary = map[string]any{"kept": 200}
	if datasetVersionArtifactPayloadEqual(base, summaryChanged) {
		t.Errorf("expected payload not equal when summary changes")
	}
	// nil/empty map 비교 (defaultMetadataMap이 nil → {} 정규화)
	nilMeta := base
	nilMeta.Summary = nil
	emptyMeta := base
	emptyMeta.Summary = map[string]any{}
	if !datasetVersionArtifactPayloadEqual(nilMeta, emptyMeta) {
		t.Errorf("nil and empty map should be treated as equal payload")
	}
}
