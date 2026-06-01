package service

import (
	"reflect"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-05-22 (옵션 α1) — Dataset에 dataset-level metadata 필드를
// 도입하면서 CreateDataset이 metadata를 받고 UpdateDatasetMetadata가 top-level
// key 단위 merge로 patch한다. 본 test는 그 계약을 잠근다.

func newDatasetServiceWithProject(t *testing.T) (*DatasetService, store.Repository, domain.Project) {
	t.Helper()
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())
	project := domain.Project{ProjectID: "project-meta", Name: "α1", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	return service, repository, project
}

func TestCreateDatasetPersistsMetadata(t *testing.T) {
	service, repository, project := newDatasetServiceWithProject(t)

	created, err := service.CreateDataset(project.ProjectID, domain.DatasetCreateRequest{
		Name: "festival",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{
				"subject_type":         "festival",
				"subject_name":         "강릉 국가유산야행",
				"subject_aliases":      []any{"문화유산야행", "문화재야행", "강릉야행"},
				"recruitment_keywords": []any{"서포터즈", "푸드트럭"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected CreateDataset error: %v", err)
	}

	// store에서 다시 읽어 round-trip 검증
	reloaded, err := repository.GetDataset(project.ProjectID, created.DatasetID)
	if err != nil {
		t.Fatalf("unexpected GetDataset error: %v", err)
	}
	dg, ok := reloaded.Metadata["doc_genuineness"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.doc_genuineness map, got %#v", reloaded.Metadata["doc_genuineness"])
	}
	if dg["subject_name"] != "강릉 국가유산야행" {
		t.Fatalf("subject_name not preserved: %v", dg["subject_name"])
	}
}

func TestCreateDatasetWithoutMetadataYieldsEmpty(t *testing.T) {
	service, repository, project := newDatasetServiceWithProject(t)

	created, err := service.CreateDataset(project.ProjectID, domain.DatasetCreateRequest{Name: "no-meta"})
	if err != nil {
		t.Fatalf("unexpected CreateDataset error: %v", err)
	}

	reloaded, err := repository.GetDataset(project.ProjectID, created.DatasetID)
	if err != nil {
		t.Fatalf("unexpected GetDataset error: %v", err)
	}
	if len(reloaded.Metadata) != 0 {
		t.Fatalf("expected empty metadata, got %#v", reloaded.Metadata)
	}
}

func TestUpdateDatasetMetadataMergesTopLevelKeys(t *testing.T) {
	service, _, project := newDatasetServiceWithProject(t)

	created, err := service.CreateDataset(project.ProjectID, domain.DatasetCreateRequest{
		Name: "merge",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{
				"subject_name": "초기 subject",
			},
			"unrelated_key": "보존되어야 함",
		},
	})
	if err != nil {
		t.Fatalf("unexpected CreateDataset error: %v", err)
	}

	updated, err := service.UpdateDatasetMetadata(project.ProjectID, created.DatasetID, map[string]any{
		"doc_genuineness": map[string]any{
			"subject_name":    "새 subject",
			"subject_aliases": []any{"별칭1", "별칭2"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected UpdateDatasetMetadata error: %v", err)
	}

	// doc_genuineness는 통째 overwrite — `subject_name`은 새 값, 옛 `subject_name`은 사라지고
	// 동시에 `subject_aliases`가 새로 등장.
	dg, ok := updated.Metadata["doc_genuineness"].(map[string]any)
	if !ok {
		t.Fatalf("expected updated doc_genuineness map, got %#v", updated.Metadata["doc_genuineness"])
	}
	if dg["subject_name"] != "새 subject" {
		t.Fatalf("subject_name not overwritten: %v", dg["subject_name"])
	}
	aliases, ok := dg["subject_aliases"].([]any)
	if !ok || !reflect.DeepEqual(aliases, []any{"별칭1", "별칭2"}) {
		t.Fatalf("subject_aliases not set: %#v", dg["subject_aliases"])
	}

	// 다른 top-level key는 보존되어야 함 — patch에 없으므로.
	if updated.Metadata["unrelated_key"] != "보존되어야 함" {
		t.Fatalf("unrelated_key dropped: %v", updated.Metadata["unrelated_key"])
	}
}

func TestUpdateDatasetMetadataNilPatchRejected(t *testing.T) {
	service, _, project := newDatasetServiceWithProject(t)

	created, err := service.CreateDataset(project.ProjectID, domain.DatasetCreateRequest{Name: "nil"})
	if err != nil {
		t.Fatalf("unexpected CreateDataset error: %v", err)
	}

	if _, err := service.UpdateDatasetMetadata(project.ProjectID, created.DatasetID, nil); err == nil {
		t.Fatalf("expected error for nil patch")
	}
}

func TestUpdateDatasetMetadataNotFound(t *testing.T) {
	service, _, project := newDatasetServiceWithProject(t)

	if _, err := service.UpdateDatasetMetadata(project.ProjectID, "does-not-exist", map[string]any{
		"doc_genuineness": map[string]any{"subject_name": "x"},
	}); err == nil {
		t.Fatalf("expected NotFound error")
	}
}
