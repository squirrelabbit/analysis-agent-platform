package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 전처리 빌드 모델 선택 (2026-06-12) — model_id가 worker payload로 전달되고,
// allowlist 밖 모델은 job 생성 전에 400으로 거절되는지 잠금.

func modelSelectionFixture(t *testing.T) (*DatasetService, domain.Project, domain.Dataset, domain.DatasetVersion) {
	t.Helper()
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("save project: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "festival",
		DataType:  "unstructured",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{
				"subject_type": "festival",
				"subject_name": "강릉 국가유산야행",
			},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	cleanedURI := "/tmp/festival.cleaned.parquet"
	version := domain.DatasetVersion{
		DatasetVersionID: "version-1",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/festival.csv",
		DataType:         "unstructured",
		CleanStatus:      "ready",
		CleanURI:         &cleanedURI,
		Metadata:         map[string]any{},
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}
	return service, project, dataset, version
}

func modelSelectionWorkerStub(t *testing.T, artifactKey string, capturedPayload *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		*capturedPayload = payload
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"done"},
			"artifact": map[string]any{
				artifactKey: "/tmp/out.jsonl",
				"summary":   map[string]any{"model": "wisenut/wise-lloa-ultra-v1.1.0"},
			},
		})
	}))
}

func TestBuildDocGenuinenessPassesModelID(t *testing.T) {
	service, project, dataset, version := modelSelectionFixture(t)
	var payload map[string]any
	server := modelSelectionWorkerStub(t, "doc_genuineness_ref", &payload)
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	_, err := service.BuildDocGenuineness(
		project.ProjectID, dataset.DatasetID, version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{ModelID: strptr("wisenut/wise-lloa-ultra-v1.1.0")},
	)
	if err != nil {
		t.Fatalf("BuildDocGenuineness: %v", err)
	}
	if got, _ := payload["model_id"].(string); got != "wisenut/wise-lloa-ultra-v1.1.0" {
		t.Fatalf("payload model_id: got %q", got)
	}
}

func TestBuildDocGenuinenessOmitsModelIDByDefault(t *testing.T) {
	service, project, dataset, version := modelSelectionFixture(t)
	var payload map[string]any
	server := modelSelectionWorkerStub(t, "doc_genuineness_ref", &payload)
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	_, err := service.BuildDocGenuineness(
		project.ProjectID, dataset.DatasetID, version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
	)
	if err != nil {
		t.Fatalf("BuildDocGenuineness: %v", err)
	}
	if _, exists := payload["model_id"]; exists {
		t.Fatalf("model_id 미지정 시 payload에서 생략돼야 함: %+v", payload["model_id"])
	}
}

func TestBuildClauseLabelPassesModelID(t *testing.T) {
	service, project, dataset, version := modelSelectionFixture(t)
	var payload map[string]any
	server := modelSelectionWorkerStub(t, "clause_label_ref", &payload)
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	// include_genuineness=[] opt-out — doc_genuineness ready 없이 실행 가능.
	_, err := service.BuildClauseLabel(
		project.ProjectID, dataset.DatasetID, version.DatasetVersionID,
		domain.DatasetClauseLabelBuildRequest{
			IncludeGenuineness: []string{},
			ModelID:            strptr("wisenut/wise-lloa-ultra-v1.1.0"),
		},
	)
	if err != nil {
		t.Fatalf("BuildClauseLabel: %v", err)
	}
	if got, _ := payload["model_id"].(string); got != "wisenut/wise-lloa-ultra-v1.1.0" {
		t.Fatalf("payload model_id: got %q", got)
	}
}

func TestCreateDocGenuinenessJobRejectsUnknownModel(t *testing.T) {
	service, project, dataset, version := modelSelectionFixture(t)
	service.SetLLOAModelOptions([]domain.LLOAModelOption{
		{ModelID: "wisenut/wise-lloa-max-v1.2.1", Label: "LLOA Max 1.2.1", Default: true},
	})

	_, err := service.CreateDocGenuinenessJob(
		project.ProjectID, dataset.DatasetID, version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{ModelID: strptr("gpt-4o")},
		"api", "req-1",
	)
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCreateClauseLabelJobRejectsUnknownModel(t *testing.T) {
	service, project, dataset, version := modelSelectionFixture(t)
	service.SetLLOAModelOptions([]domain.LLOAModelOption{
		{ModelID: "wisenut/wise-lloa-max-v1.2.1", Label: "LLOA Max 1.2.1", Default: true},
	})

	_, err := service.CreateClauseLabelJob(
		project.ProjectID, dataset.DatasetID, version.DatasetVersionID,
		domain.DatasetClauseLabelBuildRequest{ModelID: strptr("gpt-4o")},
		"api", "req-1",
	)
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

// allowlist가 비어 있으면(LLOA_MODELS/LLOA_MODEL 모두 미설정) model_id 지정
// 자체를 거절한다 — env default만 사용.
func TestValidateLLOAModelID(t *testing.T) {
	service := NewDatasetService(store.NewMemoryStore(), "", t.TempDir(), t.TempDir())

	if err := service.validateLLOAModelID(nil); err != nil {
		t.Fatalf("nil model_id should pass: %v", err)
	}
	if err := service.validateLLOAModelID(strptr("  ")); err != nil {
		t.Fatalf("blank model_id should pass: %v", err)
	}
	if err := service.validateLLOAModelID(strptr("anything")); err == nil {
		t.Fatalf("empty allowlist should reject explicit model_id")
	}

	service.SetLLOAModelOptions([]domain.LLOAModelOption{{ModelID: "a", Label: "A", Default: true}})
	if err := service.validateLLOAModelID(strptr("a")); err != nil {
		t.Fatalf("allowlisted model should pass: %v", err)
	}
	if err := service.validateLLOAModelID(strptr("b")); err == nil {
		t.Fatalf("non-allowlisted model should fail")
	}
}

// modelDisplayNameFor — allowlist 라벨 우선, 기존 단일쌍(LLOA_MODEL/
// LLOA_MODEL_DISPLAY_NAME) fallback 유지 잠금.
func TestModelDisplayNameForUsesOptionsLabel(t *testing.T) {
	service := NewDatasetService(store.NewMemoryStore(), "", t.TempDir(), t.TempDir())
	service.SetLLOAModelDisplay("wisenut/wise-lloa-max-v1.2.1", "LLOA Max (env)")
	service.SetLLOAModelOptions([]domain.LLOAModelOption{
		{ModelID: "wisenut/wise-lloa-ultra-v1.1.0", Label: "LLOA Ultra 1.1.0"},
		// 라벨 생략(=id 동일) 항목은 표시명 미노출 → 단일쌍 fallback으로.
		{ModelID: "wisenut/wise-lloa-max-v1.2.1", Label: "wisenut/wise-lloa-max-v1.2.1", Default: true},
	})

	if got := service.modelDisplayNameFor("wisenut/wise-lloa-ultra-v1.1.0"); got != "LLOA Ultra 1.1.0" {
		t.Fatalf("options label lookup failed: %q", got)
	}
	if got := service.modelDisplayNameFor("wisenut/wise-lloa-max-v1.2.1"); got != "LLOA Max (env)" {
		t.Fatalf("single-pair fallback failed: %q", got)
	}
	if got := service.modelDisplayNameFor("unknown"); got != "" {
		t.Fatalf("unknown model should return empty: %q", got)
	}
}
