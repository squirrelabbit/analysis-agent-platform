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

// doc_genuineness verify 모드 Go 배선 잠금 (ADR-026, step 4a).

func boolPtr(b bool) *bool { return &b }

func verifyFixture(t *testing.T) (*DatasetService, *store.MemoryStore) {
	t.Helper()
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	svc.SetLLOAModelOptions([]domain.LLOAModelOption{
		{ModelID: "wisenut/wise-lloa-max-v1.2.1", Label: "Max", Default: true},
		{ModelID: "wisenut/wise-lloa-ultra-v1.1.0", Label: "Ultra"},
	})
	if err := repo.SaveProject(domain.Project{ProjectID: "p1", Name: "P", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{
		DatasetID: "d1", ProjectID: "p1", Name: "ds",
		Metadata:  map[string]any{"doc_genuineness": map[string]any{"subject_type": "festival", "subject_name": "강릉 국가유산야행"}},
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("dataset: %v", err)
	}
	cleaned := "/tmp/festival.cleaned.parquet"
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "v1", DatasetID: "d1", ProjectID: "p1",
		StorageURI: "/tmp/f.csv", CleanStatus: "ready", CleanURI: &cleaned,
		Metadata: map[string]any{},
	}); err != nil {
		t.Fatalf("version: %v", err)
	}
	return svc, repo
}

func TestBuildDocGenuinenessVerifyPayload(t *testing.T) {
	svc, _ := verifyFixture(t)
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&payload)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"done"},
			"artifact": map[string]any{
				"doc_genuineness_ref": "/tmp/out.verify.jsonl",
				"summary":             map[string]any{"mode": "verify", "models": map[string]any{"a": "wisenut/wise-lloa-max-v1.2.1", "b": "wisenut/wise-lloa-ultra-v1.1.0", "judge": "wisenut/wise-lloa-ultra-v1.1.0"}},
			},
		})
	}))
	defer server.Close()
	svc.pythonAIWorkerURL = server.URL

	got, err := svc.BuildDocGenuineness("p1", "d1", "v1", domain.DatasetDocGenuinenessBuildRequest{
		Verify:         boolPtr(true),
		ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1", "wisenut/wise-lloa-ultra-v1.1.0"},
		JudgeModel:     strptr("wisenut/wise-lloa-ultra-v1.1.0"),
	})
	if err != nil {
		t.Fatalf("BuildDocGenuineness verify: %v", err)
	}
	if payload["verify"] != true {
		t.Fatalf("payload.verify not set: %+v", payload["verify"])
	}
	cm, _ := payload["classify_models"].([]any)
	if len(cm) != 2 {
		t.Fatalf("classify_models not 2: %+v", payload["classify_models"])
	}
	if payload["judge_model"] != "wisenut/wise-lloa-ultra-v1.1.0" {
		t.Fatalf("judge_model: %+v", payload["judge_model"])
	}
	if got.Metadata["doc_genuineness_mode"] != "verify" {
		t.Fatalf("mode metadata not verify: %+v", got.Metadata["doc_genuineness_mode"])
	}
	// verify는 per-model runs를 쓰지 않는다.
	if _, exists := got.Metadata["doc_genuineness_runs"]; exists {
		t.Fatalf("verify should not write doc_genuineness_runs")
	}
}

func TestCreateDocGenuinenessJobVerifyValidation(t *testing.T) {
	svc, _ := verifyFixture(t)

	// classify_models 1개 → 거절.
	_, err := svc.CreateDocGenuinenessJob("p1", "d1", "v1", domain.DatasetDocGenuinenessBuildRequest{
		Verify: boolPtr(true), ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1"},
	}, "api", "r1")
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("1 classify model should be ErrInvalidArgument, got %v", err)
	}

	// 같은 모델 2개 → 거절.
	_, err = svc.CreateDocGenuinenessJob("p1", "d1", "v1", domain.DatasetDocGenuinenessBuildRequest{
		Verify:         boolPtr(true),
		ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1", "wisenut/wise-lloa-max-v1.2.1"},
	}, "api", "r1")
	if !errors.As(err, &invalid) {
		t.Fatalf("duplicate classify model should be ErrInvalidArgument, got %v", err)
	}

	// allowlist 밖 judge → 거절.
	_, err = svc.CreateDocGenuinenessJob("p1", "d1", "v1", domain.DatasetDocGenuinenessBuildRequest{
		Verify:         boolPtr(true),
		ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1", "wisenut/wise-lloa-ultra-v1.1.0"},
		JudgeModel:     strptr("gpt-4o"),
	}, "api", "r1")
	if !errors.As(err, &invalid) {
		t.Fatalf("non-allowlist judge should be ErrInvalidArgument, got %v", err)
	}
}
