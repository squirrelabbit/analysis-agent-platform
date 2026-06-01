package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// TestBuildClauseLabelInjectsSubjectFromDatasetMetadata — silverone 2026-05-28.
// dataset.metadata.doc_genuineness map을 Python payload['doc_genuineness']로
// 그대로 pass-through하는지 잠금. doc_genuineness PR-α2 패턴을 clause_label에도
// 이식한 결과.
func TestBuildClauseLabelInjectsSubjectFromDatasetMetadata(t *testing.T) {
	service, project, dataset, version := setupClauseLabelHarness(t, map[string]any{
		"doc_genuineness": map[string]any{
			"subject_type":         "festival",
			"subject_name":         "강릉 국가유산야행",
			"subject_aliases":      []any{"문화유산야행", "강릉야행"},
			"recruitment_keywords": []any{"서포터즈"},
		},
	})

	var requestedDocGen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		requestedDocGen, _ = payload["doc_genuineness"].(map[string]any)
		_ = json.NewEncoder(w).Encode(clauseLabelSuccessResponse(map[string]any{
			"prompt_version":  "dataset-clause-label-v3",
			"subject_name":    "강릉 국가유산야행",
			"subject_aliases": []any{"문화유산야행", "강릉야행"},
			"subject_type":    "festival",
		}))
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	emptyTiers := []string{}
	result, err := service.BuildClauseLabel(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetClauseLabelBuildRequest{IncludeGenuineness: emptyTiers},
	)
	if err != nil {
		t.Fatalf("BuildClauseLabel: %v", err)
	}

	if requestedDocGen == nil {
		t.Fatalf("payload['doc_genuineness'] missing — expected pass-through from dataset metadata")
	}
	if requestedDocGen["subject_name"] != "강릉 국가유산야행" {
		t.Fatalf("subject_name not passed: %+v", requestedDocGen)
	}
	if requestedDocGen["subject_type"] != "festival" {
		t.Fatalf("subject_type not passed: %+v", requestedDocGen)
	}
	aliases, ok := requestedDocGen["subject_aliases"].([]any)
	if !ok || len(aliases) != 2 || aliases[0] != "문화유산야행" {
		t.Fatalf("subject_aliases not passed: %+v", requestedDocGen["subject_aliases"])
	}
	// recruitment_keywords는 raw pass-through라 payload엔 그대로 있지만
	// Python `_extract_subject_config`가 무시한다. Go는 이 정책을 강제하지 않음.
	if _, ok := requestedDocGen["recruitment_keywords"]; !ok {
		t.Fatalf("recruitment_keywords should be passed through as part of raw map")
	}

	// summary.applied snapshot이 version metadata로 보존됐는지.
	applied, ok := result.Metadata["clause_label_applied"].(map[string]any)
	if !ok {
		t.Fatalf("clause_label_applied snapshot missing: %+v", result.Metadata)
	}
	if applied["subject_name"] != "강릉 국가유산야행" {
		t.Fatalf("applied snapshot wrong subject: %+v", applied)
	}
}

// TestBuildClauseLabelOmitsDocGenuinenessWhenMetadataAbsent — silverone
// 2026-05-28. 옛 dataset(metadata.doc_genuineness 없음) 호환 경로. Go는 키를
// payload에 inject하지 않고, Python이 festival default로 fallback한다.
func TestBuildClauseLabelOmitsDocGenuinenessWhenMetadataAbsent(t *testing.T) {
	service, project, dataset, version := setupClauseLabelHarness(t, map[string]any{})

	var payloadKeys map[string]bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		payloadKeys = make(map[string]bool, len(payload))
		for k := range payload {
			payloadKeys[k] = true
		}
		_ = json.NewEncoder(w).Encode(clauseLabelSuccessResponse(nil))
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	emptyTiers := []string{}
	_, err := service.BuildClauseLabel(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetClauseLabelBuildRequest{IncludeGenuineness: emptyTiers},
	)
	if err != nil {
		t.Fatalf("BuildClauseLabel: %v", err)
	}

	if payloadKeys["doc_genuineness"] {
		t.Fatalf("payload should omit doc_genuineness when dataset metadata absent — Python fallback path")
	}
}

func setupClauseLabelHarness(t *testing.T, datasetMetadata map[string]any) (*DatasetService, domain.Project, domain.Dataset, domain.DatasetVersion) {
	t.Helper()
	repository := store.NewMemoryStore()
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	service := NewDatasetService(repository, "", uploadRoot, artifactRoot)

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("save project: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "festival",
		DataType:  "unstructured",
		Metadata:  datasetMetadata,
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

func clauseLabelSuccessResponse(applied map[string]any) map[string]any {
	summary := map[string]any{
		"input_row_count":     50,
		"processed_doc_count": 50,
		"clause_count":        180,
		"sentiment_counts":    map[string]any{"positive": 100, "negative": 40, "neutral": 40},
		"aspect_counts":       map[string]any{},
		"parse_failures":      0,
		"prompt_version":      "dataset-clause-label-v3",
		"model":               "wisenut/wise-lloa-max-v1.2.1",
		"concurrency":         8,
		"taxonomy_id":         "festival-v2",
		"taxonomy_hash":       "abc123",
	}
	if applied != nil {
		summary["applied"] = applied
	}
	return map[string]any{
		"notes": []string{"dataset_clause_label completed"},
		"artifact": map[string]any{
			"skill_name":                "dataset_clause_label",
			"clause_label_uri":          "/tmp/festival.clause_label.jsonl",
			"clause_label_ref":          "/tmp/festival.clause_label.jsonl",
			"clause_label_input_source": "clean",
			"prompt_version":            "dataset-clause-label-v3",
			"summary":                   summary,
		},
	}
}
