package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// TestBuildDocGenuinenessHappyPath — ADR-017 / 5/19 결정 doc-level 3-tier
// 진성 분류 happy path. clean ready 상태에서 Python worker mock으로 50건
// 분류 응답을 받고 version metadata가 doc_genuineness_status=ready로 업데이트
// 되는지 확인.
func TestBuildDocGenuinenessHappyPath(t *testing.T) {
	repository := store.NewMemoryStore()
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	service := NewDatasetService(repository, "", uploadRoot, artifactRoot)

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("save project: %v", err)
	}
	// silverone 2026-05-22 (PR-α2) — doc_genuineness build는 이제 dataset
	// metadata에서 subject variables를 읽어 payload에 inject한다. 기존 fixture를
	// festival 도메인 값으로 채워 happy path 회귀가 깨지지 않게 한다.
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "festival",
		DataType:  "unstructured",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{
				"subject_type":         "festival",
				"subject_name":         "강릉 국가유산야행",
				"subject_aliases":      []any{"문화유산야행", "문화재야행", "강릉야행"},
				"recruitment_keywords": []any{"서포터즈", "푸드트럭"},
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

	var requestedPath, requestedCleanRef, requestedOutputPath string
	var requestedDocGen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		requestedPath = r.URL.Path
		requestedCleanRef, _ = payload["clean_artifact_ref"].(string)
		requestedOutputPath, _ = payload["output_path"].(string)
		requestedDocGen, _ = payload["doc_genuineness"].(map[string]any)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"doc_genuineness completed"},
			"artifact": map[string]any{
				"skill_name":          "dataset_doc_genuineness",
				"doc_genuineness_uri": "/tmp/festival.doc_genuineness.jsonl",
				"doc_genuineness_ref": "/tmp/festival.doc_genuineness.jsonl",
				"summary": map[string]any{
					"input_row_count":     50,
					"processed_row_count": 50,
					"tier_counts": map[string]any{
						"genuine_review": 30,
						"mixed":          12,
						"non_review":     8,
					},
					"parse_failures":          0,
					"prompt_version":          "dataset-doc-genuineness-v1",
					"model":                   "wisenut/wise-lloa-max-v1.2.1",
					"total_prompt_tokens":     5000,
					"total_completion_tokens": 1500,
					"applied": map[string]any{
						"prompt_version":       "dataset-doc-genuineness-v1",
						"subject_name":         "강릉 국가유산야행",
						"subject_aliases":      []any{"문화유산야행", "문화재야행", "강릉야행"},
						"recruitment_keywords": []any{"서포터즈", "푸드트럭"},
						"subject_type":         "festival",
					},
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildDocGenuineness(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
	)
	if err != nil {
		t.Fatalf("BuildDocGenuineness: %v", err)
	}

	// ADR-017 핵심: task_path가 registry lookup으로 라우팅됐는지 잠금.
	if requestedPath != "/tasks/dataset_doc_genuineness" {
		t.Fatalf("unexpected worker path: %s", requestedPath)
	}
	if requestedCleanRef != cleanedURI {
		t.Fatalf("unexpected clean ref: %s", requestedCleanRef)
	}
	if !strings.HasPrefix(requestedOutputPath, artifactRoot) {
		t.Fatalf("unexpected output path: %s", requestedOutputPath)
	}

	// version metadata가 ready로 업데이트됐는지.
	if status := metadataString(result.Metadata, "doc_genuineness_status", ""); status != "ready" {
		t.Fatalf("unexpected status: %s", status)
	}
	if ref := metadataString(result.Metadata, "doc_genuineness_ref", ""); ref != "/tmp/festival.doc_genuineness.jsonl" {
		t.Fatalf("unexpected ref: %s", ref)
	}
	if errMsg := metadataString(result.Metadata, "doc_genuineness_error", ""); errMsg != "" {
		t.Fatalf("error key should be deleted after success: %s", errMsg)
	}
	summary, ok := result.Metadata["doc_genuineness_summary"].(map[string]any)
	if !ok {
		t.Fatalf("doc_genuineness_summary missing: %+v", result.Metadata)
	}
	tiers, ok := summary["tier_counts"].(map[string]any)
	if !ok {
		t.Fatalf("tier_counts missing: %+v", summary)
	}
	if int(tiers["genuine_review"].(float64)) != 30 {
		t.Fatalf("unexpected genuine_review count: %+v", tiers["genuine_review"])
	}

	// silverone 2026-05-22 (PR-α2) — payload의 doc_genuineness key가 dataset
	// metadata에서 정규화돼 inject됐는지 잠금.
	if requestedDocGen == nil {
		t.Fatalf("payload['doc_genuineness'] missing")
	}
	if requestedDocGen["subject_name"] != "강릉 국가유산야행" {
		t.Fatalf("subject_name not injected: %+v", requestedDocGen)
	}
	if requestedDocGen["subject_type"] != "festival" {
		t.Fatalf("subject_type not injected: %+v", requestedDocGen)
	}
	aliases, ok := requestedDocGen["subject_aliases"].([]any)
	if !ok || len(aliases) != 3 || aliases[0] != "문화유산야행" {
		t.Fatalf("subject_aliases not injected: %+v", requestedDocGen["subject_aliases"])
	}

	// applied snapshot이 version metadata로 보존됐는지.
	applied, ok := result.Metadata["doc_genuineness_applied"].(map[string]any)
	if !ok {
		t.Fatalf("doc_genuineness_applied snapshot missing: %+v", result.Metadata)
	}
	if applied["subject_name"] != "강릉 국가유산야행" {
		t.Fatalf("applied snapshot wrong subject: %+v", applied)
	}
}

// TestBuildDocGenuinenessFailsWhenSubjectNameMissing — silverone 2026-05-22
// (PR-α2). dataset.metadata.doc_genuineness.subject_name이 없으면 fail-loud.
// festival prompt fallback 없음.
func TestBuildDocGenuinenessFailsWhenSubjectNameMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-α2-fail", Name: "test", CreatedAt: time.Now().UTC()}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{
		DatasetID: "dataset-α2-fail",
		ProjectID: project.ProjectID,
		Name:      "no-subject",
		DataType:  "unstructured",
		// Metadata 통째 누락 — doc_genuineness build가 거부해야 함.
		CreatedAt: time.Now().UTC(),
	}
	_ = repository.SaveDataset(dataset)
	cleanedURI := "/tmp/x.parquet"
	version := domain.DatasetVersion{
		DatasetVersionID: "vid-α2-fail",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/x.csv",
		DataType:         "unstructured",
		CleanStatus:      "ready",
		CleanURI:         &cleanedURI,
		Metadata:         map[string]any{},
	}
	_ = repository.SaveDatasetVersion(version)

	_, err := service.BuildDocGenuineness(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
	)
	if err == nil {
		t.Fatalf("expected fail-loud when subject_name missing")
	}
	if !strings.Contains(err.Error(), "subject_name") {
		t.Fatalf("expected error to mention subject_name, got: %v", err)
	}
}

func TestCreateDocGenuinenessJobFailsBeforeQueueWhenSubjectNameMissing(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-α2-preflight", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("save project: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-α2-preflight",
		ProjectID: project.ProjectID,
		Name:      "no-subject",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	cleanedURI := "/tmp/preflight.parquet"
	version := domain.DatasetVersion{
		DatasetVersionID: "vid-α2-preflight",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/preflight.csv",
		DataType:         "unstructured",
		CleanStatus:      "ready",
		CleanURI:         &cleanedURI,
		Metadata:         map[string]any{"clean_status": "ready"},
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}

	_, err := service.CreateDocGenuinenessJob(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
		"api",
		"request-α2-preflight",
	)
	if err == nil {
		t.Fatalf("expected preflight error when subject_name missing")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "POST /projects/{pid}/datasets") || !strings.Contains(err.Error(), "PATCH /projects/{pid}/datasets/{did}/metadata") {
		t.Fatalf("expected error to guide metadata registration/retry, got: %v", err)
	}

	jobs, err := service.ListDatasetBuildJobs(project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected no queued job before metadata registration, got %+v", jobs.Items)
	}
	reloaded, err := repository.GetDatasetVersion(project.ProjectID, version.DatasetVersionID)
	if err != nil {
		t.Fatalf("reload version: %v", err)
	}
	if status := metadataString(reloaded.Metadata, "doc_genuineness_status", ""); status != "" {
		t.Fatalf("doc_genuineness_status should not be set before preflight passes, got %q", status)
	}
}

// TestExtractDocGenuinenessConfigDefaultsForOptionalFields — silverone
// 2026-05-22 (PR-α2). subject_aliases / recruitment_keywords 누락 시 빈
// 배열 default + subject_type 누락 시 "generic".
func TestExtractDocGenuinenessConfigDefaultsForOptionalFields(t *testing.T) {
	config, err := extractDocGenuinenessConfig(map[string]any{
		"doc_genuineness": map[string]any{
			"subject_name": "단일 subject",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config["subject_type"] != "generic" {
		t.Fatalf("subject_type default: %v", config["subject_type"])
	}
	aliases, _ := config["subject_aliases"].([]string)
	if aliases == nil || len(aliases) != 0 {
		t.Fatalf("subject_aliases default should be empty slice, got %#v", config["subject_aliases"])
	}
	keywords, _ := config["recruitment_keywords"].([]string)
	if keywords == nil || len(keywords) != 0 {
		t.Fatalf("recruitment_keywords default should be empty slice, got %#v", config["recruitment_keywords"])
	}
}

// TestBuildDocGenuinenessRequiresCleanReady — clean이 ready 상태가 아니면
// CreateDocGenuinenessJob에서 ErrInvalidArgument로 거부되는지 (gate 작동).
func TestBuildDocGenuinenessRequiresCleanReady(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-2", Name: "test", CreatedAt: time.Now().UTC()}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{
		DatasetID: "dataset-2",
		ProjectID: project.ProjectID,
		Name:      "festival",
		DataType:  "unstructured",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{"subject_name": "강릉 국가유산야행"},
		},
		CreatedAt: time.Now().UTC(),
	}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-2",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/festival.csv",
		DataType:         "unstructured",
		CleanStatus:      "queued",
		Metadata:         map[string]any{},
	}
	_ = repository.SaveDatasetVersion(version)

	_, err := service.CreateDocGenuinenessJob(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
		"api",
		"req-1",
	)
	if err == nil {
		t.Fatalf("expected ErrInvalidArgument when clean not ready")
	}
	if !strings.Contains(err.Error(), "clean must be ready") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestBuildDocGenuinenessRequiresCleanArtifact — clean이 ready 표시이지만
// CleanURI가 비어 있으면 BuildDocGenuineness가 명시적으로 fail (silent
// fallback 방지).
func TestBuildDocGenuinenessRequiresCleanArtifact(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-3", Name: "test", CreatedAt: time.Now().UTC()}
	_ = repository.SaveProject(project)
	dataset := domain.Dataset{
		DatasetID: "dataset-3",
		ProjectID: project.ProjectID,
		Name:      "festival",
		DataType:  "unstructured",
		Metadata: map[string]any{
			"doc_genuineness": map[string]any{"subject_name": "강릉 국가유산야행"},
		},
		CreatedAt: time.Now().UTC(),
	}
	_ = repository.SaveDataset(dataset)
	version := domain.DatasetVersion{
		DatasetVersionID: "version-3",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/festival.csv",
		DataType:         "unstructured",
		CleanStatus:      "ready",
		// CleanURI 명시적 nil — artifact ref 없음
		Metadata: map[string]any{},
	}
	_ = repository.SaveDatasetVersion(version)

	_, err := service.BuildDocGenuineness(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		domain.DatasetDocGenuinenessBuildRequest{},
	)
	if err == nil {
		t.Fatalf("expected error when clean artifact ref missing")
	}
	if !strings.Contains(err.Error(), "clean artifact ref missing") {
		t.Fatalf("unexpected error: %v", err)
	}
}
