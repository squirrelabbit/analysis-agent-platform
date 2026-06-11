package service

import (
	"encoding/json"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func seedCompletedRun(t *testing.T, svc *DatasetService, runID, threadID string) {
	t.Helper()
	result := json.RawMessage(`{
		"plan": {"plan_version": "v2", "steps": [{"id": "s1", "skill": "aggregate", "params": {}}]},
		"composer": {
			"assistant_content": "부정 후기는 음식에 집중됩니다.",
			"display": {
				"type": "table",
				"title": "Aspect별 부정 건수",
				"columns": ["aspect", "count"],
				"rows": [{"aspect": "음식", "count": 312}]
			}
		}
	}`)
	if err := svc.store.SaveAnalysisRun(domain.AnalysisRun{
		RunID: runID, ThreadID: threadID, ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", UserMessageID: "um1", Status: "completed",
		RequestJSON: map[string]any{"user_question": "부정 후기가 많은 aspect"},
		ResultJSON:  result, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
}

func TestCreateSavedResultSnapshotsRun(t *testing.T) {
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", "")
	if err := memory.SaveProject(domain.Project{ProjectID: "p1", Name: "P"}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	seedCompletedRun(t, svc, "run-1", "t1")

	saved, err := svc.CreateSavedResult("p1", domain.ReportSavedResultCreateRequest{
		ThreadID: "t1", RunID: "run-1",
	})
	if err != nil {
		t.Fatalf("CreateSavedResult: %v", err)
	}
	if saved.Title != "Aspect별 부정 건수" {
		t.Errorf("title from display.title: got %q", saved.Title)
	}
	if saved.Question != "부정 후기가 많은 aspect" {
		t.Errorf("question from request_json: got %q", saved.Question)
	}
	if saved.AssistantContent == "" {
		t.Error("assistant_content snapshot missing")
	}
	if saved.DatasetID != "d1" || saved.DatasetVersionID != "v1" {
		t.Errorf("dataset/version from run: got %q/%q", saved.DatasetID, saved.DatasetVersionID)
	}
	if saved.SourceMessageID != "um1" {
		t.Errorf("source_message_id from run.UserMessageID: got %q", saved.SourceMessageID)
	}
	if saved.Display == nil || saved.Display["title"] != "Aspect별 부정 건수" {
		t.Errorf("display snapshot missing/wrong: %v", saved.Display)
	}
	if saved.Plan == nil || saved.Plan["plan_version"] != "v2" {
		t.Errorf("plan snapshot missing/wrong: %v", saved.Plan)
	}

	// 목록 — 1건.
	list, err := svc.ListSavedResults("p1", "")
	if err != nil {
		t.Fatalf("ListSavedResults: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].ResultID != saved.ResultID {
		t.Fatalf("list want 1 matching item, got %d", len(list.Items))
	}

	// dataset 필터 — 다른 dataset이면 0건.
	other, err := svc.ListSavedResults("p1", "other")
	if err != nil {
		t.Fatalf("ListSavedResults(other): %v", err)
	}
	if len(other.Items) != 0 {
		t.Errorf("dataset filter should exclude: got %d", len(other.Items))
	}

	// 삭제 후 빈 목록.
	if err := svc.DeleteSavedResult("p1", saved.ResultID); err != nil {
		t.Fatalf("DeleteSavedResult: %v", err)
	}
	after, _ := svc.ListSavedResults("p1", "")
	if len(after.Items) != 0 {
		t.Errorf("after delete want 0, got %d", len(after.Items))
	}
}

func TestCreateSavedResultRejectsIncompleteRun(t *testing.T) {
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", "")
	if err := memory.SaveAnalysisRun(domain.AnalysisRun{
		RunID: "run-x", ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "running", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveAnalysisRun: %v", err)
	}
	_, err := svc.CreateSavedResult("p1", domain.ReportSavedResultCreateRequest{RunID: "run-x"})
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("incomplete run: want ErrInvalidArgument, got %v", err)
	}
}

func TestCreateSavedResultThreadMismatch(t *testing.T) {
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", "")
	seedCompletedRun(t, svc, "run-1", "t1")
	_, err := svc.CreateSavedResult("p1", domain.ReportSavedResultCreateRequest{
		ThreadID: "WRONG", RunID: "run-1",
	})
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("thread mismatch: want ErrInvalidArgument, got %v", err)
	}
}

func TestCreateSavedResultMissingRun(t *testing.T) {
	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", "")
	_, err := svc.CreateSavedResult("p1", domain.ReportSavedResultCreateRequest{RunID: "nope"})
	if _, ok := err.(ErrNotFound); !ok {
		t.Fatalf("missing run: want ErrNotFound, got %v", err)
	}
}
