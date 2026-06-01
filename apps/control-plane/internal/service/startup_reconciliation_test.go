package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-05-27 (Codex adversarial review fix-3) — startup reconciliation
// regression. control-plane 재기동 시 in-flight row가 단말 상태로 마감되고,
// 옛 legacy schema가 ensureSchema 흐름에서 사라지지 않았다는 잠금.

func newReconcileFixture(t *testing.T) *DatasetService {
	t.Helper()
	memory := store.NewMemoryStore()
	tmpdir := t.TempDir()
	return NewDatasetService(memory, "", "", tmpdir)
}

func TestReconcileStartup_AnalysisRunRunningToFailed(t *testing.T) {
	svc := newReconcileFixture(t)
	now := time.Now().UTC()
	run := domain.AnalysisRun{
		RunID:            "run-inflight",
		ThreadID:         "th-1",
		ProjectID:        "p1",
		DatasetID:        "d1",
		DatasetVersionID: "v1",
		UserMessageID:    "msg-u",
		Status:           "running",
		CreatedAt:        now,
	}
	if err := svc.store.SaveAnalysisRun(run); err != nil {
		t.Fatalf("save run: %v", err)
	}

	report, err := svc.ReconcileStartup(context.Background())
	if err != nil {
		t.Fatalf("ReconcileStartup: %v", err)
	}
	if len(report.AnalysisRunsFailed) != 1 || report.AnalysisRunsFailed[0] != "run-inflight" {
		t.Fatalf("expected run-inflight in failed list, got %+v", report.AnalysisRunsFailed)
	}

	got, err := svc.store.GetAnalysisRun("p1", "run-inflight")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status != "failed" {
		t.Fatalf("expected status=failed, got %s", got.Status)
	}
	if got.ErrorMessage == nil || !strings.Contains(*got.ErrorMessage, "control-plane restarted") {
		t.Fatalf("expected restart marker in error_message, got %v", got.ErrorMessage)
	}
	if got.CompletedAt == nil {
		t.Fatalf("expected completed_at to be set")
	}
}

func TestReconcileStartup_DatasetBuildJobQueuedRunningToFailed(t *testing.T) {
	svc := newReconcileFixture(t)
	now := time.Now().UTC()
	for _, status := range []string{"queued", "running"} {
		job := domain.DatasetBuildJob{
			JobID:            "job-" + status,
			ProjectID:        "p1",
			DatasetID:        "d1",
			DatasetVersionID: "v1",
			BuildType:        "clean",
			Status:           status,
			CreatedAt:        now,
		}
		if err := svc.store.SaveDatasetBuildJob(job); err != nil {
			t.Fatalf("save job %s: %v", status, err)
		}
	}

	report, err := svc.ReconcileStartup(context.Background())
	if err != nil {
		t.Fatalf("ReconcileStartup: %v", err)
	}
	if len(report.DatasetBuildJobsFailed) != 2 {
		t.Fatalf("expected 2 jobs failed, got %+v", report.DatasetBuildJobsFailed)
	}

	for _, status := range []string{"queued", "running"} {
		got, err := svc.store.GetDatasetBuildJob("p1", "job-"+status)
		if err != nil {
			t.Fatalf("get job-%s: %v", status, err)
		}
		if got.Status != "failed" {
			t.Fatalf("job-%s expected status=failed, got %s", status, got.Status)
		}
		if got.ErrorMessage == nil || !strings.Contains(*got.ErrorMessage, "control-plane restarted") {
			t.Fatalf("job-%s expected restart marker, got %v", status, got.ErrorMessage)
		}
	}
}

func TestReconcileStartup_DoesNotTouchCompletedRows(t *testing.T) {
	svc := newReconcileFixture(t)
	now := time.Now().UTC()
	completedAt := now.Add(-time.Hour)
	msg := "previous error"
	completedRun := domain.AnalysisRun{
		RunID: "run-done", ThreadID: "th-1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "completed", CreatedAt: completedAt, CompletedAt: &completedAt,
	}
	failedRun := domain.AnalysisRun{
		RunID: "run-failed", ThreadID: "th-1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", Status: "failed", ErrorMessage: &msg,
		CreatedAt: completedAt, CompletedAt: &completedAt,
	}
	if err := svc.store.SaveAnalysisRun(completedRun); err != nil {
		t.Fatalf("save completed: %v", err)
	}
	if err := svc.store.SaveAnalysisRun(failedRun); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	report, err := svc.ReconcileStartup(context.Background())
	if err != nil {
		t.Fatalf("ReconcileStartup: %v", err)
	}
	if len(report.AnalysisRunsFailed)+len(report.DatasetBuildJobsFailed) != 0 {
		t.Fatalf("expected no rows to be touched, got %+v", report)
	}

	got, err := svc.store.GetAnalysisRun("p1", "run-failed")
	if err != nil {
		t.Fatalf("get failed run: %v", err)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != "previous error" {
		t.Fatalf("expected original error message preserved, got %v", got.ErrorMessage)
	}
}

func TestReconcileStartup_OrphanRunningJobDoesNotBlockNewListing(t *testing.T) {
	// 재기동 후 active job lookup이 orphan running job 때문에 막히지 않는지.
	// 시나리오: running job이 남아 있다 → reconcile → failed로 마감 →
	// 같은 dataset_version에 새 job을 생성해도 list가 정상 반환.
	svc := newReconcileFixture(t)
	now := time.Now().UTC()
	orphan := domain.DatasetBuildJob{
		JobID: "job-orphan", ProjectID: "p1", DatasetID: "d1", DatasetVersionID: "v1",
		BuildType: "clean", Status: "running", CreatedAt: now,
	}
	if err := svc.store.SaveDatasetBuildJob(orphan); err != nil {
		t.Fatalf("save orphan: %v", err)
	}
	if _, err := svc.ReconcileStartup(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// 새 job 생성
	newJob := domain.DatasetBuildJob{
		JobID: "job-new", ProjectID: "p1", DatasetID: "d1", DatasetVersionID: "v1",
		BuildType: "clean", Status: "queued", CreatedAt: now.Add(time.Second),
	}
	if err := svc.store.SaveDatasetBuildJob(newJob); err != nil {
		t.Fatalf("save new: %v", err)
	}

	jobs, err := svc.store.ListDatasetBuildJobs("p1", "v1")
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs (orphan failed + new queued), got %d", len(jobs))
	}
	var orphanStatus, newStatus string
	for _, j := range jobs {
		switch j.JobID {
		case "job-orphan":
			orphanStatus = j.Status
		case "job-new":
			newStatus = j.Status
		}
	}
	if orphanStatus != "failed" || newStatus != "queued" {
		t.Fatalf("expected orphan=failed/new=queued, got orphan=%s new=%s", orphanStatus, newStatus)
	}
}
