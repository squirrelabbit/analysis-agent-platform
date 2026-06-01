package service

import (
	"context"
	"time"

	"analysis-support-platform/control-plane/internal/obs"
)

// ReconcileStartup — silverone 2026-05-27 (Codex adversarial review fix-2).
// control-plane 부팅 시 호출. 직전 프로세스가 in-flight 상태로 남긴 row를
// 모두 단말 상태로 마감해 active job lookup이 영원히 막히지 않게 한다.
//
// 정책:
//   - analysis_runs.status='running' → 'failed' + error_message=
//     "control-plane restarted during analysis execution" + completed_at=now.
//     plan reuse 흐름의 GetLastSuccessfulAnalysisRun에 영향 없음 (running이라
//     원래 source 후보가 아님).
//   - dataset_build_jobs.status in ('queued','running') → 'failed' +
//     error_message="control-plane restarted before job completion" +
//     completed_at=now. Temporal workflow 재조회는 후속 작업으로 분리
//     (장기적으로 workflow_id로 status pull). 1차 cut은 fail-loud.
//   - 둘 다 idempotent — 다음 부팅 때 row가 없으면 no-op.
//
// 호출자(main.go)는 결과 metric을 obs로 기록한다 (운영자가 재기동 후 어떤
// row가 정리됐는지 즉시 확인).
func (s *DatasetService) ReconcileStartup(ctx context.Context) (ReconcileReport, error) {
	report := ReconcileReport{}
	now := time.Now().UTC()

	runs, err := s.store.ListInFlightAnalysisRuns()
	if err != nil {
		return report, err
	}
	for _, run := range runs {
		message := "control-plane restarted during analysis execution"
		run.Status = "failed"
		run.ErrorMessage = &message
		run.CompletedAt = &now
		if err := s.store.SaveAnalysisRun(run); err != nil {
			obs.Logger.Warn("startup.reconcile.analysis_run_save_failed",
				"event", "startup.reconcile.analysis_run_save_failed",
				"run_id", run.RunID,
				"error", err.Error(),
			)
			continue
		}
		report.AnalysisRunsFailed = append(report.AnalysisRunsFailed, run.RunID)
	}

	jobs, err := s.store.ListInFlightDatasetBuildJobs()
	if err != nil {
		return report, err
	}
	for _, job := range jobs {
		message := "control-plane restarted before job completion"
		job.Status = "failed"
		job.ErrorMessage = &message
		job.CompletedAt = &now
		if err := s.store.SaveDatasetBuildJob(job); err != nil {
			obs.Logger.Warn("startup.reconcile.build_job_save_failed",
				"event", "startup.reconcile.build_job_save_failed",
				"job_id", job.JobID,
				"error", err.Error(),
			)
			continue
		}
		report.DatasetBuildJobsFailed = append(report.DatasetBuildJobsFailed, job.JobID)
	}

	_ = ctx // 후속에서 Temporal workflow pull 시 사용 예정
	return report, nil
}

// ReconcileReport — startup reconciliation 결과 요약. main.go가 obs log로
// 노출. failed assistant placeholder 저장은 후속 결정 항목 — 이번 PR에서는
// run.error_message만으로 운영자 추적 가능.
type ReconcileReport struct {
	AnalysisRunsFailed     []string
	DatasetBuildJobsFailed []string
}
