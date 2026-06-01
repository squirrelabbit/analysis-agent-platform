package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	apphttp "analysis-support-platform/control-plane/internal/http"
	"analysis-support-platform/control-plane/internal/obs"
)

func main() {
	obs.Init("control-plane")
	obs.Logger.Info("starting", "event", "service.boot.started")

	displaytime.UseKSTAsLocal()
	cfg := config.Load()

	obs.Logger.Info("configuration loaded",
		"event", "config.loaded",
		"workflow_engine", cfg.WorkflowEngine,
		"store_backend", cfg.StoreBackend,
		"planner_backend", cfg.PlannerBackend,
		"bind_addr", cfg.BindAddr,
	)

	if cfg.WorkflowEngine == "temporal" {
		obs.Logger.Info("temporal runtime configured",
			"persistence", cfg.TemporalPersistenceMode,
			"retention", cfg.TemporalRetentionMode,
			"recovery", cfg.TemporalRecoveryMode,
			"address", cfg.TemporalAddress,
			"namespace", cfg.TemporalNamespace,
		)
		if cfg.TemporalPersistenceMode == "dev_ephemeral" || cfg.TemporalRetentionMode == "temporal_dev_default" {
			obs.Logger.Warn("temporal history durability is limited; relying on startup reconciliation and persisted state for recovery")
		}
	}

	server := apphttp.NewServer(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		obs.Logger.Info("shutdown signal received", "event", "service.shutdown")
	}()

	// silverone 2026-05-27 (Codex adversarial review fix-2) — startup
	// reconciliation. 직전 프로세스가 in-flight 상태로 남긴 analysis_runs /
	// dataset_build_jobs를 모두 failed로 마감해 active job lookup이 영원히
	// 막히지 않게 한다. error_message로 운영자가 추적 가능.
	if report, err := server.ReconcileStartup(ctx); err != nil {
		obs.Logger.Error("startup reconciliation failed",
			"event", "startup.reconcile.failed",
			"error", err.Error(),
		)
	} else if len(report.AnalysisRunsFailed)+len(report.DatasetBuildJobsFailed) > 0 {
		obs.Logger.Warn("startup reconciliation closed in-flight rows",
			"event", "startup.reconcile.completed",
			"analysis_runs_failed_count", len(report.AnalysisRunsFailed),
			"dataset_build_jobs_failed_count", len(report.DatasetBuildJobsFailed),
			"analysis_run_ids", report.AnalysisRunsFailed,
			"dataset_build_job_ids", report.DatasetBuildJobsFailed,
		)
	} else {
		obs.Logger.Info("startup reconciliation idle",
			"event", "startup.reconcile.completed",
			"analysis_runs_failed_count", 0,
			"dataset_build_jobs_failed_count", 0,
		)
	}

	obs.Logger.Info("listening", "event", "service.boot.completed", "addr", cfg.BindAddr)
	if err := http.ListenAndServe(cfg.BindAddr, server.Handler()); err != nil {
		obs.Logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
